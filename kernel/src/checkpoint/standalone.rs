//! File and non-file state of a standalone adaptiveMetadata checkpoint.
//!
//! A standalone checkpoint at version n carries, inline, the reconciled add and remove actions of
//! the commits after its latest checkpoint action's content root, `(contentRoot.version, n]`.
//! Those commits are not covered by the metadata tree the content root references. Its txn and
//! domain metadata state is the latest checkpoint action's state, merged with the changes in
//! `(checkpointMetadata.version, n]`.

use std::iter;

use itertools::Itertools;

use super::{
    CheckpointWriter, STANDALONE_CHECKPOINT_ACTIONS_SCHEMA, checkpoint_output_transform,
    transform_batches,
};
use crate::action_reconciliation::file_metadata::build_checkpoint_read_schema;
use crate::action_reconciliation::log_replay::{
    ActionReconciliationBatch, ActionReconciliationProcessor, RemoveRetention,
};
use crate::action_reconciliation::{ActionReconciliationIterator, merge_txns_and_domain_metadata};
use crate::actions::set_transaction::scan_application_transactions;
use crate::actions::{CheckpointAction, LOG_DOMAIN_METADATA_SCHEMA, LOG_TXN_SCHEMA};
use crate::content_tree::{read_checkpoint_action, read_standalone_checkpoint_file_actions};
use crate::engine_data::FilteredEngineData;
use crate::log_replay::{ActionsBatch, HasSelectionVector};
use crate::log_segment::{CheckpointActionSource, scan_domain_metadata_actions};
use crate::path::{LogRoot, ParsedLogPath};
use crate::schema::SchemaRef;
use crate::snapshot::SnapshotRef;
use crate::{
    Engine, FileMeta, KernelError, KernelResult, KernelResultIterator, Version, version_as_i64,
};

/// Returns the output schema and the reconciled file actions of the window `(contentRoot.version,
/// n]`, where `snapshot` is at version n and `action` is the standalone checkpoint's action at n.
/// The batches are in the output schema, so they can be chained with the standalone checkpoint's
/// other rows.
///
/// When `snapshot` lists the standalone checkpoint that its latest checkpoint action comes from,
/// that checkpoint's inline file actions cover the start of the window, and only the commits after
/// it are read.
///
/// Every remove in the window is kept, including removes that cancel an add in the window. Adds
/// and removes are copied verbatim.
///
/// # Errors
///
/// Returns an error if `snapshot` lists a checkpoint at or after its latest checkpoint action's
/// version that is not a standalone checkpoint at that version, or if the table writes
/// collation-aware stats as a struct.
pub(super) fn gap_file_actions(
    snapshot: &SnapshotRef,
    engine: &dyn Engine,
    action: &CheckpointAction,
) -> KernelResult<(
    SchemaRef,
    impl KernelResultIterator<ActionReconciliationBatch> + use<>,
)> {
    let predecessor = listed_predecessor(snapshot, engine)?;
    let window_start = non_negative_version(action.content_root_version(), "Content root version")?;

    let mut context = CheckpointWriter::checkpoint_schema_context(snapshot, engine)?;
    // The inline adds are the only copy of the window's adds outside the commit JSON, so they keep
    // their JSON stats even when checkpoints of this table do not write them.
    context.stats_config.write_stats_as_json = true;
    if context.stats_config.write_stats_as_struct
        && snapshot.table_configuration().has_collations_feature()
    {
        return Err(KernelError::unsupported(
            "Standalone checkpoints do not yet support writing collation stats as a struct",
        ));
    }
    let read_schema = build_checkpoint_read_schema(
        &STANDALONE_CHECKPOINT_ACTIONS_SCHEMA,
        &context.stats_schema,
        context.partition_schema.as_deref(),
    )?;
    let (output_schema, evaluator) =
        checkpoint_output_transform(engine, &context, &read_schema, &context.stats_schema)?;

    // Commits at or below the predecessor may be cleaned up, and its inline rows already cover
    // them.
    let commits_after = predecessor
        .as_ref()
        .map_or(window_start, |file| file.version);
    // TODO: log compaction files spanning the content root version must not feed this window.
    // Compaction is disabled (#2337).
    let window = snapshot.log_segment().segment_after_version(commits_after);
    // TODO: fail when reconciliation drops a backReference: a re-add without one after a remove
    // with one, or a remove without one after an add with one. Checking it needs backReference
    // columns and per-key state in the visitor.
    let mut processor = ActionReconciliationProcessor::new(
        RemoveRetention::KeepAll,
        None,
        snapshot.table_configuration().dv_normalization_root(),
    )
    .with_file_actions_only();
    // Newest first: the commits after the predecessor, then the rows the predecessor holds.
    let inline: Box<dyn KernelResultIterator<ActionsBatch>> = match &predecessor {
        Some(file) => {
            read_standalone_checkpoint_file_actions(engine, &file.location, read_schema.clone())?
        }
        None => Box::new(iter::empty()),
    };
    let actions = window.read_actions(engine, read_schema)?.chain(inline);
    let reconciled = actions
        .map(move |batch| processor.process_actions_batch(batch?))
        .filter_ok(HasSelectionVector::has_selected_rows);
    Ok((output_schema, transform_batches(reconciled, evaluator)))
}

/// Returns the data of the standalone checkpoint at `action`'s version, with the output schema of
/// its batches: the reconciled gap file actions, then one batch of the non-file rows.
pub(super) fn standalone_checkpoint_data(
    snapshot: &SnapshotRef,
    engine: &dyn Engine,
    action: CheckpointAction,
) -> KernelResult<(ActionReconciliationIterator, SchemaRef)> {
    let (output_schema, gap) = gap_file_actions(snapshot, engine, &action)?;
    let rows = action.into_standalone_rows(engine, &output_schema)?;
    let non_file = ActionReconciliationBatch {
        actions_count: rows.len() as i64,
        filtered_data: FilteredEngineData::with_all_rows_selected(rows),
        add_actions_count: 0,
    };
    let batches = iter::once(Ok(non_file)).chain(gap);
    Ok((
        ActionReconciliationIterator::new(Box::new(batches)),
        output_schema,
    ))
}

/// Computes the non-file state of the standalone checkpoint at `snapshot`'s version n, as a
/// [`CheckpointAction`] for version n. `base` is `snapshot`'s latest checkpoint action. The result
/// carries no file actions; [`gap_file_actions`] supplies those.
///
/// Txn and domain metadata changes in `(base.version(), n]` are merged over `base`'s state.
/// Sidecars are reused per type: when no commit in the window changed that type, its sidecars are
/// referenced unchanged and never read. Otherwise they are read and inlined, and the references are
/// dropped.
///
/// # Errors
///
/// Returns an error if `snapshot` lists a checkpoint at or after `base`'s version that is not a
/// standalone checkpoint at that version.
pub(super) fn compute_standalone_non_file_state(
    snapshot: &SnapshotRef,
    engine: &dyn Engine,
    base: &CheckpointAction,
) -> KernelResult<CheckpointAction> {
    // The predecessor's file actions are gap_file_actions' concern. Here the listed checkpoint
    // only has to be one whose state `base` carries.
    listed_predecessor(snapshot, engine)?;
    let base_version = non_negative_version(base.version(), "Checkpoint version")?;

    let window = snapshot.log_segment().segment_after_version(base_version);
    // TODO: the window is read here for transactions and domain metadata and again for file
    // actions. A single pass would avoid the extra commit JSON reads.
    let window_transactions =
        scan_application_transactions(window.read_actions(engine, LOG_TXN_SCHEMA.clone())?, None)?;
    let window_domain_metadata = window.scan_tail_including_tombstones(None, engine)?;
    let transactions_changed = !window_transactions.is_empty();
    // System domains are always inline, so only a user domain change makes sidecars stale.
    let user_domain_metadata_changed = window_domain_metadata.values().any(|dm| !dm.is_internal());

    let log_root = LogRoot::new(snapshot.table_root().clone())?;
    // TODO(#2866): spill new or large inline txn and domain metadata sets into sidecars once a
    // sidecar writer exists.
    let (base_transactions, txn_sidecars) = if transactions_changed {
        let spilled = read_checkpoint_action(engine, base, &log_root, LOG_TXN_SCHEMA.clone())?;
        (scan_application_transactions(spilled, None)?, vec![])
    } else {
        let inline = base
            .transactions
            .iter()
            .map(|txn| (txn.app_id.clone(), txn.clone()))
            .collect();
        (inline, base.txn_sidecars.clone())
    };
    let (base_domain_metadata, domain_metadata_sidecars) = if user_domain_metadata_changed {
        let spilled =
            read_checkpoint_action(engine, base, &log_root, LOG_DOMAIN_METADATA_SCHEMA.clone())?;
        (scan_domain_metadata_actions(spilled, None)?, vec![])
    } else {
        let inline = base
            .domain_metadata
            .iter()
            .map(|dm| (dm.domain().to_string(), dm.clone()))
            .collect();
        (inline, base.domain_metadata_sidecars.clone())
    };

    let (domain_metadata, transactions) = merge_txns_and_domain_metadata(
        base_domain_metadata,
        base_transactions,
        window_domain_metadata.into_values(),
        window_transactions.into_values(),
        snapshot.table_configuration().table_properties(),
    )?;
    let table_config = snapshot.table_configuration();
    Ok(CheckpointAction {
        version: version_as_i64(snapshot.version())?,
        content_root: base.content_root.clone(),
        protocol: table_config.protocol().clone(),
        metadata: table_config.metadata().clone(),
        transactions: transactions.into_values().collect(),
        domain_metadata: domain_metadata.into_values().collect(),
        txn_sidecars,
        domain_metadata_sidecars,
    })
}

/// Returns the listed standalone checkpoint file that `snapshot`'s latest checkpoint action is read
/// from, or `None` when `snapshot` lists no checkpoint at or after that action's version.
///
/// That file holds the inline file actions of `(contentRoot.version, action.version]`, so a window
/// starting at the action reads it and no commit at or below its version.
///
/// # Errors
///
/// Returns an error if `snapshot` lists a checkpoint at or after the latest checkpoint action's
/// version that is not a standalone checkpoint at that version.
fn listed_predecessor(
    snapshot: &SnapshotRef,
    engine: &dyn Engine,
) -> KernelResult<Option<ParsedLogPath<FileMeta>>> {
    let Some(resolved) = snapshot.resolved_checkpoint_action(engine)? else {
        return Ok(None);
    };
    let base = &resolved.action;
    let Some(listed_version) = snapshot.log_segment().checkpoint_version else {
        return Ok(None);
    };
    if version_as_i64(listed_version)? < base.version() {
        return Ok(None);
    }
    match &resolved.source {
        CheckpointActionSource::Standalone { file, .. } if file.version == listed_version => {
            Ok(Some(file.clone()))
        }
        _ => Err(KernelError::unsupported(format!(
            "cannot write a standalone checkpoint: the listed checkpoint at version \
             {listed_version} is not the standalone checkpoint of the latest checkpoint action \
             (version {})",
            base.version()
        ))),
    }
}

/// Converts a version read from a checkpoint action, naming `what` in the error.
fn non_negative_version(value: i64, what: &str) -> KernelResult<Version> {
    Version::try_from(value)
        .map_err(|_| KernelError::generic(format!("{what} {value} must not be negative")))
}

#[cfg(test)]
mod tests {
    use std::iter;
    use std::sync::Arc;

    use rstest::rstest;
    use serde_json::json;
    use url::Url;

    use super::*;
    use crate::actions::{DomainMetadata, SetTransaction};
    use crate::arrow::array::{Array as _, AsArray as _, RecordBatch, StringArray};
    use crate::engine::arrow_data::EngineDataArrowExt as _;
    use crate::engine::sync::SyncEngine;
    use crate::expressions::Scalar;
    use crate::object_store::memory::InMemory;
    use crate::path::ParsedLogPath;
    use crate::unit_test_utils::adaptive_metadata_fixtures::{
        adaptive_metadata_pm, adaptive_metadata_protocol_and_metadata, minimal_checkpoint_action,
        protocol_and_metadata_lines, setup_adaptive_metadata_table, setup_table,
        write_commit_after_manifest_commit, write_ict_commit, write_manifest_commit,
        write_manifest_commit_keeping_pm, write_sidecar,
    };
    use crate::unit_test_utils::{parse_json_batch, test_schema_flat_with_column_mapping};
    use crate::{EngineData, Snapshot};

    /// The `CheckpointAction` version the manifest commits in these tests carry.
    const MANIFEST_VERSION: Version = 1;

    const STATS: &str = r#"{"numRecords":3}"#;

    fn add_action(path: &str) -> serde_json::Value {
        json!({ "add": { "path": path, "partitionValues": {}, "size": 1,
                         "modificationTime": 0, "dataChange": true } })
    }

    fn add(path: &str) -> String {
        add_action(path).to_string()
    }

    fn add_with_stats(path: &str, stats: &str) -> String {
        let mut action = add_action(path);
        action["add"]["stats"] = json!(stats);
        action.to_string()
    }

    fn remove(path: &str) -> String {
        json!({ "remove": { "path": path, "dataChange": true } }).to_string()
    }

    fn txn(app_id: &str) -> String {
        json!({ "txn": { "appId": app_id, "version": 1 } }).to_string()
    }

    fn commit_data(lines: &[String]) -> Box<dyn EngineData> {
        parse_json_batch(StringArray::from(lines.to_vec()))
    }

    fn strings(paths: &[&str]) -> Vec<String> {
        paths.iter().map(ToString::to_string).collect()
    }

    /// Writes a manifest commit at [`MANIFEST_VERSION`] whose content root is at that version. The
    /// table's checkpoints write JSON stats iff `write_stats_as_json`.
    fn setup_manifest_commit(write_stats_as_json: bool) -> (SyncEngine, Url) {
        let engine = SyncEngine::new_with_store(Arc::new(InMemory::new()));
        let table_root = Url::parse("memory:///").unwrap();
        let (protocol, metadata) =
            adaptive_metadata_pm(test_schema_flat_with_column_mapping(), &[]);
        let mut metadata = serde_json::to_value(metadata).unwrap();
        metadata["configuration"]["delta.checkpoint.writeStatsAsJson"] =
            json!(write_stats_as_json.to_string());
        let lines = format!(
            "{}\n{}",
            json!({ "protocol": protocol }),
            json!({ "metaData": metadata })
        );
        write_ict_commit(&engine, &table_root, 0, None, &lines);
        let checkpoint =
            minimal_checkpoint_action("metadata/root.parquet", MANIFEST_VERSION).unwrap();
        write_manifest_commit_keeping_pm(&engine, &table_root, MANIFEST_VERSION, checkpoint)
            .unwrap();
        (engine, table_root)
    }

    /// A snapshot at version 3 whose listed classic checkpoint at version 2 has no content root.
    /// The manifest commit at [`MANIFEST_VERSION`] is the latest checkpoint action the windows
    /// would start from.
    fn snapshot_after_classic_predecessor() -> (SyncEngine, SnapshotRef) {
        let (engine, table_root) = setup_manifest_commit(true);
        let checkpoint_path = ParsedLogPath::new_classic_parquet_checkpoint(&table_root, 2)
            .unwrap()
            .location;
        let pm_data = parse_json_batch(StringArray::from(
            protocol_and_metadata_lines(test_schema_flat_with_column_mapping(), &[])
                .lines()
                .map(String::from)
                .collect::<Vec<_>>(),
        ));
        engine
            .parquet_handler()
            .write_parquet_file(checkpoint_path, Box::new(iter::once(Ok(pm_data))))
            .unwrap();
        write_commit_after_manifest_commit(
            &engine,
            &table_root,
            3,
            MANIFEST_VERSION,
            commit_data(&[add("a")]),
        )
        .unwrap();
        let snapshot = Snapshot::builder_for(table_root)
            .at_version(3)
            .build(&engine)
            .unwrap();
        (engine, snapshot)
    }

    fn assert_classic_predecessor_error(err: &KernelError) {
        let message = err.to_string();
        assert!(
            message.contains("version 2") && message.contains("version 1"),
            "unexpected error: {message}"
        );
    }

    /// The selected rows of the gap file actions of the table at `version`.
    fn gap_batches(engine: &dyn Engine, table_root: &Url, version: Version) -> Vec<RecordBatch> {
        let snapshot = Snapshot::builder_for(table_root.clone())
            .at_version(version)
            .build(engine)
            .unwrap();
        let action = snapshot.latest_checkpoint_action(engine).unwrap().unwrap();
        let (_, batches) = gap_file_actions(&snapshot, engine, &action).unwrap();
        batches
            .map(|batch| {
                batch
                    .unwrap()
                    .filtered_data
                    .apply_selection_vector()
                    .unwrap()
                    .try_into_record_batch()
                    .unwrap()
            })
            .collect()
    }

    /// The `(adds, removes)` paths of the gap file actions of the table at `version`. Asserts
    /// that every selected row is a file action.
    fn gap_paths(
        engine: &dyn Engine,
        table_root: &Url,
        version: Version,
    ) -> (Vec<String>, Vec<String>) {
        let (mut adds, mut removes) = (vec![], vec![]);
        for record_batch in gap_batches(engine, table_root, version) {
            let add = record_batch.column_by_name("add").unwrap().as_struct();
            let remove = record_batch.column_by_name("remove").unwrap().as_struct();
            let add_paths = add.column_by_name("path").unwrap().as_string::<i32>();
            let remove_paths = remove.column_by_name("path").unwrap().as_string::<i32>();
            for row in 0..record_batch.num_rows() {
                assert!(
                    add.is_valid(row) || remove.is_valid(row),
                    "non-file action selected at row {row}"
                );
                if add.is_valid(row) {
                    adds.push(add_paths.value(row).to_string());
                } else if remove.is_valid(row) {
                    removes.push(remove_paths.value(row).to_string());
                }
            }
        }
        adds.sort();
        removes.sort();
        (adds, removes)
    }

    #[rstest]
    #[case::adds_survive(
        vec![vec![add("a"), add("b")]],
        &["a", "b"],
        &[],
    )]
    #[case::tree_remove_with_null_deletion_timestamp_survives(
        vec![vec![remove("tree.parquet")]],
        &[],
        &["tree.parquet"],
    )]
    #[case::remove_keeps_add_cancelled_in_window(
        vec![vec![add("a")], vec![remove("a")]],
        &[],
        &["a"],
    )]
    #[case::non_file_actions_are_not_selected(
        vec![vec![txn("app"), add("a")]],
        &["a"],
        &[],
    )]
    fn gap_file_actions_reconciles_window_commits(
        #[case] commits: Vec<Vec<String>>,
        #[case] expected_adds: &[&str],
        #[case] expected_removes: &[&str],
    ) {
        let (engine, table_root) = setup_manifest_commit(true);
        for (i, lines) in commits.iter().enumerate() {
            let version = MANIFEST_VERSION + 1 + i as Version;
            write_commit_after_manifest_commit(
                &engine,
                &table_root,
                version,
                MANIFEST_VERSION,
                commit_data(lines),
            )
            .unwrap();
        }
        let last_version = MANIFEST_VERSION + commits.len() as Version;

        let (adds, removes) = gap_paths(&engine, &table_root, last_version);
        assert_eq!(adds, strings(expected_adds));
        assert_eq!(removes, strings(expected_removes));
    }

    #[rstest]
    #[case::checkpoints_write_json_stats(true)]
    #[case::checkpoints_do_not_write_json_stats(false)]
    fn gap_file_actions_keeps_add_stats(#[case] write_stats_as_json: bool) {
        let (engine, table_root) = setup_manifest_commit(write_stats_as_json);
        write_commit_after_manifest_commit(
            &engine,
            &table_root,
            MANIFEST_VERSION + 1,
            MANIFEST_VERSION,
            commit_data(&[add_with_stats("a", STATS)]),
        )
        .unwrap();

        let stats: Vec<String> = gap_batches(&engine, &table_root, MANIFEST_VERSION + 1)
            .iter()
            .flat_map(|record_batch| {
                let add = record_batch.column_by_name("add").unwrap().as_struct();
                let stats = add.column_by_name("stats").unwrap().as_string::<i32>();
                (0..record_batch.num_rows())
                    .filter(|&row| add.is_valid(row))
                    .map(|row| stats.value(row).to_string())
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(stats, [STATS]);
    }

    #[test]
    fn gap_file_actions_excludes_commits_at_or_below_content_root() {
        let (engine, table_root) = setup_adaptive_metadata_table();
        write_ict_commit(&engine, &table_root, 1, None, &add("before"));
        let checkpoint = minimal_checkpoint_action("metadata/root.parquet", 2).unwrap();
        write_manifest_commit_keeping_pm(&engine, &table_root, 2, checkpoint).unwrap();
        write_commit_after_manifest_commit(
            &engine,
            &table_root,
            3,
            2,
            commit_data(&[add("after")]),
        )
        .unwrap();

        let (adds, removes) = gap_paths(&engine, &table_root, 3);
        assert_eq!(adds, strings(&["after"]));
        assert!(removes.is_empty());
    }

    #[test]
    fn gap_file_actions_rejects_classic_predecessor() {
        let (engine, snapshot) = snapshot_after_classic_predecessor();
        let action = minimal_checkpoint_action("metadata/root.parquet", MANIFEST_VERSION).unwrap();
        let err = gap_file_actions(&snapshot, &engine, &action).err().unwrap();
        assert_classic_predecessor_error(&err);
    }

    #[test]
    fn gap_file_actions_reads_commits_after_standalone_predecessor() {
        let (engine, table_root) = setup_manifest_commit(true);
        write_commit_after_manifest_commit(
            &engine,
            &table_root,
            MANIFEST_VERSION + 1,
            MANIFEST_VERSION,
            commit_data(&[add("a")]),
        )
        .unwrap();
        write_commit_after_manifest_commit(
            &engine,
            &table_root,
            MANIFEST_VERSION + 2,
            MANIFEST_VERSION,
            commit_data(&[add("b")]),
        )
        .unwrap();
        let predecessor_version = MANIFEST_VERSION + 2;
        let snapshot = Snapshot::builder_for(table_root.clone())
            .build(&engine)
            .unwrap();
        snapshot.checkpoint(&engine, None).unwrap();
        write_commit_after_manifest_commit(
            &engine,
            &table_root,
            predecessor_version + 1,
            MANIFEST_VERSION,
            commit_data(&[remove("a"), add("c")]),
        )
        .unwrap();

        let (adds, removes) = gap_paths(&engine, &table_root, predecessor_version + 1);
        assert_eq!(adds, strings(&["b", "c"]));
        assert_eq!(removes, strings(&["a"]));
    }

    fn dm(domain: &str, removed: bool) -> String {
        json!({ "domainMetadata": { "domain": domain, "configuration": "cfg", "removed": removed } })
            .to_string()
    }

    fn txn_row(app_id: &str) -> Scalar {
        SetTransaction::new(app_id.to_string(), 1, None).into()
    }

    fn dm_row(domain: &str) -> Scalar {
        DomainMetadata::new(domain.to_string(), "cfg".to_string()).into()
    }

    /// A manifest commit at [`MANIFEST_VERSION`] whose checkpoint action holds `inline-app` and
    /// `user.inline` inline, and `sidecar-app` and `user.sidecar` in one txn sidecar and one
    /// domain metadata sidecar. Its content root carries tags.
    fn setup_with_sidecars() -> (SyncEngine, Url) {
        let (engine, table_root) = setup_table().unwrap();
        let txn_sidecar = write_sidecar(
            &engine,
            &table_root,
            "txns.parquet",
            LOG_TXN_SCHEMA.clone(),
            vec![txn_row("sidecar-app")],
        );
        let dm_sidecar = write_sidecar(
            &engine,
            &table_root,
            "dms.parquet",
            LOG_DOMAIN_METADATA_SCHEMA.clone(),
            vec![dm_row("user.sidecar")],
        );
        let mut checkpoint =
            minimal_checkpoint_action("metadata/root.parquet", MANIFEST_VERSION).unwrap();
        checkpoint.content_root = serde_json::from_value(json!({
            "path": "metadata/root.parquet",
            "sizeInBytes": 1024,
            "version": MANIFEST_VERSION,
            "tags": { "writer": "test" },
        }))
        .unwrap();
        checkpoint.transactions = vec![SetTransaction::new("inline-app".to_string(), 1, None)];
        checkpoint.domain_metadata = vec![DomainMetadata::new(
            "user.inline".to_string(),
            "cfg".to_string(),
        )];
        checkpoint.txn_sidecars = vec![txn_sidecar];
        checkpoint.domain_metadata_sidecars = vec![dm_sidecar];
        write_manifest_commit(&engine, &table_root, MANIFEST_VERSION, checkpoint).unwrap();
        (engine, table_root)
    }

    /// Writes `window` as the commit at version 2 and returns the snapshot at that version, with
    /// its latest checkpoint action.
    fn snapshot_with_window(
        engine: &SyncEngine,
        table_root: &Url,
        window: &[String],
    ) -> (SnapshotRef, Arc<CheckpointAction>) {
        write_commit_after_manifest_commit(
            engine,
            table_root,
            2,
            MANIFEST_VERSION,
            commit_data(window),
        )
        .unwrap();
        let snapshot = Snapshot::builder_for(table_root.clone())
            .build(engine)
            .unwrap();
        let base = snapshot.latest_checkpoint_action(engine).unwrap().unwrap();
        (snapshot, base)
    }

    #[rstest]
    #[case::no_change_reuses_both_sidecar_types(
        vec![add("a")],
        &["inline-app"],
        &["user.inline"],
        true,
        true,
    )]
    #[case::txn_change_inlines_txn_sidecars(
        vec![txn("window-app")],
        &["inline-app", "sidecar-app", "window-app"],
        &["user.inline"],
        false,
        true,
    )]
    #[case::user_dm_removal_inlines_dm_sidecars(
        vec![dm("user.sidecar", true)],
        &["inline-app"],
        &["user.inline"],
        true,
        false,
    )]
    #[case::system_dm_change_keeps_dm_sidecars(
        vec![dm("delta.sys", false)],
        &["inline-app"],
        &["delta.sys", "user.inline"],
        true,
        true,
    )]
    fn non_file_state_reuses_sidecars_per_type(
        #[case] window: Vec<String>,
        #[case] expected_app_ids: &[&str],
        #[case] expected_domains: &[&str],
        #[case] reuses_txn_sidecars: bool,
        #[case] reuses_dm_sidecars: bool,
    ) {
        let (engine, table_root) = setup_with_sidecars();
        let (snapshot, base) = snapshot_with_window(&engine, &table_root, &window);

        let action = compute_standalone_non_file_state(&snapshot, &engine, &base).unwrap();

        assert_eq!(action.version(), 2);
        assert_eq!(action.content_root, base.content_root);
        let mut app_ids: Vec<_> = action
            .transactions
            .iter()
            .map(|txn| txn.app_id.as_str())
            .collect();
        app_ids.sort_unstable();
        assert_eq!(app_ids, expected_app_ids);
        let mut domains: Vec<_> = action
            .domain_metadata
            .iter()
            .map(|dm| dm.domain())
            .collect();
        domains.sort_unstable();
        assert_eq!(domains, expected_domains);
        let expected_txn_sidecars = if reuses_txn_sidecars {
            base.txn_sidecars.clone()
        } else {
            vec![]
        };
        assert_eq!(action.txn_sidecars, expected_txn_sidecars);
        let expected_dm_sidecars = if reuses_dm_sidecars {
            base.domain_metadata_sidecars.clone()
        } else {
            vec![]
        };
        assert_eq!(action.domain_metadata_sidecars, expected_dm_sidecars);
    }

    /// The inline merge drops transactions older than the table's retention, including those the
    /// window adds, and keeps the rest.
    #[test]
    fn non_file_state_drops_expired_transactions() {
        let (engine, table_root) = setup_with_sidecars();
        let (_, metadata) = adaptive_metadata_pm(test_schema_flat_with_column_mapping(), &[]);
        let mut metadata = serde_json::to_value(&metadata).unwrap();
        metadata["configuration"]["delta.setTransactionRetentionDuration"] =
            json!("interval 1 day");
        let window = [
            json!({ "metaData": metadata }).to_string(),
            json!({ "txn": { "appId": "expired-app", "version": 1, "lastUpdated": 1 } })
                .to_string(),
        ];
        let (snapshot, base) = snapshot_with_window(&engine, &table_root, &window);

        let action = compute_standalone_non_file_state(&snapshot, &engine, &base).unwrap();

        let app_ids: Vec<_> = action
            .transactions
            .iter()
            .map(|txn| txn.app_id.as_str())
            .collect();
        assert!(app_ids.contains(&"inline-app"), "{app_ids:?}");
        assert!(!app_ids.contains(&"expired-app"), "{app_ids:?}");
    }

    #[test]
    fn non_file_state_takes_protocol_and_metadata_from_snapshot() {
        let (engine, table_root) = setup_with_sidecars();
        let (_, metadata) = adaptive_metadata_protocol_and_metadata();
        let mut metadata = serde_json::to_value(metadata).unwrap();
        metadata["description"] = json!("changed");
        let window = [json!({ "metaData": metadata }).to_string()];
        let (snapshot, base) = snapshot_with_window(&engine, &table_root, &window);

        let action = compute_standalone_non_file_state(&snapshot, &engine, &base).unwrap();

        assert_eq!(base.metadata().description(), None);
        assert_eq!(action.metadata().description(), Some("changed"));
        assert_eq!(action.metadata(), snapshot.table_configuration().metadata());
        assert_eq!(action.protocol(), snapshot.table_configuration().protocol());
    }

    #[test]
    fn non_file_state_rejects_classic_predecessor() {
        let (engine, snapshot) = snapshot_after_classic_predecessor();
        let action = minimal_checkpoint_action("metadata/root.parquet", MANIFEST_VERSION).unwrap();
        let err = compute_standalone_non_file_state(&snapshot, &engine, &action)
            .err()
            .unwrap();
        assert_classic_predecessor_error(&err);
    }
}
