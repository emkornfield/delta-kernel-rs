//! File actions of a standalone adaptiveMetadata checkpoint.
//!
//! A standalone checkpoint at version n carries, inline, the reconciled add and remove actions of
//! the commits after its latest checkpoint action's content root, `(contentRoot.version, n]`.
//! Those commits are not covered by the metadata tree the content root references.

use itertools::Itertools;

use super::{
    CheckpointWriter, STANDALONE_CHECKPOINT_ACTIONS_SCHEMA, checkpoint_output_transform,
    transform_batches,
};
use crate::action_reconciliation::file_metadata::build_checkpoint_read_schema;
use crate::action_reconciliation::log_replay::{
    ActionReconciliationBatch, ActionReconciliationProcessor, RemoveRetention,
};
use crate::actions::CheckpointAction;
use crate::log_replay::HasSelectionVector;
use crate::snapshot::SnapshotRef;
use crate::{Engine, KernelError, KernelResult, KernelResultIterator, Version, version_as_i64};

/// Returns the reconciled file actions of the commits in `(action.contentRoot.version, n]`, where
/// `snapshot` is at version n and `action` is its latest checkpoint action. Each batch is in the
/// checkpoint output schema, so it can be chained with the standalone checkpoint's other rows.
///
/// Every remove in the window is kept, including removes that cancel an add in the window. Adds
/// and removes are copied verbatim.
///
/// # Errors
///
/// Returns an error if `snapshot` was checkpointed by a standalone checkpoint, whose inline file
/// actions are not yet carried into the window, or if the table uses collation-aware stats.
#[allow(dead_code)]
pub(crate) fn gap_file_actions(
    snapshot: &SnapshotRef,
    engine: &dyn Engine,
    action: &CheckpointAction,
) -> KernelResult<impl KernelResultIterator<ActionReconciliationBatch>> {
    let log_segment = snapshot.log_segment();
    // TODO: lift once a standalone predecessor's inline file actions are carried into the window.
    if let Some(standalone_version) = log_segment.checkpoint_version
        && version_as_i64(standalone_version)? >= action.version
    {
        return Err(KernelError::unsupported(format!(
            "Checkpointing after the standalone checkpoint at version {standalone_version} is not \
             yet supported; its checkpoint action's version is {}",
            action.version
        )));
    }
    let content_root_version = action.content_root_version();
    let window_start = Version::try_from(content_root_version).map_err(|_| {
        KernelError::generic(format!(
            "Content root version {content_root_version} must not be negative"
        ))
    })?;

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
    let (_, evaluator) =
        checkpoint_output_transform(engine, &context, &read_schema, &context.stats_schema)?;

    // TODO: log compaction files spanning the content root version must not feed this window.
    // Compaction is disabled (#2337).
    let window = log_segment.segment_after_version(window_start);
    // TODO: fail when reconciliation drops a backReference: a re-add without one after a remove
    // with one, or a remove without one after an add with one. Checking it needs backReference
    // columns and per-key state in the visitor.
    let mut processor = ActionReconciliationProcessor::new(
        RemoveRetention::KeepAll,
        None,
        snapshot.table_configuration().dv_normalization_root(),
    )
    .with_file_actions_only();
    let actions = window.read_actions(engine, read_schema)?;
    let reconciled = actions
        .map(move |batch| processor.process_actions_batch(batch?))
        .filter_ok(HasSelectionVector::has_selected_rows);
    Ok(transform_batches(reconciled, evaluator))
}

#[cfg(test)]
mod tests {
    use std::iter;
    use std::sync::Arc;

    use rstest::rstest;
    use serde_json::json;
    use url::Url;

    use super::*;
    use crate::arrow::array::{Array as _, AsArray as _, RecordBatch, StringArray};
    use crate::engine::arrow_data::EngineDataArrowExt as _;
    use crate::engine::sync::SyncEngine;
    use crate::object_store::memory::InMemory;
    use crate::path::ParsedLogPath;
    use crate::unit_test_utils::adaptive_metadata_fixtures::{
        adaptive_metadata_pm, minimal_checkpoint_action, protocol_and_metadata_lines,
        setup_adaptive_metadata_table, write_commit_after_manifest_commit, write_ict_commit,
        write_manifest_commit_keeping_pm,
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

    /// The selected rows of the gap file actions of the table at `version`.
    fn gap_batches(engine: &dyn Engine, table_root: &Url, version: Version) -> Vec<RecordBatch> {
        let snapshot = Snapshot::builder_for(table_root.clone())
            .at_version(version)
            .build(engine)
            .unwrap();
        let action = snapshot.latest_checkpoint_action(engine).unwrap().unwrap();
        gap_file_actions(&snapshot, engine, &action)
            .unwrap()
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
    fn gap_file_actions_rejects_standalone_predecessor() {
        let (engine, table_root) = setup_manifest_commit(true);
        // A listed checkpoint at version 2 stands in for a standalone predecessor: the window
        // would need its inline file actions, which are not carried yet.
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

        let snapshot = Snapshot::builder_for(table_root.clone())
            .at_version(3)
            .build(&engine)
            .unwrap();
        // The pointer names a commit this segment does not list, so the action is built directly.
        let action = minimal_checkpoint_action("metadata/root.parquet", MANIFEST_VERSION).unwrap();
        let err = gap_file_actions(&snapshot, &engine, &action).err().unwrap();
        let message = err.to_string();
        assert!(
            message.contains("version 2") && message.contains("version is 1"),
            "unexpected error: {message}"
        );
    }
}
