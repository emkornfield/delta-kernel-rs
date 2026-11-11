//! Scans of adaptiveMetadata (AMT) tables, whose state at or below the latest `checkpoint` action
//! is read from that action's content tree.

use std::sync::Arc;

use futures::executor::block_on;
use itertools::Itertools as _;
use rstest::rstest;
use serde_json::json;
use test_utils::delta_path_for_version;
use url::Url;

use super::get_files_for_scan;
use crate::actions::{DomainMetadata, LOG_DOMAIN_METADATA_SCHEMA};
use crate::arrow::array::{Array as _, AsArray as _, StringArray};
use crate::checkpoint::STANDALONE_CHECKPOINT_ACTIONS_SCHEMA;
use crate::content_tree::test_utils::{
    added_data_entry, data_manifest_entry, test_dv_info, write_manifest,
};
use crate::content_tree::{
    ContentTreeNodeEntry, TrackingInfo, TrackingStatus, read_standalone_checkpoint_file_actions,
};
use crate::engine::arrow_data::ArrowEngineData;
use crate::engine::sync::SyncEngine;
use crate::expressions::{Predicate as Pred, col, lit};
use crate::log_segment::CheckpointActionSource;
use crate::object_store::ObjectStoreExt as _;
use crate::object_store::memory::InMemory;
use crate::scan::{Scan, ScanMetadata};
use crate::schema::schema_ref;
use crate::transaction::BASE_ADD_FILES_SCHEMA;
use crate::unit_test_utils::adaptive_metadata_fixtures::{
    checkpoint_action_with_root, commit_file_changes, commit_files, commit_root_manifest,
    commit_root_manifest_file, create_adaptive_metadata_table, setup_adaptive_metadata_table,
    write_commit_after_manifest_commit, write_manifest_commit_keeping_pm, write_sidecar,
};
use crate::unit_test_utils::{
    assert_result_error_with_message, parse_json_batch, string_array_to_engine_data,
};
use crate::{Engine, EngineData, FileMeta, KernelResult, Snapshot, Version};

const TABLE_ROOT: &str = "memory:///";

/// The manifest commit that the standalone chain tests build on.
const MANIFEST_VERSION: Version = 1;

fn table_root() -> Url {
    Url::parse(TABLE_ROOT).unwrap()
}

fn deleted_entry(path: &str) -> ContentTreeNodeEntry {
    let mut entry = added_data_entry(path, 1, 1, 0, 0);
    entry.tracking.status = TrackingStatus::Deleted;
    entry
}

/// A table whose log is:
/// - v0: protocol and metadata;
/// - v1: an add of `stale.parquet`, which no tree lists;
/// - v2: a checkpoint action whose tree lists `a`, `b`, and a deleted `gone`;
/// - v3: an add of `g` and a remove of `a`;
/// - v4: a checkpoint action whose tree lists `b`, `g`, and `h`.
fn two_checkpoint_table() -> SyncEngine {
    let (engine, _) = setup_adaptive_metadata_table();
    commit_files(&engine, &table_root(), &["stale.parquet"], &[]);
    let tree1 = [
        added_data_entry("a.parquet", 1, 1, 0, 0),
        deleted_entry("gone.parquet"),
        added_data_entry("b.parquet", 1, 1, 1, 0),
    ];
    commit_root_manifest(&engine, &table_root(), "metadata/root-2.parquet", &tree1);
    commit_files(&engine, &table_root(), &["g.parquet"], &["a.parquet"]);

    // A root commit refuses commits pending since the last checkpoint action, so the second
    // checkpoint action, which folds in v3, is written directly.
    let tree2 = [
        added_data_entry("b.parquet", 1, 1, 1, 0),
        added_data_entry("g.parquet", 1, 1, 2, 2),
        added_data_entry("h.parquet", 1, 1, 3, 3),
    ];
    let root_path = "metadata/root-4.parquet";
    let root = write_manifest(&engine, table_root().join(root_path).unwrap(), &tree2);
    let checkpoint = checkpoint_action_with_root(root_path, root.size as i64, 4).unwrap();
    write_manifest_commit_keeping_pm(&engine, &table_root(), 4, checkpoint).unwrap();
    engine
}

fn snapshot_at(engine: &SyncEngine, version: Version) -> Arc<Snapshot> {
    Snapshot::builder_for(TABLE_ROOT)
        .at_version(version)
        .build(engine)
        .unwrap()
}

fn scan_paths(engine: &SyncEngine, snapshot: Arc<Snapshot>) -> Vec<String> {
    let scan = snapshot.scan_builder().build().unwrap();
    get_files_for_scan(scan, engine)
        .unwrap()
        .into_iter()
        .sorted()
        .collect()
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|v| v.to_string()).collect()
}

#[rstest]
#[case::at_first_checkpoint(2, &["a.parquet", "b.parquet"])]
#[case::commits_after_first_checkpoint(3, &["b.parquet", "g.parquet"])]
#[case::at_second_checkpoint(4, &["b.parquet", "g.parquet", "h.parquet"])]
fn scan_reads_latest_tree_then_later_commits(#[case] version: Version, #[case] expected: &[&str]) {
    let engine = two_checkpoint_table();
    assert_eq!(
        scan_paths(&engine, snapshot_at(&engine, version)),
        strings(expected)
    );
}

/// A table whose log is:
/// - v0: protocol and metadata;
/// - v1: a checkpoint action whose tree lists `a` and `b`;
/// - v2: a standalone checkpoint whose inline rows add `c` and remove `a`;
/// - v3: an add of `e`;
/// - v4: an add of `f` and a remove of `b`.
fn standalone_checkpoint_table() -> SyncEngine {
    let (engine, _) = setup_adaptive_metadata_table();
    commit_root_manifest(
        &engine,
        &table_root(),
        "metadata/root-1.parquet",
        &[
            added_data_entry("a.parquet", 1, 1, 0, 0),
            added_data_entry("b.parquet", 1, 1, 1, 0),
        ],
    );
    commit_files(&engine, &table_root(), &["c.parquet"], &["a.parquet"]);
    let snapshot = Snapshot::builder_for(TABLE_ROOT).build(&engine).unwrap();
    snapshot.checkpoint(&engine, None).unwrap();
    commit_files(&engine, &table_root(), &["e.parquet"], &[]);
    commit_files(&engine, &table_root(), &["f.parquet"], &["b.parquet"]);
    engine
}

#[rstest]
#[case::at_standalone_checkpoint(2, &["b.parquet", "c.parquet"])]
#[case::commit_after_standalone_checkpoint(3, &["b.parquet", "c.parquet", "e.parquet"])]
#[case::commit_removes_tree_file(4, &["c.parquet", "e.parquet", "f.parquet"])]
fn scan_chains_standalone_checkpoint_file_actions_then_tree(
    #[case] version: Version,
    #[case] expected: &[&str],
) {
    let engine = standalone_checkpoint_table();
    let snapshot = snapshot_at(&engine, version);
    assert_eq!(
        snapshot
            .latest_checkpoint_action(&engine)
            .unwrap()
            .map(|action| action.version()),
        Some(2)
    );
    assert_eq!(scan_paths(&engine, snapshot), strings(expected));
}

#[test]
fn incremental_update_reads_standalone_checkpoint_written_after_snapshot() {
    let (engine, _) = setup_adaptive_metadata_table();
    commit_root_manifest(
        &engine,
        &table_root(),
        "metadata/root-1.parquet",
        &[
            added_data_entry("a.parquet", 1, 1, 0, 0),
            added_data_entry("b.parquet", 1, 1, 1, 0),
        ],
    );
    let before = snapshot_at(&engine, 1);
    commit_files(&engine, &table_root(), &["c.parquet"], &["a.parquet"]);
    commit_files(&engine, &table_root(), &["d.parquet"], &[]);
    let latest = Snapshot::builder_for(TABLE_ROOT).build(&engine).unwrap();
    latest.checkpoint(&engine, None).unwrap();

    let updated = Snapshot::builder_from(before).build(&engine).unwrap();

    assert_eq!(updated.log_segment().checkpoint_version, Some(3));
    assert_eq!(
        scan_paths(&engine, updated),
        strings(&["b.parquet", "c.parquet", "d.parquet"])
    );
}

#[test]
fn scan_of_standalone_checkpoint_skips_inline_add_outside_predicate() {
    let (engine, _) = setup_adaptive_metadata_table();
    commit_root_manifest(
        &engine,
        &table_root(),
        "metadata/root-1.parquet",
        &[added_data_entry("tree.parquet", 1, 1, 0, 0)],
    );
    let adds = adds_with_id_ranges(
        &engine,
        &[("low.parquet", 1, 5), ("high.parquet", 200, 300)],
    );
    commit_file_changes(&engine, &table_root(), Some(adds), &[]);
    Snapshot::builder_for(TABLE_ROOT)
        .build(&engine)
        .unwrap()
        .checkpoint(&engine, None)
        .unwrap();

    let scan = snapshot_at(&engine, 2)
        .scan_builder()
        .with_predicate(Arc::new(Pred::gt(col!("id"), lit(100i64))))
        .build()
        .unwrap();
    let paths: Vec<_> = get_files_for_scan(scan, &engine)
        .unwrap()
        .into_iter()
        .sorted()
        .collect();
    assert_eq!(paths, strings(&["high.parquet", "tree.parquet"]));
}

/// A manifest commit after a standalone checkpoint supersedes it, so the scan reads only that
/// commit's tree.
#[test]
fn scan_after_manifest_commit_superseding_standalone_checkpoint_reads_its_tree() {
    let engine = standalone_checkpoint_table();
    // The write path refuses a manifest commit on a standalone checkpoint, so the commit is
    // written directly.
    write_manifest(
        &engine,
        table_root().join("metadata/root-5.parquet").unwrap(),
        &[added_data_entry("x.parquet", 1, 1, 0, 5)],
    );
    write_manifest_commit_keeping_pm(
        &engine,
        &table_root(),
        5,
        checkpoint_action_with_root("metadata/root-5.parquet", 1024, 5).unwrap(),
    )
    .unwrap();
    assert_eq!(
        scan_paths(&engine, snapshot_at(&engine, 5)),
        strings(&["x.parquet"])
    );
}

/// An inline remove of a deletion-vector tree entry cancels it, so only the kept entry remains.
#[test]
fn inline_remove_cancels_tree_entry_with_deletion_vector() {
    let (engine, _) = setup_adaptive_metadata_table();
    let dv_entry = ContentTreeNodeEntry {
        deletion_vector: Some(test_dv_info()),
        ..added_data_entry("dv.parquet", 1, 1, 0, 0)
    };
    commit_root_manifest(
        &engine,
        &table_root(),
        "metadata/root-1.parquet",
        &[dv_entry, added_data_entry("keep.parquet", 1, 1, 1, 0)],
    );
    commit_files(&engine, &table_root(), &[], &["dv.parquet"]);
    Snapshot::builder_for(TABLE_ROOT)
        .build(&engine)
        .unwrap()
        .checkpoint(&engine, None)
        .unwrap();

    assert_eq!(
        scan_paths(&engine, snapshot_at(&engine, 2)),
        strings(&["keep.parquet"])
    );
}

/// The inline remove names the tree entry by its URI-encoded Delta path, which is how the reader
/// matches it against the entry's location.
#[test]
#[ignore = "tree Add.path is the raw location, not its encoded form (#3319)"]
fn inline_remove_with_percent_encoded_path_cancels_tree_entry() {
    let (engine, _) = setup_adaptive_metadata_table();
    commit_root_manifest(
        &engine,
        &table_root(),
        "metadata/root-1.parquet",
        &[
            added_data_entry("my file.parquet", 1, 1, 0, 0),
            added_data_entry("keep.parquet", 1, 1, 1, 0),
        ],
    );
    let remove = json!({ "remove": { "path": "my%20file.parquet", "dataChange": true } });
    write_commit_after_manifest_commit(
        &engine,
        &table_root(),
        2,
        1,
        parse_json_batch(StringArray::from(vec![remove.to_string()])),
    )
    .unwrap();
    Snapshot::builder_for(TABLE_ROOT)
        .build(&engine)
        .unwrap()
        .checkpoint(&engine, None)
        .unwrap();

    assert_eq!(
        scan_paths(&engine, snapshot_at(&engine, 2)),
        strings(&["keep.parquet"])
    );
}

#[test]
fn inline_remove_cancels_leaf_manifest_entry() {
    let (engine, _) = setup_adaptive_metadata_table();
    let leaf_path = "metadata/leaf.parquet";
    let leaf = write_manifest(
        &engine,
        table_root().join(leaf_path).unwrap(),
        &[
            added_data_entry("l1.parquet", 1, 1, 0, 0),
            added_data_entry("l2.parquet", 1, 1, 1, 0),
        ],
    );
    let mut leaf_entry = data_manifest_entry(leaf_path, leaf.size as i64, TrackingStatus::Added);
    leaf_entry.tracking = TrackingInfo {
        sequence_number: Some(1),
        file_sequence_number: Some(1),
        first_row_id: Some(10),
        ..leaf_entry.tracking
    };
    commit_root_manifest(
        &engine,
        &table_root(),
        "metadata/root-1.parquet",
        &[leaf_entry, added_data_entry("r.parquet", 1, 1, 2, 0)],
    );
    commit_files(&engine, &table_root(), &[], &["l1.parquet"]);
    Snapshot::builder_for(TABLE_ROOT)
        .build(&engine)
        .unwrap()
        .checkpoint(&engine, None)
        .unwrap();

    assert_eq!(
        scan_paths(&engine, snapshot_at(&engine, 2)),
        strings(&["l2.parquet", "r.parquet"])
    );
}

#[test]
fn scan_reads_leaf_manifests() {
    let (engine, _) = setup_adaptive_metadata_table();
    let leaf_path = "metadata/leaf.parquet";
    let leaf = write_manifest(
        &engine,
        table_root().join(leaf_path).unwrap(),
        &[
            added_data_entry("l1.parquet", 1, 1, 0, 0),
            added_data_entry("l2.parquet", 1, 1, 1, 0),
        ],
    );
    let mut leaf_entry = data_manifest_entry(leaf_path, leaf.size as i64, TrackingStatus::Added);
    leaf_entry.tracking = TrackingInfo {
        sequence_number: Some(1),
        file_sequence_number: Some(1),
        first_row_id: Some(10),
        ..leaf_entry.tracking
    };
    let root = [added_data_entry("r.parquet", 1, 1, 0, 1), leaf_entry];
    let version = commit_root_manifest(&engine, &table_root(), "metadata/root.parquet", &root);

    assert_eq!(
        scan_paths(&engine, snapshot_at(&engine, version)),
        strings(&["l1.parquet", "l2.parquet", "r.parquet"])
    );
}

#[test]
fn scan_of_partitioned_table_with_checkpoint_action_is_unsupported() {
    let engine = SyncEngine::new_with_store(Arc::new(InMemory::new()));
    create_adaptive_metadata_table(&engine, &table_root(), &["id"]);
    let version = commit_root_manifest(
        &engine,
        &table_root(),
        "metadata/root.parquet",
        &[added_data_entry("a.parquet", 1, 1, 0, 0)],
    );
    let scan = snapshot_at(&engine, version)
        .scan_builder()
        .build()
        .unwrap();
    assert_result_error_with_message(
        scan.scan_metadata(&engine).map(|_| ()),
        r#"partition columns: ["id"]"#,
    );
}

/// A scan entry point that does not yet read the checkpoint action.
type EntryPoint = fn(&Scan, Arc<dyn Engine>) -> KernelResult<()>;

#[rstest]
#[case::parallel(
    "parallel_scan_metadata",
    (|scan, engine| scan.parallel_scan_metadata(engine).map(|_| ())) as EntryPoint
)]
#[cfg_attr(
    feature = "declarative-plans",
    case::declarative(
        "declarative_metadata_scan_plan",
        (|scan, engine| scan.declarative_metadata_scan_plan(engine.as_ref()).map(|_| ())) as EntryPoint
    )
)]
fn scan_entry_point_without_checkpoint_action_support_is_unsupported(
    #[case] name: &str,
    #[case] entry_point: EntryPoint,
) {
    let (engine, table_root) = setup_adaptive_metadata_table();
    let version = commit_root_manifest(
        &engine,
        &table_root,
        "metadata/root.parquet",
        &[added_data_entry("a.parquet", 1, 1, 0, 0)],
    );
    let scan = snapshot_at(&engine, version)
        .scan_builder()
        .build()
        .unwrap();
    assert_result_error_with_message(
        entry_point(&scan, Arc::new(engine)),
        &format!(
            "{name} does not yet read the adaptiveMetadata checkpoint action at version {version}"
        ),
    );
}

#[test]
fn scan_with_missing_root_manifest_names_it() {
    let (engine, _) = setup_adaptive_metadata_table();
    let file = FileMeta {
        location: table_root().join("metadata/root.parquet").unwrap(),
        last_modified: 0,
        size: 1,
    };
    let version = commit_root_manifest_file(&engine, &table_root(), file);
    let scan = snapshot_at(&engine, version)
        .scan_builder()
        .build()
        .unwrap();
    assert_result_error_with_message(get_files_for_scan(scan, &engine), "metadata/root.parquet");
}

/// Add-file write metadata for each `(path, min, max)`, with stats bounding the `id` column
/// (physical name `phys_id`) to `[min, max]`.
fn adds_with_id_ranges(engine: &SyncEngine, files: &[(&str, i64, i64)]) -> Box<dyn EngineData> {
    let schema = schema_ref! {
        ..(BASE_ADD_FILES_SCHEMA.fields().filter(|f| f.name() != "stats")),
        nullable "stats": {
            nullable "numRecords": LONG,
            nullable "nullCount": { nullable "phys_id": LONG },
            nullable "minValues": { nullable "phys_id": LONG },
            nullable "maxValues": { nullable "phys_id": LONG },
            nullable "tightBounds": BOOLEAN,
        },
    };
    let lines: StringArray = files
        .iter()
        .map(|(path, min, max)| {
            json!({
                "path": path,
                "partitionValues": {},
                "size": 1,
                "modificationTime": 1,
                "stats": {
                    "numRecords": 1,
                    "nullCount": { "phys_id": 0 },
                    "minValues": { "phys_id": min },
                    "maxValues": { "phys_id": max },
                    "tightBounds": true,
                },
            })
            .to_string()
        })
        .collect::<Vec<_>>()
        .into();
    engine
        .json_handler()
        .parse_json(string_array_to_engine_data(lines), schema)
        .unwrap()
}

#[test]
fn data_skipping_prunes_commit_files_but_keeps_tree_files_without_stats() {
    let (engine, _) = setup_adaptive_metadata_table();
    commit_root_manifest(
        &engine,
        &table_root(),
        "metadata/root.parquet",
        &[added_data_entry("tree.parquet", 1, 1, 0, 0)],
    );
    let adds = adds_with_id_ranges(
        &engine,
        &[("low.parquet", 1, 5), ("high.parquet", 200, 300)],
    );
    let version = commit_file_changes(&engine, &table_root(), Some(adds), &[]);

    let predicate = Arc::new(Pred::gt(col!("id"), lit(100i64)));
    let scan = snapshot_at(&engine, version)
        .scan_builder()
        .with_predicate(predicate)
        .build()
        .unwrap();
    let paths: Vec<_> = get_files_for_scan(scan, &engine)
        .unwrap()
        .into_iter()
        .sorted()
        .collect();
    assert_eq!(paths, strings(&["high.parquet", "tree.parquet"]));
}

/// The scan metadata of `snapshot`, with selection vectors applied, as cached by a caller of
/// `scan_metadata_from`.
fn cached_scan_metadata(engine: &SyncEngine, snapshot: Arc<Snapshot>) -> Vec<Box<dyn EngineData>> {
    let scan = snapshot.scan_builder().build().unwrap();
    scan.scan_metadata(engine)
        .unwrap()
        .map(|metadata| {
            let ScanMetadata { scan_files, .. } = metadata.unwrap();
            scan_files.apply_selection_vector().unwrap()
        })
        .collect()
}

#[rstest]
// From before the checkpoint action, the cached rows (here deliberately empty) are discarded and
// the table is fully rescanned: advancing commit by commit would miss `b`.
#[case::before_checkpoint_action(1, true, &["b.parquet", "g.parquet"])]
#[case::at_checkpoint_action(2, false, &["b.parquet", "g.parquet"])]
fn scan_metadata_from_hint_before_checkpoint_action_rescans(
    #[case] existing_version: Version,
    #[case] empty_cache: bool,
    #[case] expected: &[&str],
) {
    let engine = two_checkpoint_table();
    let existing_data = if empty_cache {
        vec![]
    } else {
        cached_scan_metadata(&engine, snapshot_at(&engine, existing_version))
    };
    let scan = snapshot_at(&engine, 3).scan_builder().build().unwrap();
    let mut paths = vec![];
    for metadata in scan
        .scan_metadata_from(&engine, existing_version, existing_data, None)
        .unwrap()
    {
        paths = metadata
            .unwrap()
            .visit_scan_files(paths, |paths: &mut Vec<String>, file| {
                paths.push(file.path.to_string())
            })
            .unwrap();
    }
    paths.sort();
    assert_eq!(paths, strings(expected));
}

/// A raw commit line adding `path`.
fn add_line(path: &str) -> serde_json::Value {
    json!({ "add": { "path": path, "partitionValues": {}, "size": 1,
                     "modificationTime": 0, "dataChange": true } })
}

/// A raw commit line removing `path`.
fn remove_line(path: &str) -> serde_json::Value {
    json!({ "remove": { "path": path, "dataChange": true } })
}

/// A raw commit line setting the transaction `app_id` to `version`.
fn txn_line(app_id: &str, version: i64) -> serde_json::Value {
    json!({ "txn": { "appId": app_id, "version": version } })
}

/// Writes `lines` as the commit at `version`, which follows the manifest commit at
/// [`MANIFEST_VERSION`] and so carries its pointer.
fn write_raw_commit(engine: &SyncEngine, version: Version, lines: &[serde_json::Value]) {
    let lines: Vec<String> = lines.iter().map(ToString::to_string).collect();
    write_commit_after_manifest_commit(
        engine,
        &table_root(),
        version,
        MANIFEST_VERSION,
        parse_json_batch(StringArray::from(lines)),
    )
    .unwrap();
}

/// The `(adds, removes)` paths inline in the standalone checkpoint file that `snapshot`'s latest
/// checkpoint action is read from.
fn inline_paths(engine: &SyncEngine, snapshot: &Snapshot) -> (Vec<String>, Vec<String>) {
    let Some(resolved) = snapshot.resolved_checkpoint_action(engine).unwrap() else {
        panic!("latest checkpoint action is not read from a standalone file");
    };
    let CheckpointActionSource::Standalone { file, .. } = &resolved.source else {
        panic!("latest checkpoint action is not read from a standalone file");
    };
    let (mut adds, mut removes) = (vec![], vec![]);
    let batches = read_standalone_checkpoint_file_actions(
        engine,
        &file.location,
        STANDALONE_CHECKPOINT_ACTIONS_SCHEMA.clone(),
    )
    .unwrap();
    for batch in batches {
        let data = ArrowEngineData::try_from_engine_data(batch.unwrap().actions).unwrap();
        let record_batch = data.record_batch();
        let add = record_batch.column_by_name("add").unwrap().as_struct();
        let remove = record_batch.column_by_name("remove").unwrap().as_struct();
        let add_paths = add.column_by_name("path").unwrap().as_string::<i32>();
        let remove_paths = remove.column_by_name("path").unwrap().as_string::<i32>();
        for row in 0..record_batch.num_rows() {
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

/// Writes the chain of standalone checkpoints on `store`:
/// - v0: protocol and metadata;
/// - v1: a manifest commit whose tree lists `a`, `b`, and the tree file `t`;
/// - v2: an add of `c`, a remove of `t`, and txn `app` at 1; then standalone checkpoint S1;
/// - v3: an add of `d` and a remove of `c`, which is an inline add of S1;
/// - v4: txn `app` at 2.
///
/// Before the standalone checkpoint S2 at v4 is written, the commits at or below S1 are deleted
/// when `cleanup` is set, as a table cleaner would.
fn standalone_chain_table(store: Arc<InMemory>, cleanup: bool) -> SyncEngine {
    let engine = SyncEngine::new_with_store(store.clone());
    create_adaptive_metadata_table(&engine, &table_root(), &[]);
    commit_root_manifest(
        &engine,
        &table_root(),
        "metadata/root-1.parquet",
        &[
            added_data_entry("a.parquet", 1, 1, 0, 0),
            added_data_entry("b.parquet", 1, 1, 1, 0),
            added_data_entry("t.parquet", 1, 1, 2, 0),
        ],
    );
    write_raw_commit(
        &engine,
        2,
        &[
            add_line("c.parquet"),
            remove_line("t.parquet"),
            txn_line("app", 1),
        ],
    );
    snapshot_at(&engine, 2).checkpoint(&engine, None).unwrap();
    write_raw_commit(
        &engine,
        3,
        &[add_line("d.parquet"), remove_line("c.parquet")],
    );
    write_raw_commit(&engine, 4, &[txn_line("app", 2)]);

    if cleanup {
        for version in 0..=2 {
            let path = delta_path_for_version(version, "json");
            block_on(store.delete(&path)).unwrap();
        }
    }
    snapshot_at(&engine, 4).checkpoint(&engine, None).unwrap();
    engine
}

#[rstest]
#[case::commits_at_or_below_standalone_kept(false)]
#[case::commits_at_or_below_standalone_cleaned_up(true)]
fn standalone_checkpoint_chains_into_next_standalone_checkpoint(#[case] cleanup: bool) {
    let engine = standalone_chain_table(Arc::new(InMemory::new()), cleanup);
    let snapshot = snapshot_at(&engine, 4);

    // Reloaded from storage, the latest action is the one in S2's file.
    let resolved = snapshot
        .resolved_checkpoint_action(&engine)
        .unwrap()
        .unwrap();
    assert_eq!(resolved.action.version(), 4);
    let CheckpointActionSource::Standalone { file, .. } = &resolved.source else {
        panic!("latest checkpoint action is not read from a standalone file");
    };
    assert_eq!(file.version, 4);
    // S2's inline rows cover (R, S2]: the window's adds, and removes that survive reconciliation.
    assert_eq!(
        inline_paths(&engine, &snapshot),
        (
            strings(&["d.parquet"]),
            strings(&["c.parquet", "t.parquet"])
        )
    );
    let app_versions = snapshot.get_app_id_versions(&engine).unwrap();
    assert_eq!(app_versions.get("app").map(|txn| txn.version), Some(2));
    assert_eq!(
        scan_paths(&engine, snapshot),
        strings(&["a.parquet", "b.parquet", "d.parquet"])
    );
}

#[test]
fn standalone_chain_reuses_untouched_domain_metadata_sidecar() {
    let (engine, table_root) = setup_adaptive_metadata_table();
    let root_path = "metadata/root-1.parquet";
    let root = write_manifest(
        &engine,
        table_root.join(root_path).unwrap(),
        &[added_data_entry("a.parquet", 1, 1, 0, 0)],
    );
    let dm_sidecar = write_sidecar(
        &engine,
        &table_root,
        "dms.parquet",
        LOG_DOMAIN_METADATA_SCHEMA.clone(),
        vec![DomainMetadata::new("user.sidecar".to_string(), "cfg".to_string()).into()],
    );
    let mut checkpoint = checkpoint_action_with_root(root_path, root.size as i64, 1).unwrap();
    checkpoint.domain_metadata_sidecars = vec![dm_sidecar.clone()];
    write_manifest_commit_keeping_pm(&engine, &table_root, 1, checkpoint).unwrap();
    write_raw_commit(&engine, 2, &[add_line("b.parquet")]);
    snapshot_at(&engine, 2).checkpoint(&engine, None).unwrap();
    write_raw_commit(&engine, 3, &[add_line("c.parquet")]);
    snapshot_at(&engine, 3).checkpoint(&engine, None).unwrap();

    let action = snapshot_at(&engine, 3)
        .latest_checkpoint_action(&engine)
        .unwrap()
        .unwrap();
    assert_eq!(action.version(), 3);
    assert_eq!(action.domain_metadata_sidecars, vec![dm_sidecar]);
}
