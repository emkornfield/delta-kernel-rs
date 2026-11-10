//! Reads the checkpoint action of an adaptiveMetadata standalone checkpoint file.

use super::{LogSegment, checkpoint_action_projection_predicate, read_parquet_action_rows};
use crate::actions::visitors::CheckpointElementVisitor;
use crate::actions::{
    CHECKPOINT_ACTION_ELEMENT_SCHEMA, CheckpointAction, LastManifestCommit,
    is_standalone_checkpoint_schema,
};
use crate::path::{LogPathFileType, ParsedLogPath};
use crate::utils::require;
use crate::{Engine, FileMeta, KernelError, KernelResult, RowVisitor, version_as_i64};

impl LogSegment {
    /// The listed checkpoint file when it is a standalone adaptiveMetadata checkpoint, or `None`
    /// for any other segment. Only classic-named single-part parquet files can be standalone.
    pub(crate) fn standalone_checkpoint(
        &self,
        engine: &dyn Engine,
    ) -> KernelResult<Option<ParsedLogPath<FileMeta>>> {
        let [checkpoint] = self.listed.checkpoint_parts.as_slice() else {
            return Ok(None);
        };
        if checkpoint.file_type != LogPathFileType::ClassicCheckpoint {
            return Ok(None);
        }
        let schema = Self::read_checkpoint_schema(
            engine,
            checkpoint,
            self.checkpoint_hint_schema().as_ref(),
        )?;
        Ok(is_standalone_checkpoint_schema(&schema).then(|| checkpoint.clone()))
    }
}

/// Rebuilds the checkpoint action from the rows of the standalone checkpoint file `file`, whose
/// version comes from its file name. Each row holds one element of the action. The file's `add`
/// and `remove` columns are not read. Fails if the file does not describe a valid action at that
/// version.
pub(crate) fn read_standalone_checkpoint_action(
    engine: &dyn Engine,
    file: &ParsedLogPath<FileMeta>,
) -> KernelResult<CheckpointAction> {
    // The element schema is exactly the non-file columns of a standalone file, so the file's rows
    // project onto it directly. Rows of other kinds read as all-null and are skipped.
    let predicate = checkpoint_action_projection_predicate(&CHECKPOINT_ACTION_ELEMENT_SCHEMA);
    let batches = read_parquet_action_rows(
        engine,
        &file.location,
        CHECKPOINT_ACTION_ELEMENT_SCHEMA.clone(),
        predicate,
    )?;
    let mut visitor = CheckpointElementVisitor::default();
    for batch in batches {
        visitor.visit_rows_of(batch?.as_ref())?;
    }
    let source = format!("standalone checkpoint {}", file.location.location);
    let action = visitor.into_checkpoint_action(&source)?;

    let file_version = version_as_i64(file.version)?;
    require!(
        action.version == file_version,
        KernelError::generic(format!(
            "{source} has checkpointMetadata.version {}, expected the file's version \
             {file_version}",
            action.version
        ))
    );
    Ok(action)
}

/// Reads the checkpoint action of the standalone checkpoint `file`, checking its content root
/// against `pointer` when the snapshot knows its `lastManifestCommit`.
///
/// # Errors
/// Returns an error if the file cannot be read as a standalone checkpoint, or if `pointer` is
/// given and its `contentRootVersion` differs from the action's `contentRoot.version`.
pub(crate) fn read_standalone_checkpoint_action_for_pointer(
    engine: &dyn Engine,
    file: &ParsedLogPath<FileMeta>,
    pointer: Option<&LastManifestCommit>,
) -> KernelResult<CheckpointAction> {
    let action = read_standalone_checkpoint_action(engine, file)?;
    if let Some(pointer) = pointer {
        require!(
            action.content_root_version() == pointer.content_root_version,
            KernelError::invalid_log_segment(format!(
                "standalone checkpoint at version {} has contentRoot.version {}, but \
                 lastManifestCommit (manifest commit version {}) has contentRootVersion {}",
                action.version,
                action.content_root_version(),
                pointer.version,
                pointer.content_root_version
            ))
        );
    }
    Ok(action)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rstest::rstest;
    use serde_json::{Value, json};
    use test_utils::delta_path_for_version;
    use url::Url;

    use super::*;
    use crate::actions::{
        DomainMetadata, LOG_DOMAIN_METADATA_SCHEMA, LOG_TXN_SCHEMA, SetTransaction, Sidecar,
    };
    use crate::arrow::array::StringArray;
    use crate::engine::sync::SyncEngine;
    use crate::object_store::ObjectStoreExt as _;
    use crate::object_store::memory::InMemory;
    use crate::snapshot::{CheckpointWriteResult, Snapshot};
    use crate::unit_test_utils::adaptive_metadata_fixtures::{
        commit_info_line, last_manifest_commit_info, minimal_checkpoint_action, reader_engine,
        setup_adaptive_metadata_table, setup_table, setup_table_on, standalone_checkpoint_rows,
        write_commit, write_commit_after_manifest_commit, write_manifest_commit, write_sidecar,
        write_standalone_checkpoint_batches,
    };
    use crate::unit_test_utils::{assert_result_error_with_message, parse_json_batch};
    use crate::{Result, Version};

    /// The `ParsedLogPath` of the standalone checkpoint file at `version` of `table_root`.
    fn standalone_file(
        engine: &dyn Engine,
        table_root: &Url,
        version: Version,
    ) -> Result<ParsedLogPath<FileMeta>> {
        let location = ParsedLogPath::new_classic_parquet_checkpoint(table_root, version)?.location;
        let meta = engine.storage_handler().head(&location)?;
        Ok(ParsedLogPath::new(meta).unwrap())
    }

    #[rstest]
    fn read_matches_action_returned_by_checkpoint(
        #[values(false, true)] plan_executor: bool,
    ) -> Result<()> {
        let store = Arc::new(InMemory::new());
        let (engine, table_root) = setup_table_on(store.clone())?;
        write_manifest_commit(
            &engine,
            &table_root,
            1,
            minimal_checkpoint_action("metadata/root.parquet", 1)?,
        )?;
        let add = json!({ "add": { "path": "a", "partitionValues": {}, "size": 1,
                                   "modificationTime": 0, "dataChange": true } });
        let txn = json!({ "txn": { "appId": "app", "version": 1 } });
        let dm = json!({ "domainMetadata": { "domain": "user.dm", "configuration": "cfg",
                                             "removed": false } });
        let commit = parse_json_batch(StringArray::from(vec![
            add.to_string(),
            txn.to_string(),
            dm.to_string(),
        ]));
        write_commit_after_manifest_commit(&engine, &table_root, 2, 1, commit)?;

        let snapshot = Snapshot::builder_for(table_root.clone()).build(&engine)?;
        let (result, checkpointed) = snapshot.checkpoint(&engine, None)?;
        assert_eq!(result, CheckpointWriteResult::Written);

        let reader = reader_engine(store, plan_executor);
        let file = standalone_file(reader.as_ref(), &table_root, 2)?;
        let action = read_standalone_checkpoint_action(reader.as_ref(), &file)?;
        assert_eq!(
            Some(&action),
            checkpointed.latest_checkpoint_action(&engine)?.as_deref()
        );
        Ok(())
    }

    /// Reads a file at `file_version` whose batches are `batches`, and returns its action. The
    /// file is read through the plan executor when `plan_executor` is set.
    fn read_fixture(
        file_version: Version,
        batches: Vec<Vec<Value>>,
        plan_executor: bool,
    ) -> Result<CheckpointAction> {
        let store = Arc::new(InMemory::new());
        let (engine, table_root) = setup_table_on(store.clone())?;
        write_standalone_checkpoint_batches(&engine, &table_root, file_version, batches)?;
        let file = standalone_file(&engine, &table_root, file_version)?;
        let reader = reader_engine(store, plan_executor);
        Ok(read_standalone_checkpoint_action(reader.as_ref(), &file)?)
    }

    #[rstest]
    fn read_reconstructs_typed_sidecars_across_batches(
        #[values(false, true)] plan_executor: bool,
    ) -> Result<()> {
        let mut rows = standalone_checkpoint_rows(3, 3);
        rows.push(
            json!({ "sidecar": { "type": "txn", "path": "txn-sidecar.parquet",
                                       "sizeInBytes": 100, "modificationTime": 1 } }),
        );
        rows.push(
            json!({ "sidecar": { "type": "domainMetadata", "path": "dm-sidecar.parquet",
                                       "sizeInBytes": 200, "modificationTime": 2 } }),
        );
        let rest = rows.split_off(4);
        let action = read_fixture(3, vec![rows, rest], plan_executor)?;

        let sidecar_paths =
            |sidecars: &[Sidecar]| sidecars.iter().map(|s| s.path.clone()).collect::<Vec<_>>();
        assert_eq!(action.version, 3);
        assert_eq!(sidecar_paths(&action.txn_sidecars), ["txn-sidecar.parquet"]);
        assert_eq!(
            sidecar_paths(&action.domain_metadata_sidecars),
            ["dm-sidecar.parquet"]
        );
        assert!(action.transactions.is_empty() && action.domain_metadata.is_empty());
        Ok(())
    }

    /// The rows of a file at version 3 without the rows of kind `name`.
    fn without(name: &str) -> Vec<Value> {
        let mut rows = standalone_checkpoint_rows(3, 3);
        rows.retain(|row| row.get(name).is_none());
        rows
    }

    /// The rows of a file at version 3 with `extra` appended.
    fn with_extra(extra: Value) -> Vec<Value> {
        let mut rows = standalone_checkpoint_rows(3, 3);
        rows.push(extra);
        rows
    }

    #[rstest]
    #[case::missing_content_root(
        3,
        without("contentRoot"),
        "is missing required `contentRoot` element"
    )]
    #[case::missing_checkpoint_metadata(
        3,
        without("checkpointMetadata"),
        "is missing required `checkpointMetadata` element"
    )]
    #[case::duplicate_content_root(
        3,
        with_extra(json!({ "contentRoot": { "path": "other.parquet", "sizeInBytes": 1,
                                            "version": 3 } })),
        "has 2 `contentRoot` elements"
    )]
    #[case::duplicate_checkpoint_metadata(
        3,
        with_extra(json!({ "checkpointMetadata": { "version": 3 } })),
        "has 2 `checkpointMetadata` elements"
    )]
    #[case::content_root_ahead_of_checkpoint(
        3,
        standalone_checkpoint_rows(3, 4),
        "contentRoot.version 4 exceeds checkpointMetadata.version 3"
    )]
    #[case::checkpoint_version_differs_from_file_name(
        2,
        standalone_checkpoint_rows(3, 3),
        "has checkpointMetadata.version 3, expected the file's version 2"
    )]
    fn read_rejects_malformed_standalone_checkpoint(
        #[case] file_version: Version,
        #[case] rows: Vec<Value>,
        #[case] expected: &str,
        #[values(false, true)] plan_executor: bool,
    ) -> Result<()> {
        let err = read_fixture(file_version, vec![rows], plan_executor).unwrap_err();
        assert!(
            err.to_string().contains(expected),
            "unexpected error: {err}"
        );
        Ok(())
    }

    /// Commits a manifest commit at 1 and a commit at 2 that carries its pointer, then checkpoints
    /// at 2. Returns the action the checkpoint wrote.
    fn write_checkpointed_table(
        store: Arc<InMemory>,
    ) -> Result<(SyncEngine, Url, Arc<CheckpointAction>)> {
        let (engine, table_root) = setup_table_on(store)?;
        write_manifest_commit(
            &engine,
            &table_root,
            1,
            minimal_checkpoint_action("metadata/root.parquet", 1)?,
        )?;
        let add = json!({ "add": { "path": "a", "partitionValues": {}, "size": 1,
                                   "modificationTime": 0, "dataChange": true } });
        write_commit_after_manifest_commit(
            &engine,
            &table_root,
            2,
            1,
            parse_json_batch(StringArray::from(vec![add.to_string()])),
        )?;
        let snapshot = Snapshot::builder_for(table_root.clone()).build(&engine)?;
        let (_, checkpointed) = snapshot.checkpoint(&engine, None)?;
        let action = checkpointed.latest_checkpoint_action(&engine)?.unwrap();
        Ok((engine, table_root, action))
    }

    /// A commit at `version` holding one `txn` action, after the manifest commit at 1.
    fn write_txn_commit_after_manifest_commit(
        engine: &SyncEngine,
        table_root: &Url,
        version: Version,
    ) -> Result<()> {
        let txn = json!({ "txn": { "appId": "app", "version": version } });
        write_commit_after_manifest_commit(
            engine,
            table_root,
            version,
            1,
            parse_json_batch(StringArray::from(vec![txn.to_string()])),
        )?;
        Ok(())
    }

    #[rstest]
    #[case::at_checkpoint(false, None)]
    #[case::after_checkpoint(true, None)]
    #[case::time_travel_to_checkpoint(true, Some(2))]
    fn latest_checkpoint_action_reloaded_from_storage_matches_written_action(
        #[case] write_later_commit: bool,
        #[case] at_version: Option<Version>,
        #[values(false, true)] plan_executor: bool,
    ) -> Result<()> {
        let store = Arc::new(InMemory::new());
        let (engine, table_root, written) = write_checkpointed_table(store.clone())?;
        if write_later_commit {
            write_txn_commit_after_manifest_commit(&engine, &table_root, 3)?;
        }
        let reader = reader_engine(store, plan_executor);
        let snapshot = match at_version {
            Some(version) => Snapshot::builder_for(table_root)
                .at_version(version)
                .build(reader.as_ref())?,
            None => Snapshot::builder_for(table_root).build(reader.as_ref())?,
        };
        assert_eq!(snapshot.log_segment().checkpoint_version, Some(2));
        assert_eq!(
            snapshot.latest_checkpoint_action(reader.as_ref())?,
            Some(written)
        );
        Ok(())
    }

    // The commit at 2 is gone and there is no CRC, so the snapshot has no pointer to check. The
    // standalone file still holds the action.
    #[rstest]
    #[tokio::test]
    async fn latest_checkpoint_action_reads_file_when_checkpoint_commit_and_crc_are_gone(
        #[values(false, true)] plan_executor: bool,
    ) -> Result<()> {
        let store = Arc::new(InMemory::new());
        let (_, table_root, written) = write_checkpointed_table(store.clone())?;
        store
            .delete(&delta_path_for_version(2, "json"))
            .await
            .unwrap();

        let reader = reader_engine(store, plan_executor);
        let snapshot = Snapshot::builder_for(table_root).build(reader.as_ref())?;
        assert_eq!(snapshot.version(), 2);
        assert_eq!(
            snapshot.latest_checkpoint_action(reader.as_ref())?,
            Some(written)
        );
        Ok(())
    }

    #[test]
    fn latest_checkpoint_action_rejects_standalone_checkpoint_without_manifest_commit() -> Result<()>
    {
        let (engine, table_root) = setup_table()?;
        write_commit(
            &engine,
            &table_root,
            1,
            parse_json_batch(StringArray::from(vec![commit_info_line(1, None)])),
        )?;
        write_standalone_checkpoint_batches(
            &engine,
            &table_root,
            1,
            vec![standalone_checkpoint_rows(1, 1)],
        )?;

        let snapshot = Snapshot::builder_for(table_root).build(&engine)?;
        assert_result_error_with_message(
            snapshot.latest_checkpoint_action(&engine),
            "standalone checkpoint at version 1 references a content tree, but lastManifestCommit \
             says no manifest commit exists",
        );
        Ok(())
    }

    #[rstest]
    fn latest_checkpoint_action_rejects_content_root_that_differs_from_pointer(
        #[values(false, true)] plan_executor: bool,
    ) -> Result<()> {
        let store = Arc::new(InMemory::new());
        let (engine, table_root) = setup_table_on(store.clone())?;
        write_manifest_commit(
            &engine,
            &table_root,
            1,
            minimal_checkpoint_action("metadata/root.parquet", 1)?,
        )?;
        // The pointer says content root 0, but the standalone file below has content root 1.
        write_commit(
            &engine,
            &table_root,
            2,
            parse_json_batch(StringArray::from(vec![
                last_manifest_commit_info(1, 0).to_string(),
            ])),
        )?;
        write_standalone_checkpoint_batches(
            &engine,
            &table_root,
            2,
            vec![standalone_checkpoint_rows(2, 1)],
        )?;

        let reader = reader_engine(store, plan_executor);
        let snapshot = Snapshot::builder_for(table_root).build(reader.as_ref())?;
        assert_result_error_with_message(
            snapshot.latest_checkpoint_action(reader.as_ref()),
            "standalone checkpoint at version 2 has contentRoot.version 1, but lastManifestCommit \
             (manifest commit version 1) has contentRootVersion 0",
        );
        Ok(())
    }

    #[rstest]
    #[case::supersedes_standalone(3, None)]
    #[case::does_not_exceed_standalone(
        2,
        Some("does not exceed the standalone checkpoint at version 2")
    )]
    fn manifest_commit_after_standalone_supersedes_it_or_errors(
        #[case] checkpoint_version: Version,
        #[case] expected_error: Option<&str>,
        #[values(false, true)] plan_executor: bool,
    ) -> Result<()> {
        let store = Arc::new(InMemory::new());
        let (engine, table_root, _) = write_checkpointed_table(store.clone())?;
        write_manifest_commit(
            &engine,
            &table_root,
            3,
            minimal_checkpoint_action("metadata/root-v3.parquet", checkpoint_version)?,
        )?;

        let reader = reader_engine(store, plan_executor);
        let snapshot = Snapshot::builder_for(table_root).build(reader.as_ref())?;
        let result = snapshot.latest_checkpoint_action(reader.as_ref());
        match expected_error {
            Some(expected) => assert_result_error_with_message(result, expected),
            None => assert_eq!(result?.map(|action| action.version), Some(3)),
        }
        Ok(())
    }

    /// Typed sidecars of a standalone checkpoint carry txn and domainMetadata entries, which the
    /// snapshot reads alongside the inline ones, and are not read as file-action sidecars.
    #[test]
    fn txn_and_domain_metadata_come_from_inline_entries_and_sidecars() -> Result<()> {
        let (engine, table_root) = setup_table()?;
        write_manifest_commit(
            &engine,
            &table_root,
            1,
            minimal_checkpoint_action("metadata/root.parquet", 1)?,
        )?;
        write_commit_after_manifest_commit(
            &engine,
            &table_root,
            2,
            1,
            parse_json_batch(StringArray::from(Vec::<String>::new())),
        )?;
        write_sidecar(
            &engine,
            &table_root,
            "txn-sidecar.parquet",
            LOG_TXN_SCHEMA.clone(),
            vec![SetTransaction::new("spilled".to_string(), 3, None).into()],
        );
        write_sidecar(
            &engine,
            &table_root,
            "dm-sidecar.parquet",
            LOG_DOMAIN_METADATA_SCHEMA.clone(),
            vec![DomainMetadata::new("user.spilled".to_string(), "cfg".to_string()).into()],
        );
        let mut rows = standalone_checkpoint_rows(2, 1);
        rows.extend([
            json!({ "txn": { "appId": "inline", "version": 1 } }),
            json!({ "domainMetadata": { "domain": "user.inline", "configuration": "cfg",
                                        "removed": false } }),
            json!({ "sidecar": { "type": "txn", "path": "txn-sidecar.parquet",
                                 "sizeInBytes": 100, "modificationTime": 1 } }),
            json!({ "sidecar": { "type": "domainMetadata", "path": "dm-sidecar.parquet",
                                 "sizeInBytes": 100, "modificationTime": 1 } }),
        ]);
        write_standalone_checkpoint_batches(&engine, &table_root, 2, vec![rows])?;

        let snapshot = Snapshot::builder_for(table_root).build(&engine)?;

        assert_eq!(snapshot.log_segment().checkpoint_version, Some(2));
        let reader = snapshot.reader();
        assert_eq!(reader.get_app_id_version("inline", &engine)?, Some(1));
        assert_eq!(reader.get_app_id_version("spilled", &engine)?, Some(3));
        assert_eq!(
            reader.get_domain_metadata("user.inline", &engine)?,
            Some("cfg".to_string())
        );
        assert_eq!(
            reader.get_domain_metadata("user.spilled", &engine)?,
            Some("cfg".to_string())
        );
        Ok(())
    }

    // A classic checkpoint on an adaptiveMetadata table with no manifest commit has no tree, so it
    // has no checkpoint action.
    #[test]
    fn latest_checkpoint_action_of_classic_checkpoint_without_tree_is_none() -> Result<()> {
        let (engine, table_root) = setup_adaptive_metadata_table();
        let snapshot = Snapshot::builder_for(table_root.clone()).build(&engine)?;
        let (result, _) = snapshot.checkpoint(&engine, None)?;
        assert_eq!(result, CheckpointWriteResult::Written);

        let reloaded = Snapshot::builder_for(table_root).build(&engine)?;
        assert_eq!(reloaded.log_segment().checkpoint_version, Some(0));
        assert!(reloaded.latest_checkpoint_action(&engine)?.is_none());
        Ok(())
    }
}
