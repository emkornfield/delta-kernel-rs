//! Reads the checkpoint action of an adaptiveMetadata standalone checkpoint file.

use std::slice;

use super::checkpoint_action_projection_predicate;
use crate::actions::visitors::CheckpointElementVisitor;
use crate::actions::{CHECKPOINT_ACTION_ELEMENT_SCHEMA, CheckpointAction};
use crate::path::ParsedLogPath;
use crate::utils::require;
use crate::{Engine, FileMeta, KernelError, KernelResult, RowVisitor, version_as_i64};

/// Rebuilds the checkpoint action from the rows of the standalone checkpoint file `file`, whose
/// version comes from its file name. Each row holds one element of the action. The file's `add`
/// and `remove` columns are not read. Fails if the file does not describe a valid action at that
/// version.
// TODO: remove once snapshot resolution reads standalone checkpoint files.
#[allow(dead_code)]
pub(crate) fn read_standalone_checkpoint_action(
    engine: &dyn Engine,
    file: &ParsedLogPath<FileMeta>,
) -> KernelResult<CheckpointAction> {
    // The element schema is exactly the non-file columns of a standalone file, so the file's rows
    // project onto it directly. Rows of other kinds read as all-null and are skipped.
    let predicate = checkpoint_action_projection_predicate(&CHECKPOINT_ACTION_ELEMENT_SCHEMA);
    let batches = engine.parquet_handler().read_parquet_files(
        slice::from_ref(&file.location),
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

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde_json::{Value, json};
    use url::Url;

    use super::*;
    use crate::actions::Sidecar;
    use crate::arrow::array::StringArray;
    use crate::snapshot::{CheckpointWriteResult, Snapshot};
    use crate::unit_test_utils::adaptive_metadata_fixtures::{
        minimal_checkpoint_action, setup_table, standalone_checkpoint_rows,
        write_commit_after_manifest_commit, write_manifest_commit,
        write_standalone_checkpoint_batches,
    };
    use crate::unit_test_utils::parse_json_batch;
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

    #[test]
    fn read_matches_action_returned_by_checkpoint() -> Result<()> {
        let (engine, table_root) = setup_table()?;
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

        let file = standalone_file(&engine, &table_root, 2)?;
        let action = read_standalone_checkpoint_action(&engine, &file)?;
        assert_eq!(
            Some(&action),
            checkpointed.latest_checkpoint_action(&engine)?.as_deref()
        );
        Ok(())
    }

    /// Reads a file at `file_version` whose batches are `batches`, and returns its action.
    fn read_fixture(file_version: Version, batches: Vec<Vec<Value>>) -> Result<CheckpointAction> {
        let (engine, table_root) = setup_table()?;
        write_standalone_checkpoint_batches(&engine, &table_root, file_version, batches)?;
        let file = standalone_file(&engine, &table_root, file_version)?;
        Ok(read_standalone_checkpoint_action(&engine, &file)?)
    }

    #[test]
    fn read_reconstructs_typed_sidecars_across_batches() -> Result<()> {
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
        let action = read_fixture(3, vec![rows, rest])?;

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
    ) -> Result<()> {
        let err = read_fixture(file_version, vec![rows]).unwrap_err();
        assert!(
            err.to_string().contains(expected),
            "unexpected error: {err}"
        );
        Ok(())
    }
}
