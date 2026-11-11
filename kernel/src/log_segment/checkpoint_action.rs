//! Resolves the latest adaptiveMetadata `checkpoint` action of a [`LogSegment`] and where it is
//! read from.

use std::sync::Arc;

use super::standalone_checkpoint::read_standalone_checkpoint_action_for_pointer;
use super::{CheckpointActionSource, LogSegment, ResolvedCheckpointAction, StandalonePointer};
use crate::actions::{CheckpointAction, LastManifestCommit};
use crate::utils::require;
use crate::{Engine, KernelError, KernelResult, version_as_i64};

impl LogSegment {
    /// The latest checkpoint action this segment covers, with the source it is read from, or
    /// `None` when the segment has no checkpoint action.
    ///
    /// `pointer` supplies the snapshot's `lastManifestCommit`. It is called once, and only its
    /// [`KernelError::MissingVersion`] failure is tolerated, when a standalone checkpoint is
    /// listed: the file then holds the action without the pointer.
    pub(crate) fn latest_checkpoint_action(
        &self,
        engine: &dyn Engine,
        pointer: impl FnOnce() -> KernelResult<Option<LastManifestCommit>>,
    ) -> KernelResult<Option<ResolvedCheckpointAction>> {
        let Some(source) = self.checkpoint_action_source(engine, pointer)? else {
            return Ok(None);
        };
        let action = Arc::new(self.read_checkpoint_action(engine, &source)?);
        Ok(Some(ResolvedCheckpointAction { action, source }))
    }

    /// Decides where the latest checkpoint action is read from. The listed standalone checkpoint
    /// wins unless `lastManifestCommit` is newer than it. `pointer` is as for
    /// [`Self::latest_checkpoint_action`].
    pub(crate) fn checkpoint_action_source(
        &self,
        engine: &dyn Engine,
        pointer: impl FnOnce() -> KernelResult<Option<LastManifestCommit>>,
    ) -> KernelResult<Option<CheckpointActionSource>> {
        let standalone = self.standalone_checkpoint(engine)?;
        let pointer = match pointer() {
            Ok(pointer) => pointer,
            // A snapshot on its standalone checkpoint, with that commit and no CRC, has no pointer
            // to check. The file alone holds the action.
            Err(KernelError::MissingVersion(_)) if standalone.is_some() => {
                return Ok(standalone.map(|file| CheckpointActionSource::Standalone {
                    file,
                    pointer: StandalonePointer::Unknown,
                }));
            }
            Err(err) => return Err(err),
        };
        let Some(file) = standalone else {
            return Ok(
                pointer.map(|pointer| CheckpointActionSource::ManifestCommit {
                    pointer,
                    superseded_standalone: None,
                }),
            );
        };
        let file_version = version_as_i64(file.version)?;
        match pointer {
            Some(pointer) if pointer.version > file_version => {
                Ok(Some(CheckpointActionSource::ManifestCommit {
                    pointer,
                    superseded_standalone: Some(file_version),
                }))
            }
            Some(pointer) => Ok(Some(CheckpointActionSource::Standalone {
                file,
                pointer: StandalonePointer::Known(pointer),
            })),
            // Every commit carries `lastManifestCommit` forward, so a standalone checkpoint is
            // only listed on a log that has a manifest commit to reference.
            None => Err(KernelError::invalid_log_segment(format!(
                "standalone checkpoint at version {file_version} references a content tree, but \
                 lastManifestCommit says no manifest commit exists"
            ))),
        }
    }

    /// Reads the checkpoint action that `source` names, and checks it against the source.
    fn read_checkpoint_action(
        &self,
        engine: &dyn Engine,
        source: &CheckpointActionSource,
    ) -> KernelResult<CheckpointAction> {
        match source {
            CheckpointActionSource::ManifestCommit {
                pointer,
                superseded_standalone,
            } => {
                let action = self.read_last_checkpoint_action_from_pointer(engine, pointer)?;
                if let Some(standalone) = superseded_standalone {
                    require!(
                        action.version > *standalone,
                        KernelError::invalid_log_segment(format!(
                            "lastManifestCommit names manifest commit version {} whose checkpoint \
                             action has checkpointMetadata.version {}, which does not exceed the \
                             standalone checkpoint at version {standalone} that it supersedes",
                            pointer.version, action.version
                        ))
                    );
                }
                Ok(action)
            }
            CheckpointActionSource::Standalone { file, pointer } => {
                read_standalone_checkpoint_action_for_pointer(engine, file, pointer.known())
            }
        }
    }
}
