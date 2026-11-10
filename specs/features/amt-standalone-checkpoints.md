---
service: delta-kernel-rs
created: 2026-11-10
status: draft
---

# Support AMT standalone checkpoints

## Intent

Let kernel write and read adaptiveMetadata (AMT) standalone checkpoints as defined in the Iceberg V4
adaptive metadata RFC (issue #3683). A standalone checkpoint is a single-file
`n.checkpoint.parquet` that references the existing metadata tree instead of building a new one.
Any writer can produce one cheaply, so the log tail readers replay stays short between manifest
commits.

## Context

Sources:

- RFC: `protocol_rfcs/iceberg-v4-metadata.md` on delta-io/delta `master`. Sections: "Standalone
  Checkpoint", "Checkpoint Action", "Checkpoints", "Last Checkpoint File", and "Reader Requirements
  for Adaptive Metadata".
- delta-spark `master` has no standalone checkpoint implementation. `AMTCheckpointProvider` only
  handles checkpoint actions inline in manifest commits. The RFC is the only reference.

RFC rules this spec relies on:

- The file uses classic single-file naming. Each row holds one top-level action:
  - one `checkpointMetadata` (version n);
  - one `contentRoot` referencing an existing root manifest, with `contentRoot.version` <= n;
  - `protocol` and `metaData`;
  - `domainMetadata` (system domains always inline) and `txn`;
  - typed `sidecar` rows (`type` is `txn` or `domainMetadata`);
  - inline `add` and `remove` actions for commits in (contentRoot.version, n].
- V2 naming (`n.checkpoint.u.parquet`) is not used.
- `checkpointMetadata.version` is strictly increasing across all checkpoint actions. Readers take
  the greatest one across manifest commits, standalone checkpoints, and `_last_checkpoint`.
- A user domain or `appId` appears in exactly one place: inline, or in a single sidecar.
- An embedded `_last_checkpoint` checkpoint action "provides the complete table state".

What exists today, on the P&M stack tip (`stack/pm_replay_rank_compaction_at_end`, #3651):

Write side:

- `Snapshot::checkpoint` -> `CheckpointWriter::try_new` -> `checkpoint_data_with_schema` reconciles
  `log_segment().read_actions` with `ActionReconciliationProcessor`. Then `finalize` writes the
  classic `_last_checkpoint`.
- `ensure_standalone_checkpoint_supported` (`checkpoint/mod.rs`) rejects every AMT table
  (TODO(#3683)).
- `checkpoint_schema_context` picks the V1 or V2 schema from the `v2Checkpoint` feature. AMT
  requires `v2Checkpoint` to be unsupported.
- `RootManifestFile::compute_checkpoint_action` (`transaction/root_manifest_file.rs`) merges the
  snapshot's txn/DM state into a `CheckpointAction`. It rejects checkpoints that spill to sidecars.
  Kernel has no sidecar writer; `CheckpointAction::new` inlines everything.
- `ActionReconciliationProcessor` drops removes older than the tombstone retention
  (`is_expired_tombstone`), ignoring `backReference`.

Read side:

- `CheckpointAction` (`actions/mod.rs`) is parsed from the `checkpoint` array column by
  `CheckpointVisitor` and `CheckpointElementVisitor` (`actions/visitors.rs`). No visitor reads
  top-level `contentRoot`, `checkpointMetadata`, or typed `sidecar` rows.
- `Snapshot::latest_checkpoint_action` resolves the action from the commit named by
  `lastManifestCommit` (`read_last_checkpoint_action_from_pointer`). `find_pointed_commit` only
  searches commits the listed checkpoint has not pruned.
- `LogSegment::read_actions_with_checkpoint_action` requires `contentRoot.version` == action
  version, and rejects a listed checkpoint file at or after the action version.
- `SidecarVisitor`, `checkpoint_shape.rs`, and `get_file_actions_schema_and_sidecars` treat any
  top-level `sidecar` column as a V2 file-action sidecar.
- P&M replay already resolves protocol/metaData from a checkpoint file's top-level rows.

## Requirements

- R1: `Snapshot::checkpoint` on an AMT table with a metadata tree writes a standalone checkpoint at
  the snapshot version n. It has the RFC's row layout and does not write a new root manifest.
- R2: `contentRoot` is copied verbatim from the latest checkpoint action, including `tags`.
- R3: Inline file actions are the reconciled `add`/`remove` actions for (contentRoot.version, n].
  Every remove in that window is kept regardless of tombstone retention, because it may cancel a
  tree entry.
- R4: Txn and domain metadata reflect the state at n. A sidecar from the previous checkpoint
  action is referenced unchanged when none of its keys changed after the previous checkpoint.
  Otherwise its entries are read, merged, and inlined. A TODO records spilling new entries to
  sidecars once a sidecar writer exists.
- R5: `_last_checkpoint` is a classic hint naming the standalone file (`version` n, `size`,
  `sizeInBytes`). It has no `amtCheckpoint` and no `numOfAddFiles`.
- R6: The snapshot returned by `Snapshot::checkpoint` carries the new checkpoint action.
- R7: An AMT table with no checkpoint action yet gets the checkpoint format its features enable
  (classic V1 in practice) instead of an error.
- R8: Checkpointing a snapshot whose version equals the latest checkpoint version returns
  `AlreadyExists`.
- R9: Readers rebuild a `CheckpointAction` from a standalone checkpoint's rows. A file with a
  `contentRoot` row is a standalone checkpoint, and its `sidecar` rows are AMT sidecars, never V2
  file sidecars.
- R10: `Snapshot::latest_checkpoint_action` resolves lazily from the listed checkpoint file S when
  `lastManifestCommit.version` <= S. It validates `checkpointMetadata.version` == S and
  `contentRoot.version` == `lastManifestCommit.contentRootVersion`.
- R11: Scans over a standalone checkpoint chain three sources, newest first: commits after S, then
  S's inline adds/removes as log batches, then the tree adds. Removes cancel tree entries.
- R12: `read_actions_with_checkpoint_action` picks its guards by where the action came from. From
  a manifest commit, the current guards apply. From a standalone file, `contentRoot.version` <=
  action version, and the file's version equals the action version.
- R13: Building a new root manifest while the latest checkpoint is standalone is rejected until
  tree building consumes the inline file actions.
- R14: The checkpoint-action lookup moves into `LogSegment` with imperative and plan-based
  versions.
- R15: P&M replay captures the standalone action opportunistically when it reads the checkpoint
  file anyway. An incomplete capture (P&M short-circuit) is not an error; the lazy lookup still
  resolves the action.
- R16: The writer accepts a previous checkpoint that is itself standalone. Its inline file actions
  are carried into the new file's window.

## Clarifications

- Q1: Where do txn/user DM go on write? A1: Reuse the previous checkpoint's sidecar references
  when untouched; otherwise inline. TODO for new sidecars. (user)
- Q2: What does `_last_checkpoint` contain? A2: A classic hint pointing at the standalone file. An
  embedded partial action is not legal, because the RFC says the embedded action provides the
  complete table state. (user)
- Q3: When is the read-side action reconstructed? A3: Lazily first. Follow-ups move the lookup
  into `LogSegment` (plan and non-plan), and capture it opportunistically in P&M replay without
  failing on a partial capture. (user)
- Q4: What should happen on an AMT table with no tree? A4: Write the checkpoint version the table
  enables. (user)
- Q5: What tells a standalone checkpoint apart from other checkpoint files? A5: The presence of a
  `contentRoot` row. Sidecars may or may not be present. (user)
- Q6: How should the stack be ordered? A6: One linear stack off #3651, write side first. Small
  PRs, with refactors separate from features. (user)
- Q7: The RFC says writers must not produce classic or V2 checkpoints once `adaptiveMetadata` is
  enabled. Does R7 need an RFC change allowing it until the first manifest commit?
  A7: UNRESOLVED
- Q8: Consider a snapshot at S whose commit-S JSON has been cleaned up, with no CRC. There is no
  `lastManifestCommit` source. Should reads proceed from file S, while writes error?
  A8: UNRESOLVED
- Q9: Should reused sidecars apply txn expiry, which forces a re-read? A9: UNRESOLVED
- Q10: Should readers check that `checkpointMetadata.version` is strictly increasing?
  A10: UNRESOLVED

## Acceptance Criteria

- AC1: GIVEN an AMT table whose latest checkpoint is a manifest commit at R, plus commits R+1..n
  WHEN `snapshot.checkpoint()` runs at n THEN `n.checkpoint.parquet` has exactly one
  `checkpointMetadata` (version n) and one `contentRoot` equal to the manifest commit's, plus
  protocol, metaData, txn/DM, and the reconciled adds/removes for (R, n]. No new root manifest is
  written.
- AC2: GIVEN a remove in (R, n] older than the tombstone retention WHEN checkpointing at n THEN
  the remove is in the file.
- AC3: GIVEN a txn sidecar whose app ids are untouched after the previous checkpoint WHEN
  checkpointing THEN the file references that sidecar unchanged. GIVEN one of its app ids is
  updated THEN the sidecar's entries are inlined and the reference dropped.
- AC4: GIVEN a standalone checkpoint written at n WHEN reading `_last_checkpoint` THEN it names
  version n with `size` and `sizeInBytes`, and has no `amtCheckpoint` or `numOfAddFiles`.
- AC5: GIVEN an AMT table with no manifest commit WHEN checkpointing THEN a classic V1 checkpoint
  is written.
- AC6: GIVEN the snapshot is at the latest checkpoint version WHEN checkpointing THEN
  `AlreadyExists` is returned.
- AC7: GIVEN a standalone checkpoint at S WHEN loading a snapshot at S or later, by time travel,
  or by incremental update THEN `latest_checkpoint_action` returns the file's action, and a scan
  returns tree files plus inline adds minus inline removes plus later commits.
- AC8: GIVEN a standalone file whose `contentRoot.version` differs from
  `lastManifestCommit.contentRootVersion` WHEN resolving the action THEN an error names both
  versions.
- AC9: GIVEN a manifest commit after S WHEN loading the latest snapshot THEN that commit's action
  supersedes S.
- AC10: GIVEN a standalone checkpoint with typed sidecars WHEN reading THEN no sidecar is read as
  a file-action sidecar, and txn/DM lookups include the sidecar entries.
- AC11: GIVEN P&M replay that stops before all of S's non-file rows WHEN later calling
  `latest_checkpoint_action` THEN it resolves through the lazy lookup without error.
- AC12: GIVEN the latest checkpoint is standalone WHEN building a new root manifest THEN it is
  rejected with an error naming the standalone version.
- AC13: GIVEN a standalone checkpoint at S1 and commits up to S2 WHEN checkpointing at S2 THEN
  S2's inline actions cover (contentRoot.version, S2], including S1's inline actions.
- AC14: Every read AC passes with both the default engine and the plan-executor engine.

## Constraints

- Everything stays behind `adaptive-metadata-in-dev`. Default, AMT-without-plans, and
  plans-without-AMT builds stay green.
- No panics in production code. Every error names the offending versions or paths.
- Non-AMT checkpoint behavior is unchanged.
- Until the read phase lands, tables with a standalone checkpoint can't be reopened by kernel.
  This is acceptable only because the feature is in-dev.

## Out of Scope

- Writing new sidecars (spilling txn/DM). TODO only.
- Building a manifest commit (new tree) that consumes a standalone checkpoint's inline actions.
- Metadata cleanup or VACUUM reachability through standalone checkpoints.
- Declarative and parallel scan over AMT checkpoint actions. These stay blocked by
  `require_no_checkpoint_action`.
- Changing the RFC.

---

## Plan

### Approach

Write side: an AMT branch in `CheckpointWriter`.

- Resolve the previous checkpoint action and its version V.
- Reconcile commits (V, n] with tombstone retention disabled. When the previous checkpoint is
  standalone, also include its inline file actions.
- Compute the txn/DM state, reusing sidecars per R4.
- Emit one `checkpointMetadata` row, one `contentRoot` row, protocol/metaData, txn/DM, the reused
  sidecars, and the file actions, using a standalone schema.
- Write a classic hint.

Read side: a row visitor rebuilds the `CheckpointAction`. `contentRoot` presence selects the AMT
interpretation of `sidecar` rows. Lazy snapshot resolution validates the action against the
`lastManifestCommit` pointer. `read_actions_with_checkpoint_action` takes the action's source and
chains the file's inline file actions between later commits and the tree.

### Affected Components

- `kernel/src/checkpoint/` — AMT branch, standalone schema, hint.
- `kernel/src/action_reconciliation/` — retention mode.
- `kernel/src/transaction/root_manifest_file.rs` — shared txn/DM merge helper, R13 guard.
- `kernel/src/actions/{mod,visitors}.rs` — standalone rows <-> `CheckpointAction`.
- `kernel/src/log_segment/`, `kernel/src/snapshot/` — resolution, guards, scan chaining, P&M
  capture.
- `kernel/src/unit_test_utils.rs` — standalone fixtures.

### Data Model Changes

- A new standalone checkpoint parquet schema with a top-level `contentRoot` and typed `sidecar`.

### API Changes

None public. `Snapshot::checkpoint` stops erroring on AMT tables.

### Risks

- The RFC conflicts with R7 (Q7).
- Readers that predate the read phase cannot open tables with standalone checkpoints.
- Dropping a remove by mistake would resurrect tree files, so AC2 is load-bearing.

---

## Tasks

One linear git-stack off `stack/pm_replay_rank_compaction_at_end` (#3651). One PR per task.

### Phase 1 — write side

- [ ] T1 `feat:` AMT table with no tree writes its enabled checkpoint format (R7, AC5).
- [ ] T2 `refactor:` tombstone retention becomes a parameter of `ActionReconciliationProcessor`.
- [ ] T3 `refactor:` extract the txn/DM merge from `compute_checkpoint_action` into a shared
  helper.
- [ ] T4 `feat:` standalone checkpoint schema and `CheckpointAction` -> non-file rows (R1, R2).
- [ ] T5 `feat:` gap file-action reconciliation for a manifest-commit predecessor (R3, AC2). A
  standalone predecessor is rejected with a TODO.
- [ ] T6 `feat:` txn/DM state with sidecar reuse (R4, AC3).
- [ ] T7 `feat:` wire `CheckpointWriter`: lift the guard, write the hint, capture the
  post-checkpoint action (R1, R5, R6, R8, AC1, AC4, AC6).

### Phase 2 — read side

- [ ] T8 `refactor:` split `CheckpointElementVisitor` per-element parsing for reuse by a row
  visitor.
- [ ] T9 `fix:` with a `contentRoot` row, `sidecar` rows are AMT sidecars, not file sidecars (R9).
- [ ] T10 `feat:` rebuild `CheckpointAction` from standalone rows (R9).
- [ ] T11 `feat:` lazy resolution and validation, the `find_pointed_commit` fix, and the R13 guard
  (R10, R13, AC8, AC9, AC12). Flip the TODO(#3683) replay tests.
- [ ] T12 `feat:` source-configured guards and scan chaining (R11, R12, AC7, AC10). Split the
  guard parameterization into a prior refactor if it stands alone.
- [ ] T13 `feat:` standalone predecessor in the writer, plus round-trip tests (R16, AC13).
- [ ] T14 `refactor:` move the checkpoint-action lookup into `LogSegment` (imperative).
- [ ] T15 `feat:` plan-based checkpoint-action lookup (R14, AC14).
- [ ] T16 `feat:` opportunistic P&M capture (R15, AC11).

### Validation

- [ ] T-val: Run acceptance criteria verification. Run lib tests and clippy for the default,
  `adaptive-metadata-in-dev`, and `adaptive-metadata-in-dev,declarative-plans` feature sets.
