// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

use uuid::Uuid;

use super::snapshot::{DefaultManifestProcess, MergeManifestProcess, SnapshotProducer};
use super::{
    MANIFEST_MERGE_ENABLED, MANIFEST_MERGE_ENABLED_DEFAULT, MANIFEST_MIN_MERGE_COUNT,
    MANIFEST_MIN_MERGE_COUNT_DEFAULT, MANIFEST_TARGET_SIZE_BYTES,
    MANIFEST_TARGET_SIZE_BYTES_DEFAULT,
};
use crate::error::Result;
use crate::spec::{
    DataContentType, DataFile, ManifestEntry, ManifestFile, ManifestStatus, Operation,
};
use crate::table::Table;
use crate::transaction::snapshot::SnapshotProduceOperation;
use crate::transaction::{ActionCommit, TransactionAction};

/// Which snapshot [`Operation`] a file replacement records.
///
/// `rewrite_files` and `overwrite_files` differ only in this value
pub(crate) trait ReplaceFilesMode: Send + Sync + 'static {
    const OPERATION: Operation;
}

/// Files were added and removed without changing table data (compaction,
/// changing file format, relocating files).
pub struct Rewrite;

/// Files were added and removed in a logical overwrite.
pub struct Overwrite;

impl ReplaceFilesMode for Rewrite {
    const OPERATION: Operation = Operation::Replace;
}

impl ReplaceFilesMode for Overwrite {
    const OPERATION: Operation = Operation::Overwrite;
}

/// A blanket `impl<M: ReplaceFilesMode> SnapshotProduceOperation for M` would
/// collide with `impl SnapshotProduceOperation for FastAppendOperation`: the
/// compiler cannot prove `FastAppendOperation` will never implement
/// `ReplaceFilesMode`. This wrapper carries the shared implementation instead.
pub(crate) struct ReplaceFilesOperation<M: ReplaceFilesMode> {
    _mode: PhantomData<M>,
    // Populated by `existing_manifest` (which always runs first — see the call
    // order in `SnapshotProducer::commit`) as a side effect of its single pass
    // over the manifest list, so `delete_entries` can drain it instead of
    // re-scanning every manifest a second time.
    deleted_entries: Mutex<Option<Vec<ManifestEntry>>>,
}

impl<M: ReplaceFilesMode> ReplaceFilesOperation<M> {
    pub(crate) fn new() -> Self {
        Self {
            _mode: PhantomData,
            deleted_entries: Mutex::new(None),
        }
    }
}

impl<M: ReplaceFilesMode> SnapshotProduceOperation for ReplaceFilesOperation<M> {
    fn operation(&self) -> Operation {
        M::OPERATION
    }

    async fn delete_entries(
        &self,
        _snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        // `existing_manifest` always runs first (see the call order in
        // `SnapshotProducer::commit`) and populates this as a side effect of its
        // one pass over the manifest list — draining it here instead of
        // re-scanning every manifest a second time is what actually matters:
        // manifest loads, not the size of the file lists being diffed, are what
        // dominate commit latency (see the perf investigation this refactor
        // came out of).
        Ok(self
            .deleted_entries
            .lock()
            .unwrap()
            .take()
            .unwrap_or_default())
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &mut SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        let table_metadata_ref = snapshot_produce.table.metadata();
        let file_io_ref = snapshot_produce.table.file_io();

        let Some(snapshot) = snapshot_produce
            .table
            .metadata()
            .snapshot_for_ref(snapshot_produce.target_branch())
        else {
            return Ok(vec![]);
        };

        let manifest_list = snapshot
            .load_manifest_list(file_io_ref, table_metadata_ref)
            .await?;

        let gen_manifest_entry = |old_entry: &Arc<ManifestEntry>| {
            ManifestEntry::builder()
                .status(ManifestStatus::Deleted)
                .snapshot_id(old_entry.snapshot_id().unwrap())
                .sequence_number(old_entry.sequence_number().unwrap())
                .file_sequence_number(old_entry.file_sequence_number().unwrap())
                .data_file(old_entry.data_file().clone())
                .build()
        };

        let mut existing_files = Vec::new();
        let mut deleted_entries = Vec::new();

        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(file_io_ref).await?;

            // Same matching predicate `delete_entries` used to compute on its
            // own, separate full scan of every manifest — collecting it here
            // instead, in the one pass this method already makes over the same
            // entries, halves the manifest loads for this commit.
            for entry in manifest.entries() {
                if entry.content_type() == DataContentType::Data
                    && snapshot_produce
                        .removed_data_file_paths
                        .contains(entry.data_file().file_path())
                {
                    deleted_entries.push(gen_manifest_entry(entry));
                }

                if (entry.content_type() == DataContentType::PositionDeletes
                    || entry.content_type() == DataContentType::EqualityDeletes)
                    && snapshot_produce
                        .removed_delete_file_paths
                        .contains(entry.data_file().file_path())
                {
                    deleted_entries.push(gen_manifest_entry(entry));
                }
            }

            let found_deleted_files: HashSet<_> = manifest
                .entries()
                .iter()
                .filter_map(|entry| {
                    if snapshot_produce
                        .removed_data_file_paths
                        .contains(entry.data_file().file_path())
                        || snapshot_produce
                            .removed_delete_file_paths
                            .contains(entry.data_file().file_path())
                    {
                        Some(entry.data_file().file_path().to_string())
                    } else {
                        None
                    }
                })
                .collect();

            if found_deleted_files.is_empty() {
                existing_files.push(manifest_file.clone());
            } else {
                // Rewrite the manifest file without the deleted data files
                let survives = |entry: &ManifestEntry| {
                    entry.is_alive() && !found_deleted_files.contains(entry.data_file().file_path())
                };

                if manifest.entries().iter().any(|entry| survives(entry)) {
                    let mut manifest_writer = snapshot_produce.new_manifest_writer(
                        manifest_file.content,
                        manifest_file.partition_spec_id,
                    )?;

                    for entry in manifest.entries() {
                        // Carry survivors forward as `Existing`: `add_entry` would
                        // restamp them as `Added` under the new snapshot and drop
                        // their file sequence number.
                        if survives(entry) {
                            manifest_writer.add_existing_entry((**entry).clone())?;
                        }
                    }

                    existing_files.push(manifest_writer.write_manifest_file().await?);
                }
            }
        }

        *self.deleted_entries.lock().unwrap() = Some(deleted_entries);

        Ok(existing_files)
    }
}

/// Transaction action that replaces one set of files with another.
///
/// `M` is sealed to [`Rewrite`] and [`Overwrite`] via the [`RewriteFilesAction`] /
/// [`OverwriteFilesAction`] type aliases below; `ReplaceFilesMode` itself stays
/// `pub(crate)` so no other type can be substituted for `M`.
#[allow(private_bounds)]
pub struct ReplaceFilesAction<M: ReplaceFilesMode> {
    target_size_bytes: u32,
    min_count_to_merge: u32,
    merge_enabled: bool,

    // below are properties used to create SnapshotProducer when commit
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
    added_delete_files: Vec<DataFile>,
    removed_data_files: Vec<DataFile>,
    removed_delete_files: Vec<DataFile>,
    snapshot_id: Option<i64>,
    new_data_file_sequence_number: Option<i64>,
    target_branch: Option<String>,
    enable_delete_filter_manager: bool,
    check_file_existence: bool,
    validate_from_snapshot_id: Option<i64>,

    _mode: PhantomData<M>,
}

/// Rewrites files without changing table data — compaction and friends.
pub type RewriteFilesAction = ReplaceFilesAction<Rewrite>;

/// Rewrites files as a logical overwrite.
pub type OverwriteFilesAction = ReplaceFilesAction<Overwrite>;

#[allow(private_bounds)]
impl<M: ReplaceFilesMode> ReplaceFilesAction<M> {
    pub fn new() -> Self {
        Self {
            target_size_bytes: MANIFEST_TARGET_SIZE_BYTES_DEFAULT,
            min_count_to_merge: MANIFEST_MIN_MERGE_COUNT_DEFAULT,
            merge_enabled: MANIFEST_MERGE_ENABLED_DEFAULT,
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::new(),
            added_data_files: Vec::new(),
            added_delete_files: Vec::new(),
            removed_data_files: Vec::new(),
            removed_delete_files: Vec::new(),
            snapshot_id: None,
            new_data_file_sequence_number: None,
            target_branch: None,
            enable_delete_filter_manager: false,
            check_file_existence: false,
            validate_from_snapshot_id: None,
            _mode: PhantomData,
        }
    }

    /// Add data files to the snapshot.
    pub fn add_data_files(mut self, data_files: impl IntoIterator<Item = DataFile>) -> Self {
        for file in data_files {
            match file.content_type() {
                DataContentType::Data => self.added_data_files.push(file),
                DataContentType::PositionDeletes | DataContentType::EqualityDeletes => {
                    self.added_delete_files.push(file)
                }
            }
        }

        self
    }

    /// Add remove files to the snapshot.
    pub fn delete_files(mut self, remove_data_files: impl IntoIterator<Item = DataFile>) -> Self {
        for file in remove_data_files {
            match file.content_type() {
                DataContentType::Data => self.removed_data_files.push(file),
                DataContentType::PositionDeletes | DataContentType::EqualityDeletes => {
                    self.removed_delete_files.push(file)
                }
            }
        }

        self
    }

    pub fn set_snapshot_properties(&mut self, properties: HashMap<String, String>) -> &mut Self {
        let target_size_bytes: u32 = properties
            .get(MANIFEST_TARGET_SIZE_BYTES)
            .and_then(|s| s.parse().ok())
            .unwrap_or(MANIFEST_TARGET_SIZE_BYTES_DEFAULT);
        let min_count_to_merge: u32 = properties
            .get(MANIFEST_MIN_MERGE_COUNT)
            .and_then(|s| s.parse().ok())
            .unwrap_or(MANIFEST_MIN_MERGE_COUNT_DEFAULT);
        let merge_enabled = properties
            .get(MANIFEST_MERGE_ENABLED)
            .and_then(|s| s.parse().ok())
            .unwrap_or(MANIFEST_MERGE_ENABLED_DEFAULT);

        self.target_size_bytes = target_size_bytes;
        self.min_count_to_merge = min_count_to_merge;
        self.merge_enabled = merge_enabled;
        self.snapshot_properties = properties;

        self
    }

    /// Set commit UUID for the snapshot.
    pub fn set_commit_uuid(&mut self, commit_uuid: Uuid) -> &mut Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    /// Set key metadata for manifest files.
    pub fn set_key_metadata(mut self, key_metadata: Vec<u8>) -> Self {
        self.key_metadata = Some(key_metadata);
        self
    }

    /// Set snapshot id
    pub fn set_snapshot_id(mut self, snapshot_id: i64) -> Self {
        self.snapshot_id = Some(snapshot_id);
        self
    }

    /// Enable delete filter manager for this snapshot.
    /// By default, delete filter manager is disabled.
    pub fn set_enable_delete_filter_manager(mut self, enable_delete_filter_manager: bool) -> Self {
        self.enable_delete_filter_manager = enable_delete_filter_manager;
        self
    }

    pub fn set_target_branch(mut self, target_branch: String) -> Self {
        self.target_branch = Some(target_branch);
        self
    }

    // If the compaction should use the sequence number of the snapshot at compaction start time for
    // new data files, instead of using the sequence number of the newly produced snapshot.
    // This avoids commit conflicts with updates that add newer equality deletes at a higher sequence number.
    pub fn set_new_data_file_sequence_number(mut self, seq: i64) -> Self {
        self.new_data_file_sequence_number = Some(seq);
        self
    }

    pub fn set_check_file_existence(mut self, check: bool) -> Self {
        self.check_file_existence = check;
        self
    }

    /// Sets the snapshot id this rewrite was planned/read against.
    ///
    /// At commit time, this is used to detect delete files added
    /// concurrently — since `snapshot_id` — for data files this rewrite is
    /// removing, and fail the commit rather than silently dropping them as
    /// dangling (see apache/iceberg#2308). If this is never called, no such
    /// validation is performed — this preserves prior behavior exactly for
    /// any caller not yet using this option, which matters here since this
    /// method is being added to an already-existing action rather than a
    /// brand-new one (unlike Java's `RewriteFiles.validateFromSnapshot`,
    /// which defaults to validating all history when unset — safe there only
    /// because the check has existed since the interface's introduction).
    pub fn validate_from_snapshot(mut self, snapshot_id: i64) -> Self {
        self.validate_from_snapshot_id = Some(snapshot_id);
        self
    }
}

#[async_trait::async_trait]
impl<M: ReplaceFilesMode> TransactionAction for ReplaceFilesAction<M> {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let mut snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.key_metadata.clone(),
            self.snapshot_id,
            self.snapshot_properties.clone(),
            self.added_data_files.clone(),
            self.added_delete_files.clone(),
            self.removed_data_files.clone(),
            self.removed_delete_files.clone(),
        );

        if let Some(seq) = self.new_data_file_sequence_number {
            snapshot_producer.set_new_data_file_sequence_number(seq);
        }

        if let Some(branch) = &self.target_branch {
            snapshot_producer.set_target_branch(branch.clone());
        }

        if self.enable_delete_filter_manager {
            snapshot_producer.enable_delete_filter_manager();
        }

        if self.check_file_existence {
            snapshot_producer.validate_data_file_changes().await?;
        }

        if let Some(starting_snapshot_id) = self.validate_from_snapshot_id {
            snapshot_producer
                .validate_no_new_deletes_for_data_files(
                    starting_snapshot_id,
                    self.new_data_file_sequence_number.is_some(),
                )
                .await?;
        }

        if self.merge_enabled {
            let process =
                MergeManifestProcess::new(self.target_size_bytes, self.min_count_to_merge);
            snapshot_producer
                .commit(ReplaceFilesOperation::<M>::new(), process)
                .await
        } else {
            snapshot_producer
                .commit(ReplaceFilesOperation::<M>::new(), DefaultManifestProcess)
                .await
        }
    }
}

impl<M: ReplaceFilesMode> Default for ReplaceFilesAction<M> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use uuid::Uuid;

    use super::{Overwrite, ReplaceFilesMode, ReplaceFilesOperation, Rewrite, RewriteFilesAction};
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Datum, Literal, MAIN_BRANCH,
        ManifestContentType, ManifestEntry, ManifestListWriter, ManifestStatus,
        ManifestWriterBuilder, Operation, Snapshot, SnapshotReference, SnapshotRetention, Struct,
        Summary,
    };
    use crate::table::Table;
    use crate::transaction::TransactionAction;
    use crate::transaction::snapshot::{SnapshotProduceOperation, SnapshotProducer};
    use crate::transaction::tests::{
        PARENT_SEQUENCE_NUMBER, PARENT_SNAPSHOT_ID, REMOVED_DELETE_FILE, RETAINED_DELETE_FILE,
        make_v2_minimal_table, make_v2_table_with_delete_manifest, position_delete_file,
    };
    use crate::{Error, ErrorKind};

    #[test]
    fn test_modes_map_to_their_operations() {
        assert_eq!(Rewrite::OPERATION, Operation::Replace);
        assert_eq!(Overwrite::OPERATION, Operation::Overwrite);
        assert_eq!(
            ReplaceFilesOperation::<Rewrite>::new().operation(),
            Operation::Replace
        );
        assert_eq!(
            ReplaceFilesOperation::<Overwrite>::new().operation(),
            Operation::Overwrite
        );
    }

    /// Regression test: a rewrite/overwrite that removes one delete file must not
    /// mark *unrelated* delete files as deleted.
    ///
    /// `delete_entries` once guarded the delete-file branch with
    ///   `content == PositionDeletes || content == EqualityDeletes && removed.contains(path)`
    /// and because `&&` binds tighter than `||`, every `PositionDeletes` entry in
    /// the parent snapshot matched regardless of `removed_delete_file_paths`.
    async fn assert_only_removed_delete_files_marked<M: ReplaceFilesMode>() {
        let table = make_v2_table_with_delete_manifest().await;
        let removed = position_delete_file(&table, REMOVED_DELETE_FILE);

        let mut producer = SnapshotProducer::new(
            &table,
            Uuid::now_v7(),
            None,
            None,
            HashMap::new(),
            vec![],
            vec![],
            vec![],
            vec![removed],
        );

        let operation = ReplaceFilesOperation::<M>::new();
        // `delete_entries` now depends on `existing_manifest` having run first —
        // it populates the deleted-entries cache as a side effect of its single
        // pass over the manifest list. This matches the real call order in
        // `SnapshotProducer::commit`.
        operation.existing_manifest(&mut producer).await.unwrap();

        let deleted_entries = operation.delete_entries(&producer).await.unwrap();
        let deleted_paths: Vec<&str> = deleted_entries
            .iter()
            .map(|entry| entry.data_file().file_path())
            .collect();

        assert_eq!(
            deleted_paths,
            vec![REMOVED_DELETE_FILE],
            "only the removed delete file should be marked deleted; \
             {RETAINED_DELETE_FILE} must stay live"
        );
    }

    /// Regression test: rewriting a partially-deleted *delete* manifest must
    /// preserve its `Deletes` content type, and must carry survivors forward as
    /// `Existing` rather than restamping them as `Added`.
    async fn assert_delete_manifest_carried_forward_intact<M: ReplaceFilesMode>() {
        let table = make_v2_table_with_delete_manifest().await;
        let removed = position_delete_file(&table, REMOVED_DELETE_FILE);

        let mut producer = SnapshotProducer::new(
            &table,
            Uuid::now_v7(),
            None,
            None,
            HashMap::new(),
            vec![],
            vec![],
            vec![],
            vec![removed],
        );

        let existing = ReplaceFilesOperation::<M>::new()
            .existing_manifest(&mut producer)
            .await
            .unwrap();

        assert_eq!(existing.len(), 1, "the delete manifest should be rewritten");
        assert_eq!(
            existing[0].content,
            ManifestContentType::Deletes,
            "a rewritten delete manifest must stay a Deletes manifest"
        );

        let entries = existing[0].load_manifest(table.file_io()).await.unwrap();
        let paths: Vec<&str> = entries
            .entries()
            .iter()
            .map(|entry| entry.data_file().file_path())
            .collect();
        assert_eq!(paths, vec![RETAINED_DELETE_FILE]);

        let retained = &entries.entries()[0];
        assert_eq!(retained.status(), ManifestStatus::Existing);
        assert_eq!(retained.snapshot_id(), Some(PARENT_SNAPSHOT_ID));
        assert_eq!(retained.sequence_number(), Some(PARENT_SEQUENCE_NUMBER));
        assert_eq!(
            retained.file_sequence_number(),
            Some(PARENT_SEQUENCE_NUMBER)
        );
    }

    /// `delete_entries` no longer scans manifests itself — it drains the cache
    /// `existing_manifest` populates as a side effect of its own pass. Calling
    /// it before `existing_manifest` has run must not panic or fabricate
    /// deletes; it should just come back empty. Documents the ordering
    /// dependency so a future reorder of the calls in `SnapshotProducer::commit`
    /// fails a test instead of silently dropping deletes in production.
    #[tokio::test]
    async fn test_delete_entries_without_prior_existing_manifest_call_is_empty() {
        let table = make_v2_table_with_delete_manifest().await;
        let removed = position_delete_file(&table, REMOVED_DELETE_FILE);

        let producer = SnapshotProducer::new(
            &table,
            Uuid::now_v7(),
            None,
            None,
            HashMap::new(),
            vec![],
            vec![],
            vec![],
            vec![removed],
        );

        let deleted_entries = ReplaceFilesOperation::<Rewrite>::new()
            .delete_entries(&producer)
            .await
            .unwrap();

        assert!(
            deleted_entries.is_empty(),
            "delete_entries without a prior existing_manifest call should be \
             empty, not fabricate results: {deleted_entries:?}"
        );
    }

    #[tokio::test]
    async fn test_overwrite_only_marks_removed_delete_files() {
        assert_only_removed_delete_files_marked::<Overwrite>().await;
    }

    #[tokio::test]
    async fn test_rewrite_only_marks_removed_delete_files() {
        assert_only_removed_delete_files_marked::<Rewrite>().await;
    }

    #[tokio::test]
    async fn test_overwrite_preserves_delete_manifest_content_type() {
        assert_delete_manifest_carried_forward_intact::<Overwrite>().await;
    }

    #[tokio::test]
    async fn test_rewrite_preserves_delete_manifest_content_type() {
        assert_delete_manifest_carried_forward_intact::<Rewrite>().await;
    }

    // --- `validate_from_snapshot` / apache/iceberg#2308 regression tests ---
    //
    // These exercise the full `RewriteFilesAction` builder through
    // `TransactionAction::commit`, matching how a real caller (e.g. the
    // compaction service) uses this API, rather than calling the validator
    // directly — this also catches wiring bugs between the builder and
    // `SnapshotProducer::validate_no_new_deletes_for_data_files`.

    const STARTING_SNAPSHOT_ID: i64 = 200;
    const STARTING_SEQUENCE_NUMBER: i64 = 5;
    const CONCURRENT_SNAPSHOT_ID: i64 = 201;
    const CONCURRENT_SEQUENCE_NUMBER: i64 = 6;
    const TARGET_DATA_FILE: &str = "test/target-data-file.parquet";
    const OTHER_DATA_FILE: &str = "test/other-data-file.parquet";

    fn data_file(table: &Table, path: &str) -> DataFile {
        DataFileBuilder::default()
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .build()
            .unwrap()
    }

    /// A position delete / deletion vector. `is_dv` only sets the Puffin
    /// `content_offset`/`content_size_in_bytes` fields — our matching logic
    /// (like `ManifestFilterManager::is_dangling_delete`) doesn't distinguish
    /// DVs from legacy position deletes, since both share
    /// `DataContentType::PositionDeletes`; this is set purely so the DV test
    /// below is testing something that actually looks like a DV.
    fn position_delete_referencing(table: &Table, path: &str, referenced: &str, is_dv: bool) -> DataFile {
        let mut builder = DataFileBuilder::default();
        builder
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .content(DataContentType::PositionDeletes)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Puffin)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition(Struct::from_iter([Some(Literal::long(300))]))
            .referenced_data_file(Some(referenced.to_string()));
        if is_dv {
            builder.content_offset(Some(4)).content_size_in_bytes(Some(96));
        }
        builder.build().unwrap()
    }

    fn equality_delete_in_partition(table: &Table, path: &str, partition_value: i64) -> DataFile {
        DataFileBuilder::default()
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .content(DataContentType::EqualityDeletes)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition(Struct::from_iter([Some(Literal::long(partition_value))]))
            .equality_ids(Some(vec![1]))
            .build()
            .unwrap()
    }

    fn equality_delete(table: &Table, path: &str) -> DataFile {
        equality_delete_in_partition(table, path, 300)
    }

    /// Iceberg field id for the `file_path` column in position delete files (matches
    /// `delete_file_index.rs`'s `FIELD_ID_POSITIONAL_DELETE_FILE_PATH`).
    const FIELD_ID_POSITIONAL_DELETE_FILE_PATH: i32 = 2147483546;

    /// A position delete with no `referenced_data_file` — can span multiple data files, unlike
    /// `position_delete_referencing`. `bounds` optionally sets matching lower/upper bounds on
    /// the `file_path` column; when `lower == upper`, the matcher can infer it's effectively
    /// scoped to a single file even without `referenced_data_file` set.
    fn multi_file_position_delete(table: &Table, path: &str, bounds: Option<(&str, &str)>) -> DataFile {
        let mut builder = DataFileBuilder::default();
        builder
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .content(DataContentType::PositionDeletes)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(1)
            .partition(Struct::from_iter([Some(Literal::long(300))]));
        if let Some((lower, upper)) = bounds {
            builder
                .lower_bounds(HashMap::from([(
                    FIELD_ID_POSITIONAL_DELETE_FILE_PATH,
                    Datum::string(lower),
                )]))
                .upper_bounds(HashMap::from([(
                    FIELD_ID_POSITIONAL_DELETE_FILE_PATH,
                    Datum::string(upper),
                )]));
        }
        builder.build().unwrap()
    }

    /// Builds a V2 table with two snapshots on `main`:
    /// - `STARTING_SNAPSHOT_ID`: the snapshot a rewrite is presumed to have
    ///   planned against. Empty manifest list — the validator never inspects
    ///   data manifests, only delete manifests added after this point.
    /// - `CONCURRENT_SNAPSHOT_ID` (parent = `STARTING_SNAPSHOT_ID`, operation
    ///   = `concurrent_op`): simulates a concurrent writer's commit. Its
    ///   manifest list holds one delete manifest containing `delete_files`,
    ///   each written at the given sequence number (independent per file, so
    ///   tests can place an already-stale delete alongside a fresh one).
    async fn make_v2_table_with_concurrent_snapshot(
        concurrent_op: Operation,
        delete_files: Vec<(DataFile, i64)>,
    ) -> Table {
        let base = make_v2_minimal_table();
        let metadata = base
            .metadata()
            .clone()
            .into_builder(Some("s3://bucket/test/location/metadata/v1.json".into()))
            .set_location("memory:///test/location".to_string())
            .build()
            .unwrap()
            .metadata;
        let base = base.with_metadata(Arc::new(metadata));
        let file_io = base.file_io().clone();

        let starting_manifest_list_path =
            "memory:///test/location/metadata/manifest-list-starting.avro";
        let starting_manifest_list_writer = ManifestListWriter::v2(
            file_io.new_output(starting_manifest_list_path).unwrap(),
            STARTING_SNAPSHOT_ID,
            None,
            STARTING_SEQUENCE_NUMBER,
        );
        starting_manifest_list_writer.close().await.unwrap();

        let starting_snapshot = Snapshot::builder()
            .with_snapshot_id(STARTING_SNAPSHOT_ID)
            .with_timestamp_ms(base.metadata().last_updated_ms() + 1)
            .with_sequence_number(STARTING_SEQUENCE_NUMBER)
            .with_schema_id(0)
            .with_manifest_list(starting_manifest_list_path)
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .build();

        let mut metadata_builder = base
            .metadata()
            .clone()
            .into_builder(Some("s3://bucket/test/location/metadata/v1.json".into()))
            .add_snapshot(starting_snapshot)
            .unwrap();

        let table_with_starting_snapshot = base.clone().with_metadata(Arc::new(
            metadata_builder.clone().build().unwrap().metadata,
        ));

        if !delete_files.is_empty() {
            let mut delete_manifest_writer = ManifestWriterBuilder::new(
                file_io
                    .new_output("memory:///test/location/metadata/delete-manifest-concurrent.avro")
                    .unwrap(),
                Some(CONCURRENT_SNAPSHOT_ID),
                None,
                table_with_starting_snapshot.metadata().current_schema().clone(),
                table_with_starting_snapshot
                    .metadata()
                    .default_partition_spec()
                    .as_ref()
                    .clone(),
            )
            .build_v2_deletes();
            for (delete_file, sequence_number) in &delete_files {
                let entry = ManifestEntry::builder()
                    .status(ManifestStatus::Added)
                    .data_file(delete_file.clone())
                    .sequence_number_opt(Some(*sequence_number))
                    .build();
                delete_manifest_writer.add_entry(entry).unwrap();
            }
            let delete_manifest = delete_manifest_writer.write_manifest_file().await.unwrap();

            let concurrent_manifest_list_path =
                "memory:///test/location/metadata/manifest-list-concurrent.avro";
            let mut concurrent_manifest_list_writer = ManifestListWriter::v2(
                file_io.new_output(concurrent_manifest_list_path).unwrap(),
                CONCURRENT_SNAPSHOT_ID,
                Some(STARTING_SNAPSHOT_ID),
                CONCURRENT_SEQUENCE_NUMBER,
            );
            concurrent_manifest_list_writer
                .add_manifests(vec![delete_manifest].into_iter())
                .unwrap();
            concurrent_manifest_list_writer.close().await.unwrap();

            let concurrent_snapshot = Snapshot::builder()
                .with_snapshot_id(CONCURRENT_SNAPSHOT_ID)
                .with_parent_snapshot_id(Some(STARTING_SNAPSHOT_ID))
                .with_timestamp_ms(table_with_starting_snapshot.metadata().last_updated_ms() + 2)
                .with_sequence_number(CONCURRENT_SEQUENCE_NUMBER)
                .with_schema_id(0)
                .with_manifest_list(concurrent_manifest_list_path)
                .with_summary(Summary {
                    operation: concurrent_op,
                    additional_properties: HashMap::new(),
                })
                .build();

            metadata_builder = metadata_builder.add_snapshot(concurrent_snapshot).unwrap();
        }

        let tip_snapshot_id = if delete_files.is_empty() {
            STARTING_SNAPSHOT_ID
        } else {
            CONCURRENT_SNAPSHOT_ID
        };

        let metadata = metadata_builder
            .set_ref(MAIN_BRANCH, SnapshotReference {
                snapshot_id: tip_snapshot_id,
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            })
            .unwrap()
            .build()
            .unwrap()
            .metadata;

        base.with_metadata(Arc::new(metadata))
    }

    fn rewrite_action(removed: DataFile) -> RewriteFilesAction {
        RewriteFilesAction::new()
            .delete_files(vec![removed])
            .set_target_branch(MAIN_BRANCH.to_string())
    }

    /// `ActionCommit` doesn't implement `Debug`, so `Result::unwrap_err`
    /// can't be used directly on `TransactionAction::commit`'s return value.
    fn expect_err(result: crate::error::Result<crate::transaction::ActionCommit>) -> Error {
        match result {
            Err(e) => e,
            Ok(_) => panic!("expected commit to fail, but it succeeded"),
        }
    }

    #[tokio::test]
    async fn test_validate_from_snapshot_ok_when_tip_is_starting_snapshot() {
        // No concurrent commit at all — the rewrite's starting snapshot is
        // still the branch tip.
        let table = make_v2_table_with_concurrent_snapshot(Operation::Append, vec![]).await;
        let removed = data_file(&table, TARGET_DATA_FILE);

        let action = rewrite_action(removed).validate_from_snapshot(STARTING_SNAPSHOT_ID);
        assert!(Arc::new(action).commit(&table).await.is_ok());
    }

    #[tokio::test]
    async fn test_validate_from_snapshot_fails_on_new_delete_via_append_ancestor() {
        // This fork has no `RowDelta` action — `FastAppendAction` is the
        // only way to add data + delete files together without also
        // removing data files (confirmed: `add_data_files` routes
        // position/equality-delete content into `added_delete_files`, same
        // as `ReplaceFilesAction`), and it is *unconditionally*
        // `Operation::Append` regardless of content. So a real concurrent
        // writer here plausibly commits its new deletes under `Append`, not
        // `Overwrite`/`Delete` as Java's `RowDelta` would — the ancestor
        // scan must not filter by operation type, or this exact case would
        // be silently missed.
        let new_delete =
            position_delete_referencing(&make_v2_minimal_table(), "test/del.parquet", TARGET_DATA_FILE, false);
        let table = make_v2_table_with_concurrent_snapshot(
            Operation::Append,
            vec![(new_delete, CONCURRENT_SEQUENCE_NUMBER)],
        )
        .await;
        let removed = data_file(&table, TARGET_DATA_FILE);

        let action = rewrite_action(removed)
            .validate_from_snapshot(STARTING_SNAPSHOT_ID)
            .set_new_data_file_sequence_number(STARTING_SEQUENCE_NUMBER);

        let err = expect_err(Arc::new(action).commit(&table).await);
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message().contains("found new position delete"),
            "unexpected message: {}",
            err.message()
        );
    }

    #[tokio::test]
    async fn test_validate_from_snapshot_fails_on_new_position_delete() {
        let new_delete =
            position_delete_referencing(&make_v2_minimal_table(), "test/del.parquet", TARGET_DATA_FILE, false);
        let table = make_v2_table_with_concurrent_snapshot(
            Operation::Overwrite,
            vec![(new_delete, CONCURRENT_SEQUENCE_NUMBER)],
        )
        .await;
        let removed = data_file(&table, TARGET_DATA_FILE);

        let action = rewrite_action(removed)
            .validate_from_snapshot(STARTING_SNAPSHOT_ID)
            .set_new_data_file_sequence_number(STARTING_SEQUENCE_NUMBER);

        let err = expect_err(Arc::new(action).commit(&table).await);
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message().contains("found new position delete"),
            "unexpected message: {}",
            err.message()
        );
        assert!(!err.retryable(), "conflict must not be retryable");
        assert!(
            err.context().contains(&(
                crate::transaction::CONCURRENT_DELETE_CONFLICT_CONTEXT_KEY,
                crate::transaction::CONCURRENT_DELETE_CONFLICT_CONTEXT_VALUE.to_string()
            )),
            "conflict must carry the stable context marker callers key off of: {:?}",
            err.context()
        );
    }

    #[tokio::test]
    async fn test_validate_from_snapshot_fails_on_new_deletion_vector() {
        let new_dv =
            position_delete_referencing(&make_v2_minimal_table(), "test/del.puffin", TARGET_DATA_FILE, true);
        let table = make_v2_table_with_concurrent_snapshot(
            Operation::Overwrite,
            vec![(new_dv, CONCURRENT_SEQUENCE_NUMBER)],
        )
        .await;
        let removed = data_file(&table, TARGET_DATA_FILE);

        let action = rewrite_action(removed)
            .validate_from_snapshot(STARTING_SNAPSHOT_ID)
            .set_new_data_file_sequence_number(STARTING_SEQUENCE_NUMBER);

        let err = expect_err(Arc::new(action).commit(&table).await);
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message().contains("found new position delete"),
            "a deletion vector must be caught the same way as a legacy position delete: {}",
            err.message()
        );
    }

    #[tokio::test]
    async fn test_validate_from_snapshot_ignores_new_equality_delete_with_seq_override() {
        let new_delete = equality_delete(&make_v2_minimal_table(), "test/eq-del.parquet");
        let table = make_v2_table_with_concurrent_snapshot(
            Operation::Overwrite,
            vec![(new_delete, CONCURRENT_SEQUENCE_NUMBER)],
        )
        .await;
        let removed = data_file(&table, TARGET_DATA_FILE);

        let action = rewrite_action(removed)
            .validate_from_snapshot(STARTING_SNAPSHOT_ID)
            .set_new_data_file_sequence_number(STARTING_SEQUENCE_NUMBER);

        assert!(Arc::new(action).commit(&table).await.is_ok());
    }

    #[tokio::test]
    async fn test_validate_from_snapshot_fails_on_new_equality_delete_without_seq_override() {
        let new_delete = equality_delete(&make_v2_minimal_table(), "test/eq-del.parquet");
        let table = make_v2_table_with_concurrent_snapshot(
            Operation::Overwrite,
            vec![(new_delete, CONCURRENT_SEQUENCE_NUMBER)],
        )
        .await;
        let removed = data_file(&table, TARGET_DATA_FILE);

        // No `set_new_data_file_sequence_number` this time — equality deletes
        // are not exempted, matching Java's `ignoreEqualityDeletes = false`.
        let action = rewrite_action(removed).validate_from_snapshot(STARTING_SNAPSHOT_ID);

        let err = expect_err(Arc::new(action).commit(&table).await);
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message().contains("found new delete"),
            "unexpected message: {}",
            err.message()
        );
    }

    #[tokio::test]
    async fn test_validate_from_snapshot_ok_when_delete_targets_different_file() {
        let new_delete = position_delete_referencing(
            &make_v2_minimal_table(),
            "test/del.parquet",
            OTHER_DATA_FILE,
            false,
        );
        let table = make_v2_table_with_concurrent_snapshot(
            Operation::Overwrite,
            vec![(new_delete, CONCURRENT_SEQUENCE_NUMBER)],
        )
        .await;
        let removed = data_file(&table, TARGET_DATA_FILE);

        let action = rewrite_action(removed).validate_from_snapshot(STARTING_SNAPSHOT_ID);
        assert!(Arc::new(action).commit(&table).await.is_ok());
    }

    #[tokio::test]
    async fn test_validate_from_snapshot_not_called_skips_validation() {
        // Same conflicting setup as `test_validate_from_snapshot_fails_on_new_position_delete`,
        // but the action never opts into the new check — deliberate
        // divergence from Java (see snapshot.rs doc comment): preserves prior
        // behavior for any caller not yet using this option.
        let new_delete =
            position_delete_referencing(&make_v2_minimal_table(), "test/del.parquet", TARGET_DATA_FILE, false);
        let table = make_v2_table_with_concurrent_snapshot(
            Operation::Overwrite,
            vec![(new_delete, CONCURRENT_SEQUENCE_NUMBER)],
        )
        .await;
        let removed = data_file(&table, TARGET_DATA_FILE);

        let action = rewrite_action(removed);
        assert!(Arc::new(action).commit(&table).await.is_ok());
    }

    #[tokio::test]
    async fn test_validate_from_snapshot_ok_when_delete_predates_starting_snapshot() {
        // The delete is placed in the "concurrent" snapshot's manifest but
        // stamped with a sequence number <= the starting snapshot's — i.e.
        // it was already known as of the plan-time read, so the existing
        // dangling-delete-drop behavior (not this validator) is what's
        // supposed to clean it up. Isolates the sequence-number filter.
        let stale_delete =
            position_delete_referencing(&make_v2_minimal_table(), "test/del.parquet", TARGET_DATA_FILE, false);
        let table = make_v2_table_with_concurrent_snapshot(
            Operation::Overwrite,
            vec![(stale_delete, STARTING_SEQUENCE_NUMBER)],
        )
        .await;
        let removed = data_file(&table, TARGET_DATA_FILE);

        let action = rewrite_action(removed).validate_from_snapshot(STARTING_SNAPSHOT_ID);
        assert!(Arc::new(action).commit(&table).await.is_ok());
    }

    #[tokio::test]
    async fn test_validate_from_snapshot_fails_when_starting_snapshot_not_in_metadata() {
        // `starting_snapshot_id` given to the action doesn't exist anywhere in table metadata
        // at all (simulates it having expired via GC between plan time and commit time) —
        // caught by the fail-fast lookup, before the separate ancestry-continuity walk even
        // runs.
        let table = make_v2_table_with_concurrent_snapshot(Operation::Append, vec![]).await;
        let removed = data_file(&table, TARGET_DATA_FILE);

        const EXPIRED_SNAPSHOT_ID: i64 = 999_999;
        let action = rewrite_action(removed).validate_from_snapshot(EXPIRED_SNAPSHOT_ID);

        let err = expect_err(Arc::new(action).commit(&table).await);
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message().contains("is not present in the table metadata"),
            "unexpected message: {}",
            err.message()
        );
        assert!(
            err.context().contains(&(
                crate::transaction::CONCURRENT_DELETE_CONFLICT_CONTEXT_KEY,
                crate::transaction::CONCURRENT_DELETE_CONFLICT_CONTEXT_VALUE.to_string()
            )),
            "must also carry the stable context marker: {:?}",
            err.context()
        );
    }

    #[tokio::test]
    async fn test_validate_from_snapshot_fails_when_starting_snapshot_not_an_ancestor() {
        // `starting_snapshot_id` *is* present in table metadata (survives the fail-fast lookup
        // above) but is not actually an ancestor of the current branch tip — e.g. a
        // disconnected snapshot added to metadata outside the main
        // `STARTING_SNAPSHOT_ID -> CONCURRENT_SNAPSHOT_ID` chain. Must still be caught, via the
        // separate ancestry-continuity check.
        let table = make_v2_table_with_concurrent_snapshot(Operation::Append, vec![]).await;

        const DISCONNECTED_SNAPSHOT_ID: i64 = 12_345;
        let disconnected_snapshot = Snapshot::builder()
            .with_snapshot_id(DISCONNECTED_SNAPSHOT_ID)
            .with_timestamp_ms(table.metadata().last_updated_ms() + 10)
            .with_sequence_number(CONCURRENT_SEQUENCE_NUMBER + 1)
            .with_schema_id(0)
            .with_manifest_list("memory:///test/location/metadata/manifest-list-disconnected.avro")
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .build();
        let metadata = table
            .metadata()
            .clone()
            .into_builder(Some("s3://bucket/test/location/metadata/v3.json".into()))
            .add_snapshot(disconnected_snapshot)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let table = table.with_metadata(Arc::new(metadata));

        let removed = data_file(&table, TARGET_DATA_FILE);
        let action = rewrite_action(removed).validate_from_snapshot(DISCONNECTED_SNAPSHOT_ID);

        let err = expect_err(Arc::new(action).commit(&table).await);
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message().contains("Cannot determine history"),
            "unexpected message: {}",
            err.message()
        );
        assert!(
            err.context().contains(&(
                crate::transaction::CONCURRENT_DELETE_CONFLICT_CONTEXT_KEY,
                crate::transaction::CONCURRENT_DELETE_CONFLICT_CONTEXT_VALUE.to_string()
            )),
            "broken ancestry must also carry the stable context marker: {:?}",
            err.context()
        );
    }

    #[tokio::test]
    async fn test_validate_from_snapshot_ok_when_equality_delete_in_different_partition() {
        // A new equality delete lands in a partition (999) that doesn't match the removed
        // file's partition (300) — must not be a false-positive conflict now that equality
        // deletes are matched via the same partition-scoped index as position deletes.
        let new_delete = equality_delete_in_partition(&make_v2_minimal_table(), "test/eq-del.parquet", 999);
        let table = make_v2_table_with_concurrent_snapshot(
            Operation::Overwrite,
            vec![(new_delete, CONCURRENT_SEQUENCE_NUMBER)],
        )
        .await;
        let removed = data_file(&table, TARGET_DATA_FILE);

        let action = rewrite_action(removed).validate_from_snapshot(STARTING_SNAPSHOT_ID);
        assert!(Arc::new(action).commit(&table).await.is_ok());
    }

    #[tokio::test]
    async fn test_validate_from_snapshot_fails_on_multi_file_position_delete() {
        // A position delete with no `referenced_data_file` and no bounds narrowing it to a
        // single file — previously silently ignored entirely. Must now be caught via the
        // partition-scoped index's conservative "assume it might match" fallback.
        let new_delete = multi_file_position_delete(&make_v2_minimal_table(), "test/multi-del.parquet", None);
        let table = make_v2_table_with_concurrent_snapshot(
            Operation::Overwrite,
            vec![(new_delete, CONCURRENT_SEQUENCE_NUMBER)],
        )
        .await;
        let removed = data_file(&table, TARGET_DATA_FILE);

        let action = rewrite_action(removed)
            .validate_from_snapshot(STARTING_SNAPSHOT_ID)
            .set_new_data_file_sequence_number(STARTING_SEQUENCE_NUMBER);

        let err = expect_err(Arc::new(action).commit(&table).await);
        assert_eq!(err.kind(), ErrorKind::DataInvalid);
        assert!(
            err.message().contains("found new position delete file"),
            "unexpected message: {}",
            err.message()
        );
    }

    #[tokio::test]
    async fn test_validate_from_snapshot_ok_when_multi_file_position_delete_bounds_exclude_target() {
        // A position delete with no `referenced_data_file`, but whose `file_path` column
        // bounds prove it only ever touches a *different* single file — must not be a
        // false-positive conflict against our target file.
        let new_delete = multi_file_position_delete(
            &make_v2_minimal_table(),
            "test/multi-del.parquet",
            Some((OTHER_DATA_FILE, OTHER_DATA_FILE)),
        );
        let table = make_v2_table_with_concurrent_snapshot(
            Operation::Overwrite,
            vec![(new_delete, CONCURRENT_SEQUENCE_NUMBER)],
        )
        .await;
        let removed = data_file(&table, TARGET_DATA_FILE);

        let action = rewrite_action(removed)
            .validate_from_snapshot(STARTING_SNAPSHOT_ID)
            .set_new_data_file_sequence_number(STARTING_SEQUENCE_NUMBER);
        assert!(Arc::new(action).commit(&table).await.is_ok());
    }
}
