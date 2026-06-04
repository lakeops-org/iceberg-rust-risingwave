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
use std::sync::Arc;

use uuid::Uuid;

use super::snapshot::{DefaultManifestProcess, MergeManifestProcess, SnapshotProducer};
use super::{
    MANIFEST_MERGE_ENABLED, MANIFEST_MERGE_ENABLED_DEFAULT, MANIFEST_MIN_MERGE_COUNT,
    MANIFEST_MIN_MERGE_COUNT_DEFAULT, MANIFEST_TARGET_SIZE_BYTES,
    MANIFEST_TARGET_SIZE_BYTES_DEFAULT,
};
use crate::error::Result;
use crate::spec::{
    DataContentType, DataFile, ManifestContentType, ManifestEntry, ManifestFile, ManifestStatus,
    Operation,
};
use crate::table::Table;
use crate::transaction::snapshot::SnapshotProduceOperation;
use crate::transaction::{ActionCommit, TransactionAction};

/// Transaction action for rewriting files.
pub struct RewriteFilesAction {
    // snapshot_produce_action: SnapshotProduceAction<'a>,
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
}

pub struct RewriteFilesOperation;

impl RewriteFilesAction {
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

    /// Enable delete filter manager for this snapshot.
    /// By default, delete filter manager is disabled.
    pub fn set_enable_delete_filter_manager(mut self, enable_delete_filter_manager: bool) -> Self {
        self.enable_delete_filter_manager = enable_delete_filter_manager;
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
}

impl SnapshotProduceOperation for RewriteFilesOperation {
    fn operation(&self) -> Operation {
        Operation::Replace
    }

    async fn delete_entries(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        // generate delete manifest entries from removed files
        let snapshot = snapshot_produce
            .table
            .metadata()
            .snapshot_for_ref(snapshot_produce.target_branch());

        if let Some(snapshot) = snapshot {
            let gen_manifest_entry = |old_entry: &Arc<ManifestEntry>| {
                let builder = ManifestEntry::builder()
                    .status(ManifestStatus::Deleted)
                    .snapshot_id(old_entry.snapshot_id().unwrap())
                    .sequence_number(old_entry.sequence_number().unwrap())
                    .file_sequence_number(old_entry.file_sequence_number().unwrap())
                    .data_file(old_entry.data_file().clone());

                builder.build()
            };

            let manifest_list = snapshot
                .load_manifest_list(
                    snapshot_produce.table.file_io(),
                    snapshot_produce.table.metadata(),
                )
                .await?;

            let mut deleted_entries = Vec::new();

            for manifest_file in manifest_list.entries() {
                let manifest = manifest_file
                    .load_manifest(snapshot_produce.table.file_io())
                    .await?;

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
            }

            Ok(deleted_entries)
        } else {
            Ok(vec![])
        }
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

        let mut existing_files = Vec::new();

        for manifest_file in manifest_list.entries() {
            let manifest = manifest_file.load_manifest(file_io_ref).await?;

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
                // Drop manifests that contain only Deleted entries and no live
                // files — they are tombstone-only manifests left over from prior
                // snapshots and should not be carried forward (matches Java's
                // liveEntries() semantics: already-Deleted entries are dropped
                // from rewritten manifests, so pure-delete manifests become
                // empty and are not written).
                if manifest_file.has_added_files() || manifest_file.has_existing_files() {
                    existing_files.push(manifest_file.clone());
                }
            } else {
                // Rewrite the manifest file without the deleted data files
                if manifest
                    .entries()
                    .iter()
                    .any(|entry| !found_deleted_files.contains(entry.data_file().file_path()))
                {
                    let mut manifest_writer = snapshot_produce.new_manifest_writer(
                        ManifestContentType::Data,
                        manifest_file.partition_spec_id,
                    )?;

                    for entry in manifest.entries() {
                        // Skip entries already marked Deleted in prior snapshots —
                        // matches Java ManifestFilterManager which iterates
                        // liveEntries() (status != DELETED) only.
                        if entry.status() == ManifestStatus::Deleted {
                            continue;
                        }
                        if !found_deleted_files.contains(entry.data_file().file_path()) {
                            manifest_writer.add_entry_preserving_status((**entry).clone())?;
                        }
                    }

                    existing_files.push(manifest_writer.write_manifest_file().await?);
                }
            }
        }

        Ok(existing_files)
    }
}

#[async_trait::async_trait]
impl TransactionAction for RewriteFilesAction {
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

        if self.merge_enabled {
            let process =
                MergeManifestProcess::new(self.target_size_bytes, self.min_count_to_merge);
            snapshot_producer
                .commit(RewriteFilesOperation, process)
                .await
        } else {
            snapshot_producer
                .commit(RewriteFilesOperation, DefaultManifestProcess)
                .await
        }
    }
}

impl Default for RewriteFilesAction {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::memory::tests::new_memory_catalog;
    use crate::spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, ManifestStatus, NestedField,
        PrimitiveType, Schema, Struct, Type,
    };
    use crate::transaction::{ApplyTransactionAction, Transaction};
    use crate::{Catalog, NamespaceIdent, TableCreation, TableIdent};

    fn make_data_file(path: &str) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(10)
            .partition(Struct::empty())
            .build()
            .unwrap()
    }

    async fn make_unpartitioned_table(catalog: &impl Catalog) -> crate::table::Table {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap();
        let ns = NamespaceIdent::new(format!("ns_{}", uuid::Uuid::new_v4()));
        catalog.create_namespace(&ns, HashMap::new()).await.unwrap();
        catalog
            .create_table(
                &ns,
                TableCreation::builder()
                    .name("t".to_string())
                    .schema(schema)
                    .build(),
            )
            .await
            .unwrap()
    }

    /// Deleted entries written by a prior snapshot must be dropped — not
    /// carried forward — when rewrite_files rewrites a manifest.
    ///
    /// Scenario:
    ///   Snapshot 1: append A + B
    ///   Snapshot 2: overwrite_files deletes B  → manifest contains [B=Deleted]
    ///   Snapshot 3: rewrite_files replaces A with C
    ///
    /// After snap 3, B must not appear in any manifest (Java liveEntries()
    /// semantics). Before the fix it would be resurrected as Added.
    #[tokio::test]
    async fn test_rewrite_drops_prior_deleted_entries() {
        let catalog = new_memory_catalog().await;
        let table = make_unpartitioned_table(&catalog).await;

        let file_a = make_data_file("test/a.parquet");
        let file_b = make_data_file("test/b.parquet");
        let file_c = make_data_file("test/c.parquet");

        // Snapshot 1: append A + B
        let tx = Transaction::new(&table);
        let tx = tx
            .fast_append()
            .add_data_files(vec![file_a.clone(), file_b.clone()])
            .apply(tx)
            .unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Snapshot 2: delete B — creates a [B=Deleted] manifest entry
        let tx = Transaction::new(&table);
        let tx = tx
            .overwrite_files()
            .delete_files(vec![file_b.clone()])
            .apply(tx)
            .unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Snapshot 3: rewrite A → C (triggers the bug path)
        let tx = Transaction::new(&table);
        let tx = tx
            .rewrite_files()
            .add_data_files(vec![file_c.clone()])
            .delete_files(vec![file_a.clone()])
            .apply(tx)
            .unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Collect all entries from snapshot 3's manifest list
        let snap3 = table.metadata().current_snapshot().unwrap();
        let ml = snap3
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();

        let mut b_found = false;
        let mut saw_c_added = false;
        let mut saw_a_deleted = false;

        for mf in ml.entries() {
            let m = mf.load_manifest(table.file_io()).await.unwrap();
            for entry in m.entries() {
                let path = entry.data_file().file_path();
                if path.ends_with("b.parquet") {
                    b_found = true;
                }
                if path.ends_with("c.parquet") && entry.status() == ManifestStatus::Added {
                    saw_c_added = true;
                }
                if path.ends_with("a.parquet") && entry.status() == ManifestStatus::Deleted {
                    saw_a_deleted = true;
                }
            }
        }

        assert!(
            !b_found,
            "b.parquet was deleted in snapshot 2 and must not appear in snapshot 3's manifests"
        );
        assert!(saw_c_added, "c.parquet must appear as Added in snapshot 3");
        assert!(
            saw_a_deleted,
            "a.parquet must appear as Deleted in snapshot 3 (it was replaced)"
        );
    }

    /// Carried-forward live entries must be normalised to Existing (not remain
    /// Added), matching Java ManifestWriter.existing() behaviour.
    #[tokio::test]
    async fn test_rewrite_normalizes_carried_entries_to_existing() {
        let catalog = new_memory_catalog().await;
        let table = make_unpartitioned_table(&catalog).await;

        let file_a = make_data_file("test/a.parquet");
        let file_b = make_data_file("test/b.parquet");
        let file_c = make_data_file("test/c.parquet");

        // Snapshot 1: append A + B
        let tx = Transaction::new(&table);
        let tx = tx
            .fast_append()
            .add_data_files(vec![file_a.clone(), file_b.clone()])
            .apply(tx)
            .unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // Snapshot 2: rewrite A → C, keeping B (B carried forward)
        let tx = Transaction::new(&table);
        let tx = tx
            .rewrite_files()
            .add_data_files(vec![file_c.clone()])
            .delete_files(vec![file_a.clone()])
            .apply(tx)
            .unwrap();
        let table = tx.commit(&catalog).await.unwrap();

        // B was Added in snap 1; after being carried forward through a manifest
        // rewrite it must appear as Existing (Java alignment).
        let snap2 = table.metadata().current_snapshot().unwrap();
        let ml = snap2
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .unwrap();

        let mut b_status: Option<ManifestStatus> = None;
        for mf in ml.entries() {
            let m = mf.load_manifest(table.file_io()).await.unwrap();
            for entry in m.entries() {
                if entry.data_file().file_path().ends_with("b.parquet") {
                    b_status = Some(entry.status());
                }
            }
        }

        assert_eq!(
            b_status,
            Some(ManifestStatus::Existing),
            "b.parquet was Added in snap 1 but must be Existing after being carried forward"
        );
    }
}
