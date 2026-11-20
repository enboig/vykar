//! `vykar copy`: copy snapshots from one repository into another.
//!
//! The destination may use a different repository format, encryption mode or
//! key — this is how a format-v2 (BLAKE2b) repository is migrated to the
//! current format: initialize a new repository, copy every snapshot into it,
//! and retire the old one once the copy has been checked.
//!
//! Chunk IDs are keyed hashes and differ between any two repositories, so
//! nothing can be copied byte-for-byte. Every chunk a copied snapshot
//! references is read from the source once per run, verified against its
//! source chunk ID, and stored in the destination under the destination's
//! chunk ID (deduplicated like any backup). Chunk boundaries are kept: the
//! chunker is not keyed, so a destination with the same chunker parameters
//! deduplicates future backups against the copied data. Item streams are
//! rewritten with the new chunk IDs, and each snapshot keeps its name,
//! timestamps, host, source and comment.
//!
//! Each snapshot is committed on its own, exactly like a backup (session,
//! pack upload, commit under the lock), so an interrupted copy resumes at the
//! next run: snapshots whose name already exists in the destination are
//! skipped, and packs uploaded by the interrupted snapshot are recovered from
//! its session journal.
//!
//! Both repositories hold a session marker for the whole run, so neither can
//! be pruned or compacted underneath the copy.

use std::collections::HashSet;
use std::sync::atomic::AtomicBool;

use tracing::{info, warn};

use super::backup::{
    append_item_to_stream, flush_item_stream_chunk, items_chunker_config, phase2_commit,
    BackupProgressEvent, BackupSession,
};
use super::list::{for_each_decoded_item, load_item_stream_from_ptrs, load_snapshot_meta};
use super::util::{check_interrupted, open_repo_with_read_session};
use crate::compress::Compression;
use crate::config::VykarConfig;
use crate::index::hasher::ChunkIdHashMap;
use crate::repo::file_cache::FileCache;
use crate::repo::format::{pack_object_with_context, ObjectType};
use crate::repo::lock;
use crate::repo::manifest::SnapshotEntry;
use crate::repo::pack::PackType;
use crate::repo::{OpenOptions, Repository, WriteSessionOptions};
use crate::snapshot::item::Item;
use crate::snapshot::{SnapshotMeta, SnapshotStats, CURRENT_FORMAT_VERSION};
use vykar_types::chunk_id::ChunkId;
use vykar_types::error::{Result, VykarError};
use vykar_types::pack_id::PackId;
use vykar_types::snapshot_id::SnapshotId;

/// Source plaintext gathered before one batch of chunks is written to the
/// destination. Bounds the copy's read buffer.
const BATCH_BYTES: u64 = 64 * 1024 * 1024;

/// Items held back while their chunks are gathered. Keeps a long run of small
/// files to one coalesced read without letting the queue grow unbounded.
const MAX_PENDING_ITEMS: usize = 8192;

/// What to copy and how.
pub struct CopyRequest<'a> {
    /// Snapshot names (or `latest`) to copy; empty copies every snapshot.
    pub snapshots: &'a [String],
    /// Only copy snapshots of these source labels; empty means all.
    pub source_labels: &'a [String],
    /// Compression for chunks written to the destination.
    pub compression: Compression,
    /// Report what would be copied without writing anything.
    pub dry_run: bool,
}

#[derive(Debug, Clone)]
pub enum CopyProgressEvent {
    /// Copying of snapshot `index` (1-based) of `total` started.
    SnapshotStarted {
        name: String,
        index: usize,
        total: usize,
    },
    /// Periodic progress within the current snapshot.
    Progress {
        nfiles: u64,
        original_size: u64,
        /// Bytes newly stored in the destination so far.
        deduplicated_size: u64,
    },
    /// The snapshot is committed in the destination.
    SnapshotCopied { name: String, stats: SnapshotStats },
    /// A non-fatal issue; `tracing::warn!` has already fired.
    Warning { message: String },
}

#[derive(Debug, Clone)]
pub struct CopiedSnapshot {
    pub name: String,
    pub stats: SnapshotStats,
}

#[derive(Debug, Clone, Default)]
pub struct CopyOutcome {
    /// Snapshots committed to the destination (or, in a dry run, that would be).
    pub copied: Vec<CopiedSnapshot>,
    /// Selected snapshots skipped because the destination already has the name.
    pub already_present: Vec<String>,
    /// Source snapshots this binary cannot read (written by a newer vykar).
    pub hidden_in_source: usize,
    /// Non-fatal issues, also emitted as `CopyProgressEvent::Warning`.
    pub warnings: Vec<String>,
}

fn emit(progress: &mut Option<&mut dyn FnMut(CopyProgressEvent)>, event: CopyProgressEvent) {
    if let Some(cb) = progress.as_deref_mut() {
        cb(event);
    }
}

fn warn_event(
    outcome: &mut CopyOutcome,
    progress: &mut Option<&mut dyn FnMut(CopyProgressEvent)>,
    message: String,
) {
    warn!("{message}");
    emit(
        progress,
        CopyProgressEvent::Warning {
            message: message.clone(),
        },
    );
    outcome.warnings.push(message);
}

/// Copy snapshots from the repository of `src_config` into the (already
/// initialized) repository of `dst_config`.
pub fn run(
    src_config: &VykarConfig,
    src_passphrase: Option<&str>,
    dst_config: &VykarConfig,
    dst_passphrase: Option<&str>,
    req: &CopyRequest<'_>,
    mut progress: Option<&mut dyn FnMut(CopyProgressEvent)>,
    shutdown: Option<&AtomicBool>,
) -> Result<CopyOutcome> {
    let (mut src, _src_session) =
        open_repo_with_read_session(src_config, src_passphrase, OpenOptions::new().with_index())?;
    let (mut dst, _dst_session) =
        open_repo_with_read_session(dst_config, dst_passphrase, OpenOptions::new().with_index())?;

    if src.config.id == dst.config.id {
        return Err(VykarError::Config(
            "source and destination are the same repository".into(),
        ));
    }

    let mut outcome = CopyOutcome {
        hidden_in_source: src.skipped_snapshots().len(),
        ..Default::default()
    };
    if outcome.hidden_in_source > 0 {
        let msg = format!(
            "{} source snapshot(s) were written by a newer vykar and cannot be copied",
            outcome.hidden_in_source
        );
        warn_event(&mut outcome, &mut progress, msg);
    }
    if src.config.chunker_params != dst.config.chunker_params {
        let msg = "source and destination use different chunker parameters: copied \
                   snapshots keep the source's chunk boundaries, so new backups will \
                   not deduplicate against copied data"
            .to_string();
        warn_event(&mut outcome, &mut progress, msg);
    }

    let selected = select_snapshots(&src, req)?;
    let mut to_copy = Vec::with_capacity(selected.len());
    for entry in selected {
        if dst.manifest().find_snapshot(&entry.name).is_some() {
            outcome.already_present.push(entry.name);
        } else {
            to_copy.push(entry);
        }
    }

    if req.dry_run {
        for entry in &to_copy {
            let meta = load_snapshot_meta(&src, &entry.name)?;
            outcome.copied.push(CopiedSnapshot {
                name: entry.name.clone(),
                stats: meta.stats,
            });
        }
        return Ok(outcome);
    }

    // Every chunk is verified and hashed here; the source blob cache would
    // only hold plaintext the chunk map already makes redundant.
    src.set_blob_cache_max_bytes(0);
    let mut copier = Copier {
        compression: req.compression,
        map: ChunkIdHashMap::default(),
    };

    let total = to_copy.len();
    for (i, entry) in to_copy.into_iter().enumerate() {
        check_interrupted(shutdown)?;
        emit(
            &mut progress,
            CopyProgressEvent::SnapshotStarted {
                name: entry.name.clone(),
                index: i + 1,
                total,
            },
        );
        let stats = copy_snapshot(
            &mut src,
            &mut dst,
            &mut copier,
            &entry,
            &mut outcome,
            &mut progress,
            shutdown,
        )?;
        info!(snapshot = %entry.name, "snapshot copied");
        emit(
            &mut progress,
            CopyProgressEvent::SnapshotCopied {
                name: entry.name.clone(),
                stats: stats.clone(),
            },
        );
        outcome.copied.push(CopiedSnapshot {
            name: entry.name,
            stats,
        });
    }

    Ok(outcome)
}

/// The selected source snapshots, oldest first.
fn select_snapshots(src: &Repository, req: &CopyRequest<'_>) -> Result<Vec<SnapshotEntry>> {
    let manifest = src.manifest();
    let mut selected: Vec<SnapshotEntry> = if req.snapshots.is_empty() {
        manifest.snapshots.clone()
    } else {
        let mut seen = HashSet::new();
        let mut v = Vec::new();
        for query in req.snapshots {
            let entry = manifest.resolve_snapshot(query)?;
            if seen.insert(entry.id) {
                v.push(entry.clone());
            }
        }
        v
    };
    if !req.source_labels.is_empty() {
        selected.retain(|e| req.source_labels.contains(&e.source_label));
    }
    selected.sort_by_key(|e| e.time);
    Ok(selected)
}

/// Run-wide state: the source → destination chunk mapping. A chunk is read
/// from the source at most once per run, however many snapshots share it.
struct Copier {
    compression: Compression,
    /// Source chunk ID → (destination chunk ID, destination stored size).
    map: ChunkIdHashMap<(ChunkId, u32)>,
}

/// Per-snapshot state of the item rewrite.
struct SnapshotCopy {
    /// Items whose chunks are not all mapped yet, in stream order.
    pending_items: Vec<Item>,
    /// Unmapped source chunks referenced by `pending_items`, with their
    /// source locations and uncompressed sizes.
    wanted: Vec<(ChunkId, PackId, u64, u32)>,
    wanted_sizes: Vec<u32>,
    wanted_set: HashSet<ChunkId>,
    wanted_bytes: u64,
    /// References the destination already counted for a chunk when it was
    /// stored during this snapshot: the next that many references to it
    /// must not bump its refcount again.
    credits: ChunkIdHashMap<u32>,
    item_stream: Vec<u8>,
    item_ptrs: Vec<ChunkId>,
    stats: SnapshotStats,
}

fn copy_snapshot(
    src: &mut Repository,
    dst: &mut Repository,
    copier: &mut Copier,
    entry: &SnapshotEntry,
    outcome: &mut CopyOutcome,
    progress: &mut Option<&mut dyn FnMut(CopyProgressEvent)>,
    shutdown: Option<&AtomicBool>,
) -> Result<SnapshotStats> {
    let src_meta = load_snapshot_meta(src, &entry.name)?;
    let items_stream = load_item_stream_from_ptrs(src, &src_meta.item_ptrs, shutdown)?;

    // Same session lifecycle as a backup: register, write, commit under lock.
    let session_id = format!("{:032x}", rand::random::<u128>());
    lock::register_session(dst.storage.as_ref(), &session_id)?;
    let mut session = BackupSession::adopt(&dst.storage, session_id.clone())?;
    let session_opts = WriteSessionOptions::for_backup(session_id, dst.storage.profile());

    let written = write_snapshot(
        src,
        dst,
        copier,
        &src_meta,
        &items_stream,
        &session_opts,
        progress,
        shutdown,
    );
    let (meta, stats) = match written {
        Ok(v) => v,
        Err(e) => {
            dst.flush_on_abort();
            return Err(e);
        }
    };

    // Keep the source's snapshot ID unless the destination already uses it.
    let snapshot_id = if dst.manifest().snapshots.iter().any(|s| s.id == entry.id) {
        SnapshotId::generate()
    } else {
        entry.id
    };
    let meta_bytes = rmp_serde::to_vec(&meta)?;
    let snapshot_packed = pack_object_with_context(
        ObjectType::SnapshotMeta,
        snapshot_id.as_bytes(),
        &meta_bytes,
        dst.crypto.as_ref(),
    )?;
    let snapshot_entry = SnapshotEntry {
        name: meta.name.clone(),
        id: snapshot_id,
        time: meta.time,
        source_label: meta.source_label.clone(),
        label: meta.label.clone(),
        source_paths: meta.source_paths.clone(),
        hostname: meta.hostname.clone(),
    };

    let mut warnings = Vec::new();
    let mut on_backup_event = |evt: BackupProgressEvent| {
        if let BackupProgressEvent::Warning { message } = evt {
            warnings.push(message);
        }
    };
    let mut backup_progress: Option<&mut dyn FnMut(BackupProgressEvent)> =
        Some(&mut on_backup_event);
    let committed = phase2_commit(
        dst,
        &mut session,
        snapshot_entry,
        snapshot_packed,
        &mut FileCache::new(),
        &mut backup_progress,
        shutdown,
    );
    if committed.is_err() {
        dst.flush_on_abort();
    }
    committed?;
    for message in warnings {
        // Already logged by the commit path.
        emit(
            progress,
            CopyProgressEvent::Warning {
                message: message.clone(),
            },
        );
        outcome.warnings.push(message);
    }
    Ok(stats)
}

/// Phase 1 of copying one snapshot: rewrite its items into the destination
/// and upload every pack. Returns the destination snapshot metadata.
#[allow(clippy::too_many_arguments)]
fn write_snapshot(
    src: &mut Repository,
    dst: &mut Repository,
    copier: &mut Copier,
    src_meta: &SnapshotMeta,
    items_stream: &[u8],
    session_opts: &WriteSessionOptions,
    progress: &mut Option<&mut dyn FnMut(CopyProgressEvent)>,
    shutdown: Option<&AtomicBool>,
) -> Result<(SnapshotMeta, SnapshotStats)> {
    dst.begin_write_session(session_opts)?;
    match dst.recover_pending_index() {
        Ok(recovery) if recovery.recovered_chunks > 0 => info!(
            recovered_chunks = recovery.recovered_chunks,
            "recovered pending index from interrupted session"
        ),
        Ok(_) => {}
        Err(e) => warn!("failed to recover pending index: {e}"),
    }
    dst.enable_tiered_dedup_mode();

    let mut snap = SnapshotCopy {
        pending_items: Vec::new(),
        wanted: Vec::new(),
        wanted_sizes: Vec::new(),
        wanted_set: HashSet::new(),
        wanted_bytes: 0,
        credits: ChunkIdHashMap::default(),
        item_stream: Vec::new(),
        item_ptrs: Vec::new(),
        stats: SnapshotStats {
            nfiles: src_meta.stats.nfiles,
            errors: src_meta.stats.errors,
            ..Default::default()
        },
    };

    for_each_decoded_item(items_stream, |item| {
        check_interrupted(shutdown)?;
        snap.push_item(src, dst, copier, item, shutdown)?;
        if snap.pending_items.len() >= MAX_PENDING_ITEMS {
            snap.resolve_wanted(src, dst, copier, shutdown)?;
            snap.emit_items(dst, copier)?;
            emit(
                progress,
                CopyProgressEvent::Progress {
                    nfiles: snap.stats.nfiles,
                    original_size: snap.stats.original_size,
                    deduplicated_size: snap.stats.deduplicated_size,
                },
            );
        }
        Ok(())
    })?;
    snap.resolve_wanted(src, dst, copier, shutdown)?;
    snap.emit_items(dst, copier)?;
    if let Some(id) = snap.credits.keys().next() {
        return Err(VykarError::Other(format!(
            "internal error: stored chunk {id} was never referenced"
        )));
    }

    check_interrupted(shutdown)?;
    flush_item_stream_chunk(
        dst,
        &mut snap.item_stream,
        &mut snap.item_ptrs,
        copier.compression,
    )?;
    dst.flush_packs_journal_in_background()?;
    check_interrupted(shutdown)?;

    let meta = SnapshotMeta {
        name: src_meta.name.clone(),
        hostname: src_meta.hostname.clone(),
        username: src_meta.username.clone(),
        time: src_meta.time,
        time_end: src_meta.time_end,
        // The chunks keep the boundaries the source chunker cut.
        chunker_params: src_meta.chunker_params.clone(),
        comment: src_meta.comment.clone(),
        item_ptrs: std::mem::take(&mut snap.item_ptrs),
        stats: snap.stats.clone(),
        source_label: src_meta.source_label.clone(),
        source_paths: src_meta.source_paths.clone(),
        label: src_meta.label.clone(),
        ext: src_meta.ext.clone(),
        // The items were re-encoded by this binary.
        format_version: CURRENT_FORMAT_VERSION,
    };
    Ok((meta, snap.stats))
}

impl SnapshotCopy {
    /// Queue `item`, collecting the source chunks it needs that are not
    /// mapped yet. Gathered chunks are written out whenever they reach
    /// [`BATCH_BYTES`], so a single large file never has to fit in memory.
    fn push_item(
        &mut self,
        src: &mut Repository,
        dst: &mut Repository,
        copier: &mut Copier,
        item: Item,
        shutdown: Option<&AtomicBool>,
    ) -> Result<()> {
        for cr in &item.chunks {
            if copier.map.contains_key(&cr.id) || !self.wanted_set.insert(cr.id) {
                continue;
            }
            let loc = src.chunk_index().get(&cr.id).ok_or_else(|| {
                VykarError::Other(format!(
                    "chunk {} of '{}' is missing from the source index",
                    cr.id, item.path
                ))
            })?;
            self.wanted
                .push((cr.id, loc.pack_id, loc.pack_offset, loc.stored_size));
            self.wanted_sizes.push(cr.size);
            self.wanted_bytes += u64::from(cr.size);
            if self.wanted_bytes >= BATCH_BYTES {
                self.resolve_wanted(src, dst, copier, shutdown)?;
            }
        }
        self.pending_items.push(item);
        if self.wanted.is_empty() {
            self.emit_items(dst, copier)?;
        }
        Ok(())
    }

    /// Read every wanted chunk from the source, verify it against its source
    /// chunk ID, and store it in the destination.
    fn resolve_wanted(
        &mut self,
        src: &mut Repository,
        dst: &mut Repository,
        copier: &mut Copier,
        shutdown: Option<&AtomicBool>,
    ) -> Result<()> {
        if self.wanted.is_empty() {
            return Ok(());
        }
        let mut buf = Vec::with_capacity(usize::try_from(self.wanted_bytes).unwrap_or(0));
        src.read_chunks_coalesced_into(&self.wanted, &mut buf, shutdown)?;

        let mut offset = 0usize;
        for ((old_id, ..), &size) in self.wanted.iter().zip(&self.wanted_sizes) {
            let end = offset + size as usize;
            let data = buf.get(offset..end).ok_or_else(|| {
                VykarError::Other(format!(
                    "source chunk {old_id} is shorter than its recorded size"
                ))
            })?;
            offset = end;
            // Never launder a damaged source chunk into a fresh, valid ID.
            if ChunkId::compute(src.crypto.chunk_hasher(), data) != *old_id {
                return Err(VykarError::Other(format!(
                    "source chunk {old_id} does not match its chunk ID; run \
                     `vykar check --verify-data` on the source repository"
                )));
            }
            let (new_id, stored_size, is_new) =
                dst.store_chunk(data, copier.compression, PackType::Data)?;
            if is_new {
                self.stats.deduplicated_size += u64::from(stored_size);
            }
            *self.credits.entry(new_id).or_insert(0) += 1;
            copier.map.insert(*old_id, (new_id, stored_size));
        }
        if offset != buf.len() {
            return Err(VykarError::Other(
                "source chunks are longer than their recorded sizes".into(),
            ));
        }

        self.wanted.clear();
        self.wanted_sizes.clear();
        self.wanted_set.clear();
        self.wanted_bytes = 0;
        Ok(())
    }

    /// Rewrite every pending item's chunk references (all mapped by now) and
    /// append it to the destination item stream.
    fn emit_items(&mut self, dst: &mut Repository, copier: &Copier) -> Result<()> {
        let items_config = items_chunker_config();
        for mut item in self.pending_items.drain(..) {
            for cr in &mut item.chunks {
                let &(new_id, stored_size) = copier.map.get(&cr.id).ok_or_else(|| {
                    VykarError::Other(format!("internal error: chunk {} was not copied", cr.id))
                })?;
                match self.credits.get_mut(&new_id) {
                    Some(n) => {
                        *n -= 1;
                        if *n == 0 {
                            self.credits.remove(&new_id);
                        }
                    }
                    None => {
                        dst.bump_ref_if_exists(&new_id).ok_or_else(|| {
                            VykarError::Other(format!(
                                "copied chunk {new_id} is missing from the destination"
                            ))
                        })?;
                    }
                }
                cr.id = new_id;
                cr.csize = stored_size;
                self.stats.original_size += u64::from(cr.size);
                self.stats.compressed_size += u64::from(stored_size);
            }
            append_item_to_stream(
                dst,
                &mut self.item_stream,
                &mut self.item_ptrs,
                &item,
                &items_config,
                copier.compression,
            )?;
        }
        Ok(())
    }
}
