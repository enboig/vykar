use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};

use crate::commands;
use crate::commands::copy::{CopyOutcome, CopyRequest};
use crate::commands::list::{for_each_snapshot_item, load_snapshot_meta};
use crate::compress::Compression;
use crate::config::VykarConfig;
use vykar_types::chunk_id::ChunkId;

use super::helpers::{backup_single_source, init_repo, open_local_repo};

/// Deterministic incompressible bytes, so files span several chunks.
fn pseudo_random(seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
    (0..len)
        .map(|_| {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (state >> 33) as u8
        })
        .collect()
}

fn write_source(dir: &Path) {
    std::fs::create_dir_all(dir.join("sub")).unwrap();
    std::fs::write(dir.join("big.bin"), pseudo_random(1, 3 * 1024 * 1024)).unwrap();
    // Same content twice: dedups within the snapshot.
    std::fs::write(dir.join("sub/copy-a.bin"), pseudo_random(2, 700 * 1024)).unwrap();
    std::fs::write(dir.join("sub/copy-b.bin"), pseudo_random(2, 700 * 1024)).unwrap();
    std::fs::write(dir.join("small.txt"), b"hello").unwrap();
    std::fs::write(dir.join("empty"), b"").unwrap();
}

fn read_tree(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut out = BTreeMap::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else {
                let rel = path.strip_prefix(root).unwrap().to_path_buf();
                out.insert(rel, std::fs::read(&path).unwrap());
            }
        }
    }
    out
}

fn copy_all(src: &VykarConfig, dst: &VykarConfig) -> CopyOutcome {
    commands::copy::run(
        src,
        None,
        dst,
        None,
        &CopyRequest {
            snapshots: &[],
            source_labels: &[],
            compression: Compression::Lz4,
            dry_run: false,
        },
        None,
        None,
    )
    .unwrap()
}

fn restore(config: &VykarConfig, snapshot: &str, dest: &Path) {
    commands::restore::run(
        config,
        None,
        snapshot,
        dest.to_str().unwrap(),
        None,
        false,
        true,
        None,
    )
    .unwrap();
}

/// Two snapshots in a fresh source repository, the second after a change.
struct Setup {
    tmp: tempfile::TempDir,
    src: VykarConfig,
    dst: VykarConfig,
}

impl Setup {
    fn new() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("source");
        write_source(&source);
        let src = init_repo(&tmp.path().join("src-repo"));
        let dst = init_repo(&tmp.path().join("dst-repo"));
        backup_single_source(&src, &source, "docs", "snap-1");
        std::fs::write(source.join("small.txt"), b"hello, changed").unwrap();
        std::fs::write(source.join("added.bin"), pseudo_random(3, 200 * 1024)).unwrap();
        backup_single_source(&src, &source, "docs", "snap-2");
        Self { tmp, src, dst }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.tmp.path().join(name)
    }
}

#[test]
fn copy_restores_identical_trees_and_verifies_clean() {
    let s = Setup::new();
    let outcome = copy_all(&s.src, &s.dst);
    let names: Vec<_> = outcome.copied.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["snap-1", "snap-2"], "oldest first");
    assert!(outcome.already_present.is_empty());

    for snap in ["snap-1", "snap-2"] {
        let from_src = s.path(&format!("restore-src-{snap}"));
        let from_dst = s.path(&format!("restore-dst-{snap}"));
        restore(&s.src, snap, &from_src);
        restore(&s.dst, snap, &from_dst);
        assert_eq!(read_tree(&from_dst), read_tree(&from_src), "{snap}");
    }

    let check = commands::check::run(&s.dst, None, true, false).unwrap();
    assert!(check.errors.is_empty(), "{:?}", check.errors);
}

#[test]
fn copy_preserves_snapshot_metadata() {
    let s = Setup::new();
    copy_all(&s.src, &s.dst);
    let src_repo = open_local_repo(&s.path("src-repo"));
    let dst_repo = open_local_repo(&s.path("dst-repo"));
    for snap in ["snap-1", "snap-2"] {
        let a = load_snapshot_meta(&src_repo, snap).unwrap();
        let b = load_snapshot_meta(&dst_repo, snap).unwrap();
        assert_eq!(a.hostname, b.hostname);
        assert_eq!(a.username, b.username);
        assert_eq!(a.time, b.time);
        assert_eq!(a.time_end, b.time_end);
        assert_eq!(a.source_label, b.source_label);
        assert_eq!(a.source_paths, b.source_paths);
        assert_eq!(a.stats.nfiles, b.stats.nfiles);
        assert_eq!(a.stats.original_size, b.stats.original_size);
        let src_id = src_repo.manifest().find_snapshot(snap).unwrap().id;
        let dst_id = dst_repo.manifest().find_snapshot(snap).unwrap().id;
        assert_eq!(src_id, dst_id, "snapshot ID is kept");
    }
}

/// The destination index must count exactly one reference per chunk
/// reference in its snapshots, or delete/compact would free live data (too
/// few) or leak it forever (too many).
#[test]
fn copy_leaves_exact_refcounts() {
    let s = Setup::new();
    copy_all(&s.src, &s.dst);
    let mut repo = open_local_repo(&s.path("dst-repo"));

    let mut expected: HashMap<ChunkId, u32> = HashMap::new();
    let names: Vec<String> = repo
        .manifest()
        .snapshots
        .iter()
        .map(|e| e.name.clone())
        .collect();
    for name in &names {
        for id in load_snapshot_meta(&repo, name).unwrap().item_ptrs {
            *expected.entry(id).or_default() += 1;
        }
        for_each_snapshot_item(&mut repo, name, None, |item| {
            for cr in &item.chunks {
                *expected.entry(cr.id).or_default() += 1;
            }
            Ok(())
        })
        .unwrap();
    }

    let index = repo.chunk_index();
    assert_eq!(index.len(), expected.len(), "no unreferenced chunks");
    for (id, count) in &expected {
        let entry = index.get(id).unwrap_or_else(|| panic!("{id} missing"));
        assert_eq!(entry.refcount, *count, "refcount of {id}");
    }
}

#[test]
fn copy_is_resumable_and_skips_present_snapshots() {
    let s = Setup::new();
    let first = commands::copy::run(
        &s.src,
        None,
        &s.dst,
        None,
        &CopyRequest {
            snapshots: &["snap-1".to_string()],
            source_labels: &[],
            compression: Compression::Lz4,
            dry_run: false,
        },
        None,
        None,
    )
    .unwrap();
    assert_eq!(first.copied.len(), 1);

    let second = copy_all(&s.src, &s.dst);
    let names: Vec<_> = second.copied.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["snap-2"]);
    assert_eq!(second.already_present, ["snap-1"]);
    // snap-2 shares most of its data with snap-1, already in the destination.
    let stats = &second.copied[0].stats;
    assert!(
        stats.deduplicated_size < stats.original_size / 4,
        "{} of {} bytes newly stored",
        stats.deduplicated_size,
        stats.original_size
    );

    let third = copy_all(&s.src, &s.dst);
    assert!(third.copied.is_empty());
    assert_eq!(third.already_present.len(), 2);
}

#[test]
fn copy_dry_run_writes_nothing() {
    let s = Setup::new();
    let outcome = commands::copy::run(
        &s.src,
        None,
        &s.dst,
        None,
        &CopyRequest {
            snapshots: &[],
            source_labels: &[],
            compression: Compression::Lz4,
            dry_run: true,
        },
        None,
        None,
    )
    .unwrap();
    assert_eq!(outcome.copied.len(), 2);
    let repo = open_local_repo(&s.path("dst-repo"));
    assert!(repo.manifest().snapshots.is_empty());
    assert_eq!(repo.chunk_index().len(), 0);
}

#[test]
fn copy_filters_by_source_label() {
    let s = Setup::new();
    let outcome = commands::copy::run(
        &s.src,
        None,
        &s.dst,
        None,
        &CopyRequest {
            snapshots: &[],
            source_labels: &["other".to_string()],
            compression: Compression::Lz4,
            dry_run: false,
        },
        None,
        None,
    )
    .unwrap();
    assert!(outcome.copied.is_empty());
}

#[test]
fn copy_into_same_repository_is_refused() {
    let s = Setup::new();
    let err = commands::copy::run(
        &s.src,
        None,
        &s.src,
        None,
        &CopyRequest {
            snapshots: &[],
            source_labels: &[],
            compression: Compression::Lz4,
            dry_run: false,
        },
        None,
        None,
    )
    .unwrap_err();
    assert!(err.to_string().contains("same repository"), "{err}");
}
