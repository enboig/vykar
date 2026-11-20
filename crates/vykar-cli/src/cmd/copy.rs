use std::sync::atomic::AtomicBool;

use vykar_core::commands::copy::{self, CopyProgressEvent, CopyRequest};
use vykar_core::compress::Compression;
use vykar_core::config::{CompressionAlgorithm, ResolvedRepo};

use crate::error::{CliError, CliResult};
use crate::format::{format_bytes, print_backup_stats};
use crate::passphrase::get_passphrase;

pub(crate) struct CopyRunOpts<'a> {
    pub snapshots: &'a [String],
    pub source_filter: &'a [String],
    pub compression_override: Option<&'a str>,
    pub dry_run: bool,
    pub shutdown: Option<&'a AtomicBool>,
}

pub(crate) fn run_copy(
    src: &ResolvedRepo,
    dst: &ResolvedRepo,
    opts: CopyRunOpts<'_>,
) -> CliResult<()> {
    let compression = match opts.compression_override {
        None => Compression::from_algorithm(
            dst.config.compression.algorithm,
            dst.config.compression.zstd_level,
        ),
        Some(algo) => {
            let algorithm = match algo {
                "none" => CompressionAlgorithm::None,
                "lz4" => CompressionAlgorithm::Lz4,
                "zstd" => CompressionAlgorithm::Zstd,
                _ => return Err(CliError::from(format!("unsupported compression: {algo}"))),
            };
            Compression::from_algorithm(algorithm, dst.config.compression.zstd_level)
        }
    };

    let src_pass = get_passphrase(&src.config, src.label.as_deref())?;
    let dst_pass = get_passphrase(&dst.config, dst.label.as_deref())?;

    eprintln!(
        "Copying from '{}' to '{}'",
        src.label_or_url(),
        dst.label_or_url()
    );
    let mut on_event = |event: CopyProgressEvent| match event {
        CopyProgressEvent::SnapshotStarted { name, index, total } => {
            eprintln!("[{index}/{total}] {name}");
        }
        // Warnings are printed with the summary; progress ticks are quiet.
        CopyProgressEvent::Progress { .. }
        | CopyProgressEvent::SnapshotCopied { .. }
        | CopyProgressEvent::Warning { .. } => {}
    };

    let outcome = copy::run(
        &src.config,
        src_pass.as_deref().map(|s| s.as_str()),
        &dst.config,
        dst_pass.as_deref().map(|s| s.as_str()),
        &CopyRequest {
            snapshots: opts.snapshots,
            source_labels: opts.source_filter,
            compression,
            dry_run: opts.dry_run,
        },
        Some(&mut on_event),
        opts.shutdown,
    )?;

    for name in &outcome.already_present {
        eprintln!("Skipped '{name}': already present in destination");
    }
    for copied in &outcome.copied {
        if opts.dry_run {
            // Only the source's numbers are known; what the destination will
            // store depends on what it already holds.
            println!(
                "Dry run: would copy snapshot '{}' ({} files, {})",
                copied.name,
                copied.stats.nfiles,
                format_bytes(copied.stats.original_size)
            );
        } else {
            println!("Copied snapshot '{}'", copied.name);
            print_backup_stats(&copied.stats);
        }
    }
    for w in &outcome.warnings {
        eprintln!("warning: {w}");
    }
    if outcome.copied.is_empty() {
        println!("Nothing to copy");
    } else if !opts.dry_run {
        println!(
            "Copied {} snapshot(s). Run `vykar check --verify-data -R {}` before retiring the source.",
            outcome.copied.len(),
            dst.label_or_url()
        );
    }
    Ok(())
}
