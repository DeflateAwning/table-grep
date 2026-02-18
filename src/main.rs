mod cli;
mod grep;
mod output;

use anyhow::Result;
use clap::Parser;
use cli::Cli;
use std::path::Path;
use walkdir::WalkDir;

fn main() -> Result<()> {
    let cli = Cli::parse();
    let pattern = cli.build_regex()?;

    let path = Path::new(&cli.path);

    if path.is_file() {
        grep::search_file(path, &pattern, &cli)?;
    } else if path.is_dir() {
        let mut found_any = false;
        for entry in WalkDir::new(path)
            .follow_links(true)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_file())
        {
            let file_path = entry.path();
            if is_supported(file_path) {
                found_any = true;
                grep::search_file(file_path, &pattern, &cli)?;
            }
        }
        if !found_any {
            eprintln!(
                "No supported table files (.csv, .parquet, .pq, .parq) found in '{}'",
                cli.path
            );
        }
    } else {
        anyhow::bail!("'{}' is not a valid file or directory", cli.path);
    }

    Ok(())
}

/// Check if a file path is a supported file type, based on its extension.
fn is_supported(path: &Path) -> bool {
    // TODO: Could detect the file header, especially for parquet files.
    match path.extension().and_then(|e| e.to_str()) {
        Some("csv") | Some("parquet") | Some("pq") | Some("parq") => true,
        _ => false,
    }
}
