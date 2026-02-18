use anyhow::Result;
use clap::Parser;
use regex::{Regex, RegexBuilder};

/// table-grep: grep through CSV and Parquet table files
#[derive(Parser, Debug)]
#[command(
    name = "table-grep",
    version,
    about = "Search for patterns in CSV and Parquet table files",
    long_about = "table-grep lets you search for patterns across rows in CSV and Parquet files,\n\
                  either in a single file or recursively across an entire directory."
)]
pub struct Cli {
    /// Pattern to search for (supports regex)
    pub pattern: String,

    /// File or directory to search
    pub path: String,

    /// Search only in specific columns (comma-separated column names)
    #[arg(long, value_delimiter = ',', value_name = "COLUMNS")]
    pub columns: Option<Vec<String>>,

    /// Case-insensitive matching
    #[arg(short = 'i', long)]
    pub ignore_case: bool,

    /// Invert match: show rows that do NOT match
    #[arg(short = 'v', long)]
    pub invert: bool,

    /// Show only matching column values (not full rows)
    #[arg(short = 'o', long)]
    pub only_matching: bool,

    /// Suppress filename headers in output
    #[arg(long = "no-filename")]
    pub no_filename: bool,

    /// Print column headers in output
    #[arg(short = 'H', long = "with-headers", default_value_t = true, action = clap::ArgAction::Set)]
    pub with_headers: bool,

    /// Count matching rows per file instead of printing them
    #[arg(short = 'c', long)]
    pub count: bool,

    /// Treat pattern as a literal string (not regex)
    #[arg(short = 'F', long)]
    pub fixed_strings: bool,

    /// Limit output to N matching rows per file
    #[arg(short = 'm', long, value_name = "N")]
    pub max_count: Option<usize>,

    /// Disable color output
    #[arg(long)]
    pub no_color: bool,
}

impl Cli {
    pub fn build_regex(&self) -> Result<Regex> {
        let pattern = if self.fixed_strings {
            regex::escape(&self.pattern)
        } else {
            self.pattern.clone()
        };

        let re = RegexBuilder::new(&pattern)
            .case_insensitive(self.ignore_case)
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid regex pattern '{}': {}", self.pattern, e))?;

        Ok(re)
    }
}
