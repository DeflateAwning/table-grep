use crate::cli::{Cli, OutputFormat};
use crate::output::Printer;
use anyhow::Result;
use regex::Regex;
use std::path::Path;

pub fn search_file(path: &Path, pattern: &Regex, cli: &Cli) -> Result<()> {
    let filename = path.display().to_string();
    let use_color = !cli.no_color && atty::is(atty::Stream::Stdout);
    let show_filename = !cli.no_filename;

    let printer = Printer::new(use_color, show_filename, cli.format);

    match path.extension().and_then(|e| e.to_str()) {
        Some("csv") => search_csv(path, &filename, pattern, cli, &printer),
        Some("parquet") => search_parquet(path, &filename, pattern, cli, &printer),
        _ => Ok(()),
    }
}

// ── shared output logic ───────────────────────────────────────────────────────

/// Emit the collected matching rows in whichever format the user chose.
fn emit_matches(
    filename: &str,
    headers: &[String],
    matches: &[(usize, Vec<String>)],
    pattern: &Regex,
    cli: &Cli,
    printer: &Printer,
) {
    if matches.is_empty() {
        return;
    }

    printer.print_file_header(filename);

    match printer.format {
        OutputFormat::Csv => {
            if cli.with_headers {
                printer.print_headers(headers);
            }
            for (row_num, row) in matches {
                printer.print_match(*row_num, row, pattern);
            }
            printer.print_separator();
        }
        OutputFormat::Table => {
            // print_table handles its own header row
            printer.print_table(headers, matches, pattern, cli.with_headers);
        }
    }
}

// ── CSV ───────────────────────────────────────────────────────────────────────

fn search_csv(
    path: &Path,
    filename: &str,
    pattern: &Regex,
    cli: &Cli,
    printer: &Printer,
) -> Result<()> {
    let mut rdr = csv::ReaderBuilder::new()
        .flexible(true)
        .from_path(path)
        .map_err(|e| anyhow::anyhow!("Failed to open CSV '{}': {}", filename, e))?;

    let headers: Vec<String> = rdr.headers()?.iter().map(|h| h.to_string()).collect();

    let col_indices = resolve_column_indices(&headers, &cli.columns);

    let mut match_count = 0usize;
    let mut row_num = 0usize;
    let mut matched_rows: Vec<(usize, Vec<String>)> = Vec::new();

    for result in rdr.records() {
        let record =
            result.map_err(|e| anyhow::anyhow!("CSV parse error in '{}': {}", filename, e))?;
        row_num += 1;

        let row: Vec<String> = record.iter().map(|f| f.to_string()).collect();

        if row_matches(&row, pattern, &col_indices, cli.invert) {
            match_count += 1;

            if !cli.count {
                if cli.only_matching {
                    // only_matching bypasses the buffering path
                    if matched_rows.is_empty() {
                        printer.print_file_header(filename);
                    }
                    print_only_matching(&row, &headers, pattern, &col_indices);
                } else {
                    matched_rows.push((row_num, row));
                }
            }

            if let Some(max) = cli.max_count {
                if match_count >= max {
                    break;
                }
            }
        }
    }

    if cli.count && match_count > 0 {
        printer.print_count(filename, match_count);
    } else if !cli.only_matching {
        emit_matches(filename, &headers, &matched_rows, pattern, cli, printer);
    }

    Ok(())
}

// ── Parquet ───────────────────────────────────────────────────────────────────

fn search_parquet(
    path: &Path,
    filename: &str,
    pattern: &Regex,
    cli: &Cli,
    printer: &Printer,
) -> Result<()> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs::File;

    let file = File::open(path)
        .map_err(|e| anyhow::anyhow!("Failed to open Parquet '{}': {}", filename, e))?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| anyhow::anyhow!("Failed to read Parquet '{}': {}", filename, e))?;

    let schema = builder.schema().clone();
    let headers: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();

    let col_indices = resolve_column_indices(&headers, &cli.columns);

    let reader = builder
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to build Parquet reader for '{}': {}", filename, e))?;

    let mut match_count = 0usize;
    let mut global_row_num = 0usize;
    let mut matched_rows: Vec<(usize, Vec<String>)> = Vec::new();

    'outer: for batch_result in reader {
        let batch = batch_result
            .map_err(|e| anyhow::anyhow!("Parquet batch error in '{}': {}", filename, e))?;

        for row_idx in 0..batch.num_rows() {
            global_row_num += 1;

            let row: Vec<String> = batch
                .columns()
                .iter()
                .map(|col| array_value_to_string(col.as_ref(), row_idx))
                .collect();

            if row_matches(&row, pattern, &col_indices, cli.invert) {
                match_count += 1;

                if !cli.count {
                    if cli.only_matching {
                        if matched_rows.is_empty() {
                            printer.print_file_header(filename);
                        }
                        print_only_matching(&row, &headers, pattern, &col_indices);
                    } else {
                        matched_rows.push((global_row_num, row));
                    }
                }

                if let Some(max) = cli.max_count {
                    if match_count >= max {
                        break 'outer;
                    }
                }
            }
        }
    }

    if cli.count && match_count > 0 {
        printer.print_count(filename, match_count);
    } else if !cli.only_matching {
        emit_matches(filename, &headers, &matched_rows, pattern, cli, printer);
    }

    Ok(())
}

// ── helpers ───────────────────────────────────────────────────────────────────

fn resolve_column_indices(headers: &[String], filter: &Option<Vec<String>>) -> Option<Vec<usize>> {
    filter.as_ref().map(|cols| {
        cols.iter()
            .filter_map(|col_name| {
                let idx = headers.iter().position(|h| h == col_name);
                if idx.is_none() {
                    eprintln!("Warning: column '{}' not found", col_name);
                }
                idx
            })
            .collect()
    })
}

pub fn row_matches(
    row: &[String],
    pattern: &Regex,
    col_indices: &Option<Vec<usize>>,
    invert: bool,
) -> bool {
    let mut cells_to_check: Box<dyn Iterator<Item = &String>> = match col_indices {
        Some(indices) => Box::new(indices.iter().filter_map(|&i| row.get(i))),
        None => Box::new(row.iter()),
    };

    let any_match = cells_to_check.any(|cell| pattern.is_match(cell));
    if invert { !any_match } else { any_match }
}

fn print_only_matching(
    row: &[String],
    headers: &[String],
    pattern: &Regex,
    col_indices: &Option<Vec<usize>>,
) {
    let indices_to_check: Vec<usize> = match col_indices {
        Some(indices) => indices.clone(),
        None => (0..row.len()).collect(),
    };

    for idx in indices_to_check {
        if let Some(cell) = row.get(idx) {
            if pattern.is_match(cell) {
                let col_name = headers.get(idx).map(|s| s.as_str()).unwrap_or("?");
                println!("  [{}] {}", col_name, cell);
            }
        }
    }
}

fn array_value_to_string(array: &dyn arrow::array::Array, index: usize) -> String {
    use arrow::array::*;
    use arrow::datatypes::DataType;

    if array.is_null(index) {
        return String::from("NULL");
    }

    match array.data_type() {
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|a| a.value(index).to_string())
            .unwrap_or_default(),
        DataType::LargeUtf8 => array
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .map(|a| a.value(index).to_string())
            .unwrap_or_default(),
        DataType::Int8 => array
            .as_any()
            .downcast_ref::<Int8Array>()
            .map(|a| a.value(index).to_string())
            .unwrap_or_default(),
        DataType::Int16 => array
            .as_any()
            .downcast_ref::<Int16Array>()
            .map(|a| a.value(index).to_string())
            .unwrap_or_default(),
        DataType::Int32 => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|a| a.value(index).to_string())
            .unwrap_or_default(),
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| a.value(index).to_string())
            .unwrap_or_default(),
        DataType::UInt8 => array
            .as_any()
            .downcast_ref::<UInt8Array>()
            .map(|a| a.value(index).to_string())
            .unwrap_or_default(),
        DataType::UInt16 => array
            .as_any()
            .downcast_ref::<UInt16Array>()
            .map(|a| a.value(index).to_string())
            .unwrap_or_default(),
        DataType::UInt32 => array
            .as_any()
            .downcast_ref::<UInt32Array>()
            .map(|a| a.value(index).to_string())
            .unwrap_or_default(),
        DataType::UInt64 => array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(|a| a.value(index).to_string())
            .unwrap_or_default(),
        DataType::Float32 => array
            .as_any()
            .downcast_ref::<Float32Array>()
            .map(|a| a.value(index).to_string())
            .unwrap_or_default(),
        DataType::Float64 => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|a| a.value(index).to_string())
            .unwrap_or_default(),
        DataType::Boolean => array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|a| a.value(index).to_string())
            .unwrap_or_default(),
        DataType::Date32 => array
            .as_any()
            .downcast_ref::<Date32Array>()
            .map(|a| {
                a.value_as_date(index)
                    .map(|d| d.to_string())
                    .unwrap_or_else(|| a.value(index).to_string())
            })
            .unwrap_or_default(),
        DataType::Date64 => array
            .as_any()
            .downcast_ref::<Date64Array>()
            .map(|a| {
                a.value_as_datetime(index)
                    .map(|d| d.to_string())
                    .unwrap_or_else(|| a.value(index).to_string())
            })
            .unwrap_or_default(),
        dt => format!("<{}>", dt),
    }
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn re(pattern: &str) -> Regex {
        Regex::new(pattern).unwrap()
    }

    #[test]
    fn test_row_matches_basic() {
        let row = vec![
            "Alice".to_string(),
            "30".to_string(),
            "Engineer".to_string(),
        ];

        // Positive match — pattern present in row
        assert!(row_matches(&row, &re("Alice"), &None, false));

        // No match — pattern absent
        assert!(!row_matches(&row, &re("Bob"), &None, false));

        // Inverted: absent pattern → true
        assert!(row_matches(&row, &re("Bob"), &None, true));

        // Inverted: present pattern → false
        assert!(!row_matches(&row, &re("Alice"), &None, true));

        // Column filter: match only in column 1 (age), pattern matches
        assert!(row_matches(&row, &re("30"), &Some(vec![1]), false));

        // Column filter: restrict to column 0 (name), numeric pattern should not match
        assert!(!row_matches(&row, &re("30"), &Some(vec![0]), false));

        // Regex: match on partial word
        assert!(row_matches(&row, &re("Eng.*"), &None, false));

        // Case sensitivity: lowercase should not match "Alice" by default
        assert!(!row_matches(&row, &re("alice"), &None, false));

        // Case insensitive via regex flag
        assert!(row_matches(&row, &re("(?i)alice"), &None, false));
    }
}
