use crate::cli::Cli;
use crate::output::Printer;
use anyhow::Result;
use regex::Regex;
use std::path::Path;

pub fn search_file(path: &Path, pattern: &Regex, cli: &Cli) -> Result<()> {
    let filename = path.display().to_string();
    let use_color = !cli.no_color && atty::is(atty::Stream::Stdout);
    let show_filename = !cli.no_filename;

    let printer = Printer::new(use_color, show_filename);

    match path.extension().and_then(|e| e.to_str()) {
        Some("csv") => search_csv(path, &filename, pattern, cli, &printer),
        Some("parquet") => search_parquet(path, &filename, pattern, cli, &printer),
        _ => Ok(()), // already filtered by main
    }
}

// ── CSV ──────────────────────────────────────────────────────────────────────

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

    // Collect headers
    let headers: Vec<String> = rdr.headers()?.iter().map(|h| h.to_string()).collect();

    // Resolve column filter indices
    let col_indices = resolve_column_indices(&headers, &cli.columns);

    let mut match_count = 0usize;
    let mut row_num = 0usize;
    let mut printed_file_header = false;

    for result in rdr.records() {
        let record =
            result.map_err(|e| anyhow::anyhow!("CSV parse error in '{}': {}", filename, e))?;
        row_num += 1;

        let row: Vec<String> = record.iter().map(|f| f.to_string()).collect();
        let matched = row_matches(&row, pattern, &col_indices, cli.invert);

        if matched {
            if !printed_file_header {
                printer.print_file_header(filename);
                if cli.with_headers && !cli.count {
                    printer.print_headers(&headers);
                }
                printed_file_header = true;
            }

            match_count += 1;

            if !cli.count {
                if cli.only_matching {
                    print_only_matching(&row, &headers, pattern, &col_indices);
                } else {
                    printer.print_match(row_num, &row, pattern);
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
    }

    if printed_file_header && !cli.count {
        printer.print_separator();
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

    // Resolve column filter indices
    let col_indices = resolve_column_indices(&headers, &cli.columns);

    let reader = builder
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to build Parquet reader for '{}': {}", filename, e))?;

    let mut match_count = 0usize;
    let mut global_row_num = 0usize;
    let mut printed_file_header = false;

    'outer: for batch_result in reader {
        let batch = batch_result
            .map_err(|e| anyhow::anyhow!("Parquet batch error in '{}': {}", filename, e))?;

        let num_rows = batch.num_rows();

        for row_idx in 0..num_rows {
            global_row_num += 1;

            // Convert this row to Vec<String>
            let row: Vec<String> = batch
                .columns()
                .iter()
                .map(|col| array_value_to_string(col.as_ref(), row_idx))
                .collect();

            let matched = row_matches(&row, pattern, &col_indices, cli.invert);

            if matched {
                if !printed_file_header {
                    printer.print_file_header(filename);
                    if cli.with_headers && !cli.count {
                        printer.print_headers(&headers);
                    }
                    printed_file_header = true;
                }

                match_count += 1;

                if !cli.count {
                    if cli.only_matching {
                        print_only_matching(&row, &headers, pattern, &col_indices);
                    } else {
                        printer.print_match(global_row_num, &row, pattern);
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
    }

    if printed_file_header && !cli.count {
        printer.print_separator();
    }

    Ok(())
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Determine which column indices to check. None means all columns.
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

/// Returns true if the row matches (or doesn't match when inverted).
fn row_matches(
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

/// Print only the matching cells with their column names.
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

/// Convert any Arrow array element to a string representation.
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
        DataType::Timestamp(_, _) => {
            // Generic fallback via cast to string
            format!("{:?}", array.as_any().type_id())
        }
        dt => format!("<{}>", dt),
    }
}
