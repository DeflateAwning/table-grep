use crate::cli::OutputFormat;
use colored::Colorize;
use comfy_table::{Attribute, Cell, CellAlignment, Color, ContentArrangement, Table, presets};
use regex::Regex;

pub struct Printer {
    pub use_color: bool,
    pub show_filename: bool,
    pub format: OutputFormat,
}

impl Printer {
    pub fn new(use_color: bool, show_filename: bool, format: OutputFormat) -> Self {
        Self {
            use_color,
            show_filename,
            format,
        }
    }

    pub fn print_file_header(&self, filename: &str) {
        if self.show_filename {
            if self.use_color {
                println!("{}", format!("==> {} <==", filename).cyan().bold());
            } else {
                println!("==> {} <==", filename);
            }
        }
    }

    /// CSV mode: print a dimmed header row.
    pub fn print_headers(&self, headers: &[String]) {
        let line = headers.join(",");
        if self.use_color {
            println!("{}", line.dimmed());
        } else {
            println!("{}", line);
        }
    }

    /// CSV mode: print a single matching row with the row number prefix.
    pub fn print_match(&self, row_num: usize, row: &[String], pattern: &Regex) {
        let highlighted: Vec<String> = row
            .iter()
            .map(|cell| self.highlight_cell(cell, pattern))
            .collect();

        if self.use_color {
            print!("{} ", format!("{}:", row_num).yellow());
        } else {
            print!("{}: ", row_num);
        }
        println!("{}", highlighted.join(","));
    }

    /// Table mode: render all buffered rows (+ optional headers) as a pretty table.
    pub fn print_table(
        &self,
        headers: &[String],
        rows: &[(usize, Vec<String>)], // (row_num, fields)
        pattern: &Regex,
        with_headers: bool,
    ) {
        if rows.is_empty() {
            return;
        }

        let mut table = Table::new();
        table
            .load_preset(presets::UTF8_FULL)
            .set_content_arrangement(ContentArrangement::Dynamic);

        // Header row
        if with_headers {
            // Prepend a "#" column for the row number
            let mut header_cells: Vec<Cell> = vec![
                Cell::new("#")
                    .add_attribute(Attribute::Bold)
                    .set_alignment(CellAlignment::Right)
                    .fg(if self.use_color {
                        Color::DarkCyan
                    } else {
                        Color::Reset
                    }),
            ];
            for h in headers {
                header_cells.push(Cell::new(h).add_attribute(Attribute::Bold).fg(
                    if self.use_color {
                        Color::DarkCyan
                    } else {
                        Color::Reset
                    },
                ));
            }
            table.set_header(header_cells);
        }

        for (row_num, row) in rows {
            let mut cells: Vec<Cell> = vec![
                Cell::new(row_num.to_string())
                    .set_alignment(CellAlignment::Right)
                    .fg(if self.use_color {
                        Color::Yellow
                    } else {
                        Color::Reset
                    }),
            ];
            for cell_str in row {
                let cell = if self.use_color && pattern.is_match(cell_str) {
                    // Mark matching cells in the table
                    Cell::new(cell_str)
                        .add_attribute(Attribute::Bold)
                        .fg(Color::Red)
                } else {
                    Cell::new(cell_str)
                };
                cells.push(cell);
            }
            table.add_row(cells);
        }

        println!("{table}");
    }

    pub fn print_count(&self, filename: &str, count: usize) {
        if self.use_color {
            println!("{}: {}", filename.cyan(), count.to_string().green().bold());
        } else {
            println!("{}: {}", filename, count);
        }
    }

    fn highlight_cell(&self, cell: &str, pattern: &Regex) -> String {
        if !self.use_color {
            return cell.to_string();
        }
        let result = pattern.replace_all(cell, |caps: &regex::Captures| {
            caps[0].red().bold().to_string()
        });
        result.into_owned()
    }

    pub fn print_separator(&self) {
        if self.use_color {
            println!("{}", "---".dimmed());
        } else {
            println!("---");
        }
    }
}
