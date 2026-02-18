use colored::Colorize;
use regex::Regex;

pub struct Printer {
    pub use_color: bool,
    pub show_filename: bool,
}

impl Printer {
    pub fn new(use_color: bool, show_filename: bool) -> Self {
        Self { use_color, show_filename }
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

    pub fn print_headers(&self, headers: &[String]) {
        let line = headers.join(",");
        if self.use_color {
            println!("{}", line.dimmed());
        } else {
            println!("{}", line);
        }
    }

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

    pub fn print_count(&self, filename: &str, count: usize) {
        if self.use_color {
            println!(
                "{}: {}",
                filename.cyan(),
                count.to_string().green().bold()
            );
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
