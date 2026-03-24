# table-grep

A `grep`-like tool for searching through **CSV** and **Parquet** table files. Find rows matching a pattern across individual files or entire directory trees.

## Installation

```bash
cargo install table-grep
```

Or, you can build the project from source.

## Usage

```
table-grep [OPTIONS] <PATTERN> <PATH>
```

`PATH` can be a **single file** (`.csv` or `.parquet`/`.pq`/`.parq`) or a **directory** (searched recursively).

## Examples

```bash
# Search for "Alice" in a CSV file
table-grep Alice data.csv

# Search across all table files in a directory
table-grep "error|warn" ./logs/

# Case-insensitive search
table-grep -i alice users.parquet

# Search only in specific columns
table-grep --columns name,email "gmail" users.csv

# Count matching rows per file (don't print them)
table-grep -c "2024" ./reports/

# Invert match: show rows that do NOT contain "active"
table-grep -v "active" accounts.csv

# Treat pattern as literal string (no regex)
table-grep -F "price(usd)" products.csv

# Limit to first 10 matches per file
table-grep -m 10 "California" customers.parquet

# Show only matching cell values (not full rows)
table-grep -o "^[A-Z]{2}$" states.csv

# Disable color output (useful for piping)
table-grep --no-color "foo" data.csv | sort

# Suppress file headers when searching a directory
table-grep --no-filename "error" ./logs/
```

## Options

| Flag | Long | Description |
|------|------|-------------|
| `-i` | `--ignore-case` | Case-insensitive matching |
| `-v` | `--invert` | Show rows that do NOT match |
| `-c` | `--count` | Print match count per file instead of rows |
| `-o` | `--only-matching` | Show only the matching column values |
| `-F` | `--fixed-strings` | Treat pattern as literal, not regex |
| `-m N` | `--max-count N` | Stop after N matches per file |
|        | `--no-filename` | Suppress filename headers |
| `-H` | `--with-headers` | Show column headers above results (default: true) |
| | `--columns col1,col2` | Only search in these columns |
| | `--no-color` | Disable colored output |

## Supported Formats

| Format  | Extension  | Notes |
|---------|------------|-------|
| CSV     | `.csv`     | Auto-detects headers; handles flexible/malformed CSVs |
| Parquet | `.parquet`, `.pq`, `.parq` | Supports all Arrow scalar types; batch-streamed for memory efficiency |

## Output Formats

### CSV Output Format

```
> table-grep Alice test_data/

==> test_data/test_file_1.csv <==
name,age,position
3: Alice,30,Engineer
5: Alice,28,Designer
---
```

### Table Output Format (`--format table`)

```
> table-grep -f table Alice test_data/

==> test_data/test_file_1.csv <==
┌───┬───────┬─────┬──────────┐
│ # ┆ name  ┆ age ┆ position │
╞═══╪═══════╪═════╪══════════╡
│ 3 ┆ Alice ┆ 30  ┆ Engineer │
├╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┤
│ 5 ┆ Alice ┆ 28  ┆ Designer │
└───┴───────┴─────┴──────────┘
```

## Inspiration

* Similar Project: https://github.com/hyparam/parquet-grep
