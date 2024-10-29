use anyhow::Result;
use ml::full::util::crate_table;
use polars::prelude::{CsvWriter, SerWriter};
use ml::core::push_cols::TPushCols;
use ml::preview::util::create;

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();

    let mut df = crate_table()?;
    let mut name = "out.tsv";
    if args.contains(&"-s".to_string()) {
        name = "short.tsv";
        let all = create()?;
        df = all.0
            .push_cols("flat_id", all.1)?;
    }

    let mut file = std::fs::File::create(name)?;
    CsvWriter::new(&mut file)
        .include_header(true)
        .with_separator(b'\t')
        .finish(&mut df)?;

    Ok(())
}
