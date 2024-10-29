# [Domclick](https://domclick.ru) html pages parser

### Written in Rust

#### For educational purposes only!


Website was blocking python scripts for parsing it source code, so I decide to download 
pages with undetected chromedriver and then parse it in rust. 
This project uses `scraper` crate to get values from html code, `polars` to create and maintain 
data-table in runtime and `rayon` to scan html files in multi thread.

It first gets raw data, then fills in the gaps, does one hot encoding for categorical columns and normalizes numeric columns. 

It saves a result into `out.tsv` table. 

You also can get a raw data table in `short.tsv` running project with `-s` flag.


It has scanned 2000 html web pages (total 1 Gb) and built a table in 1.5 seconds