use std::collections::HashMap;
use crate::core::read::TReadHtml;
use anyhow::Result;
use polars::df;
use polars::frame::DataFrame;
use rayon::prelude::*;
use scraper::Html;
use std::fs;
use std::io::Read;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

pub fn create() -> Result<(DataFrame, HashMap<u32, Vec<String>>, HashMap<u32, Vec<(String, String)>>)> {
    let dir_path = PathBuf::from("html_sources");
    let df = DataFrame::new(vec![]).unwrap();

    let facs = Arc::new(Mutex::new(HashMap::<u32, Vec<String>>::new()));
    let metro = Arc::new(Mutex::new(HashMap::<u32, Vec<(String, String)>>::new()));

    let writer = Arc::new(Mutex::new(df));
    let files = fs::read_dir(&dir_path)?
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .collect::<Vec<_>>();

    files.par_iter().for_each(|file_path| {
        let id = file_path.file_name().unwrap().to_str().unwrap().to_string();
        let mut content = String::new();
        let mut file = fs::File::open(&file_path).unwrap();
        file.read_to_string(&mut content).unwrap();

        let dock = Html::parse_document(&content);
        if dock.read_rent().is_none() || dock.read_total_area().is_none() {
            return;
        }
        let id = id.parse::<u32>().unwrap();

        let inner_df = df![
            "flat_id"           => [ id ],
            "rooms"             => [ dock.read_living_rooms().unwrap_or(0) ],
            "studio"            => [ if dock.read_living_rooms().is_none() {1} else {0} ],
            "total_area"        => [ dock.read_total_area() ],
            "living_area"       => [ dock.read_living_area() ],
            "kitchen_area"      => [ dock.read_kitchen_area() ],
            "refit"             => [ dock.read_refit() ],
            "prepayment"        => [ dock.read_prepayment() ],
            "view_from_windows" => [ dock.read_view_from_windows() ],
            "balconies"         => [ dock.read_balconies() ],
            "floor"             => [ dock.read_floor() ],
            "floors"            => [ dock.read_total_floors() ],
            "deposit"           => [ dock.read_deposit() ],
            "utility_bills"     => [ dock.read_utility_bills() ],
            "ceiling_height"    => [ dock.read_ceiling_height() ],
            "gaz"               => [ dock.read_gaz() ],
            "garbage_chute"     => [ dock.read_garbage_chute() ],
            "building_year"     => [ dock.read_building_year() ],
            "walls"             => [ dock.read_walls_material() ],
            "ceiling_type"      => [ dock.read_ceilings_type() ],
            "rent"              => [ dock.read_rent() ],
        ]
        .unwrap();

        let mut locked_df = match writer.lock() {
            Ok(locked) => locked,
            Err(_) => {
                eprintln!("Mutex is poisoned, skipping file: {:?}", file_path);
                return;
            }
        };

        if let Err(e) = locked_df.vstack_mut(&inner_df) {
            eprintln!(
                "Error while stacking DataFrame for file {:?}: {:?}",
                file_path, e
            );
        }

        facs.lock().unwrap().insert(id, dock.read_facilities());
        metro.lock().unwrap().insert(id, dock.read_metro_stations());
    });

    let final_df = Arc::try_unwrap(writer)
        .expect("Arc still has multiple owners")
        .into_inner()
        .expect("Mutex is poisoned");

    let final_facs = Arc::try_unwrap(facs)
        .expect("Arc still has multiple owners")
        .into_inner()
        .unwrap();

    let final_metro = Arc::try_unwrap(metro)
        .expect("Arc still has multiple owners")
        .into_inner()
        .unwrap();


    Ok((final_df, final_facs, final_metro))
}
