use crate::core::fill::TFill;
use crate::core::one_hot::TOneHot;
use crate::core::push_cols::TPushCols;
use crate::preview::util::create;
use anyhow::Result;
use polars::frame::DataFrame;
use crate::core::norm::TNormalize;

pub fn crate_table() -> Result<DataFrame> {
    let all = create()?;
    let mut df = all.0
        .fill_rent()?
        .fill_living_area()?
        .fill_kitchen_area()?
        .fill_prepayment()?
        .fill_balconies()?
        .fill_floors()?
        .fill_deposit()?
        .fill_building_year()?
        .fill_ceiling_height()?
        .fill_walls()?
        .fill_ceiling_type()?
        .fill_view_from_windows()?
        .one_hot("refit")?
        .one_hot("view_from_windows")?
        .one_hot("utility_bills")?
        .one_hot("walls")?
        .one_hot("ceiling_type")?
        .push_cols("flat_id", all.1)?
        .normalize(&[
            "rooms", "total_area", "living_area",
            "kitchen_area", "balconies", "floor",
            "floors", "deposit", "ceiling_height",
            "building_year", "prepayment",
        ])?;

    df = df.drop("flat_id")?;

    Ok(df)
}


// #[warn(dead_code)]
// fn check_no_nulls(df: &DataFrame) -> Result<()> {
//     for col in df.get_column_names() {
//         let null_count = df.column(col)?.null_count();
//         if null_count > 0 {
//             return Err(anyhow!("{col}"));
//         }
//     }
//     Ok(())
// }