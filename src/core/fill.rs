use anyhow::Result;
use polars::frame::DataFrame;
use polars::prelude::{col, lit, ChunkFillNullValue, DataFrameJoinOps, FillNullStrategy, Float64Chunked, IntoLazy, IntoSeries, PlSmallStr, SeriesMethods, StringChunked, UInt32Chunked};

static ATT1: f64 = 0.565;
static ATT2: f64 = 0.23;

trait TF64Beau {
    fn comp(self, precision: i32) -> f64;
}

pub trait TFill {
    fn fill_living_area(self) -> Result<Self>
    where
        Self: Sized;
    fn fill_kitchen_area(self) -> Result<Self>
    where
        Self: Sized;
    fn fill_prepayment(self) -> Result<Self>
    where
        Self: Sized;

    fn fill_balconies(self) -> Result<Self>
    where
        Self: Sized;

    fn fill_floors(self) -> Result<Self>
    where
        Self: Sized;

    fn fill_deposit(self) -> Result<Self>
    where
        Self: Sized;

    fn fill_ceiling_height(self) -> Result<Self>
    where
        Self: Sized;

    fn fill_building_year(self) -> Result<Self>
    where
        Self: Sized;

    fn fill_view_from_windows(self) -> Result<Self>
    where
        Self: Sized;

    fn fill_walls(self) -> Result<Self>
    where
        Self: Sized;

    fn fill_ceiling_type(self) -> Result<Self>
    where
        Self: Sized;

    fn fill_rent(self) -> Result<Self>
    where
        Self: Sized;
}

impl TFill for DataFrame {
    fn fill_living_area(mut self) -> Result<Self> {
        fill_area(&mut self, "total_area", "kitchen_area", ATT1)?;

        Ok(self)
    }

    fn fill_kitchen_area(mut self) -> Result<Self> {
        fill_area(&mut self, "total_area", "living_area", ATT2)?;

        Ok(self)
    }

    fn fill_prepayment(mut self) -> Result<Self> {
        let prepayment_series = self
            .column("prepayment")?
            .str()?
            .iter()
            .map(|x| match x {
                Some(value) if value == "нет" => Some("0"),
                None => Some("0"),
                x => x,
            })
            .collect::<StringChunked>()
            .into_series();
        let rent_iter = self.column("rent")?.u32()?.iter();

        let new_prepayment = prepayment_series
            .str()?
            .iter()
            .zip(rent_iter)
            .map(|(prepayment, rent)| match prepayment {
                Some(x) if x.parse::<u32>().ok() == rent => Some(1),
                x => x.unwrap().parse::<u32>().ok(),
            })
            .collect::<UInt32Chunked>()
            .into_series();

        self.replace("prepayment", new_prepayment)?;

        Ok(self)
    }

    fn fill_balconies(mut self) -> Result<Self> {
        fill_zero(&mut self, "balconies")?;

        Ok(self)
    }

    fn fill_floors(mut self) -> Result<Self>
    where
        Self: Sized,
    {
        fill_mean(&mut self, "floors")?;

        Ok(self)
    }

    fn fill_deposit(mut self) -> Result<Self>
    where
        Self: Sized,
    {
        fill_zero(&mut self, "deposit")?;
        Ok(self)
    }

    fn fill_ceiling_height(mut self) -> Result<Self>
    where
        Self: Sized,
    {
        // Рассчитываем среднее значение по всему столбцу 'ceiling height' игнорируя нули и `None`
        let overall_mean = self.column("ceiling_height")?.mean().unwrap();

        // Группировка по 'building year' и расчет среднего по 'ceiling height' в каждой группе
        let mut grouped_means = self
            .select(["ceiling_height", "building_year"])?
            .lazy()
            .group_by_stable([col("building_year")])
            .agg([col("ceiling_height").mean().alias("mean")])
            .collect()?;

        let upd_means = grouped_means
            .column("mean")?
            .f64()?
            .fill_null_with_values(overall_mean)?
            .into_series();

        grouped_means.replace("mean", upd_means)?;

        // Присоединяем средние значения для каждой группы к основному DataFrame
        self = self.left_join(&grouped_means, ["building_year"], ["building_year"])?;

        // Обновляем нулевые и None значения в 'ceiling height'
        let updated_ceiling_heights = self
            .column("ceiling_height")?
            .f64()?
            .iter()
            .zip(self.column("mean")?.f64()?.iter())
            .map(|(a, b)| match a {
                None => Some(b?.comp(2)),
                Some(x) => Some(x),
            })
            .collect::<Float64Chunked>()
            .into_series();

        // Заменяем старый столбец новым
        self.replace("ceiling_height", updated_ceiling_heights)?;
        self = self.drop("mean")?;

        Ok(self)
    }

    fn fill_building_year(mut self) -> Result<Self>
    where
        Self: Sized,
    {
        fill_mean(&mut self, "building_year")?;
        Ok(self)
    }

    fn fill_view_from_windows(self) -> Result<Self>
    where
        Self: Sized,
    {
        let df = self
            .lazy()
            .with_column(col("view_from_windows").fill_null(lit("street")))
            .collect()?;

        Ok(df)
    }

    fn fill_walls(mut self) -> Result<Self>
    where
        Self: Sized,
    {
        fill_str_with_avg(&mut self, "walls", "building_year")?;

        Ok(self)
    }

    fn fill_ceiling_type(mut self) -> Result<Self>
    where
        Self: Sized,
    {
        fill_str_with_avg(&mut self, "ceiling_type", "building_year")?;

        Ok(self)
    }

    fn fill_rent(mut self) -> Result<Self>
    where
        Self: Sized
    {
        let col = self
            .column("rent")?
            .u32()?
            .iter()
            .map(|x| match x.unwrap() {
                x if x < 15_000 => Some(0),
                x if x >= 15_000 && x < 30_000 => Some(1),
                x if x >= 30_000 && x < 50_000 => Some(2),
                x if x >= 50_000 && x < 80_000 => Some(3),
                x if x >= 80_000 && x < 100_000 => Some(4),
                x if x >= 100_000 && x < 250_000 => Some(5),
                x if x >= 250_000 && x < 500_000 => Some(6),
                x if x >= 500_000    => Some(7),
                x => Some(x),
            })
            .collect::<UInt32Chunked>()
            .into_series();

        self.replace("rent", col)?;

        Ok(self)
    }
}

impl TF64Beau for f64 {
    fn comp(self, precision: i32) -> f64 {
        let multiplier = 10_f64.powi(precision);
        (self * multiplier).round() / multiplier
    }
}

fn fill_area(df: &mut DataFrame, first: &str, second: &str, attitude: f64) -> Result<()> {
    let total_area_series = df.column(first)?.f64()?.iter();
    let target_area_series = df.column(second)?.f64()?.iter();

    let filled_living_area = target_area_series
        .zip(total_area_series)
        .map(|(target, total)| match target {
            Some(value) => Some(value),
            None => total.map(|v| (v * attitude).comp(1)),
        })
        .collect::<Float64Chunked>()
        .into_series();

    df.replace(second, filled_living_area)?;

    Ok(())
}

fn fill_zero(df: &mut DataFrame, col: &str) -> Result<()> {
    let series = df.column(col)?.fill_null(FillNullStrategy::Zero)?;
    df.replace(col, series)?;

    Ok(())
}

fn fill_mean(df: &mut DataFrame, col: &str) -> Result<()> {
    let series = df.column(col)?.fill_null(FillNullStrategy::Mean)?;
    df.replace(col, series)?;

    Ok(())
}

fn fill_str_with_avg(df: &mut DataFrame, col_tgt: &str, col_avg: &str) -> Result<()> {
    let vc = df
        .column(col_tgt)?
        .value_counts(true, true, PlSmallStr::from("__counts"), false)?;

    let mode = vc.column(col_tgt)?.get(0)?;

    let grouped_modes = df
        .select([col_tgt, col_avg])?
        .lazy()
        .group_by_stable([col(col_avg)])
        .agg([col(col_tgt).mode().get(0).alias("__mode")])
        .collect()?;

    // println!("{grouped_modes}");

    let upd_modes = grouped_modes
        .lazy()
        .with_column(col("__mode").fill_null(lit(mode.get_str().unwrap())))
        .collect()?;

    *df = df.left_join(&upd_modes, [col_avg], [col_avg])?;

    let out_series = df
        .column(col_tgt)?
        .str()?
        .iter()
        .zip(df.column("__mode")?.str()?.iter())
        .map(|(a, b)| match a {
            None => b,
            Some(x) => Some(x),
        })
        .collect::<StringChunked>()
        .into_series();

    df.replace(col_tgt, out_series)?;
    *df = df.drop("__mode")?;

    Ok(())
}
