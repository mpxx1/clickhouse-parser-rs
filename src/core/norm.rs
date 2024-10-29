use anyhow::Result;
use polars::frame::DataFrame;
use polars::prelude::{ChunkApply, DataType, IntoSeries};

pub trait TNormalize {
    fn normalize(self, cols: &[&str]) -> Result<Self>
    where
        Self: Sized;
}


impl TNormalize for DataFrame {
    fn normalize(mut self, cols: &[&str]) -> Result<Self> {
        let mut df = self.clone();

        for col in cols {
            df
                .try_apply(
                    col,
                    |c| c.cast(&DataType::Float64)
                )?;
        }

        let columns = df.columns(cols)?;
        for column in columns {

            if let Ok(sth) = column.f64() {
                let min_val: f64 = column.min()?.unwrap();
                let max_val: f64 = column.max()?.unwrap();

                let out = sth.apply_values(
                    |opt_v| (opt_v - min_val) / (max_val - min_val)
                ).into_series();

                self.replace(column.name(), out)?;
            }
        }

        Ok(self)
    }
}
