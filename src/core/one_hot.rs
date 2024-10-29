use anyhow::Result;
use polars::frame::DataFrame;
use polars::prelude::DataFrameOps;

pub trait TOneHot {
    fn one_hot(self, col: &str) -> Result<Self>
    where
        Self: Sized;
}

impl TOneHot for DataFrame {
    fn one_hot(mut self, col: &str) -> Result<Self>
    where
        Self: Sized,
    {
        let mut dummy_df = self.select([col])?.to_dummies(None, false)?;

        let name = dummy_df
            .iter()
            .filter(|col| col.name().ends_with("_null"))
            .collect::<Vec<_>>();
        let name = name.first();

        if let Some(name) = name {
            dummy_df = dummy_df.drop(name.name())?;
        }

        // Удаляем исходный столбец и добавляем новые
        self = self.drop(col)?;
        self.hstack_mut(&dummy_df.get_columns())?;

        Ok(self)
    }
}
