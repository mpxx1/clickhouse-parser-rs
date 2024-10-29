use std::collections::{HashMap, HashSet};
use polars::datatypes::PlSmallStr;
use polars::frame::DataFrame;
use polars::prelude::{ChunkSet, NamedFrom, Series};
use anyhow::Result;

pub trait TPushCols<T> {
    fn push_cols(self, key_col: &str, val: T) -> Result<Self>
    where
        Self: Sized;
}


impl TPushCols<HashMap<u32, Vec<String>>> for DataFrame
{
    fn push_cols(mut self, key_col: &str, val: HashMap<u32, Vec<String>>) -> Result<Self>
    where
        Self: Sized
    {
        // Получаем уникальные имена столбцов из значений в HashMap
        let mut all_column_names = HashSet::new();
        for columns in val.values() {
            for col in columns {
                all_column_names.insert(col.clone());
            }
        }

        // Добавляем новые столбцы с нулями по умолчанию
        for col_name in &all_column_names {
            let new_col = Series::new(PlSmallStr::from(col_name), vec![0; self.height()]);
            self.with_column(new_col).expect("Failed to add column");
        }

        // Мапа значений в индексах для более быстрого доступа
        let tmp = self.clone();
        let key_col_data = tmp.column(key_col)?.u32()?;

        // Проходим по каждой строке и проставляем единицы на основании HashMap
        for (row_idx, key) in key_col_data
            .into_iter()
            .flatten()
            .enumerate()
        {
            if let Some(columns_to_update) = val.get(&key) {
                for col_name in columns_to_update {
                    self.try_apply(
                        col_name, |s| s
                                       .i32()?
                                       .scatter_with(
                                           vec![row_idx as u32], |_| Some(1)
                                       )
                    )?;
                }
            }
        }

        Ok(self)
    }
}