import polars as pl

def read_tsv_to_dataframe(file_path: str) -> pl.DataFrame:
    try:
        # Чтение TSV-файла в DataFrame с указанием разделителя '\t'
        df = pl.read_csv(file_path, separator='\t')
        return df
    except Exception as e:
        print(f"Ошибка при чтении файла {file_path}: {e}")
        return None

def polars_df_to_arff(dataset: pl.DataFrame, path: str, relation_name: str):
    # Открываем файл для записи
    with open(path, 'w', encoding="utf-8") as file:
        # Записываем RELATION
        file.write(f'@RELATION {relation_name}\n\n')

        # Записываем ATTRIBUTES
        columns_with_types = zip(dataset.columns, dataset.dtypes)

        for column_name, column_type in columns_with_types:
            # Определяем тип для ARFF
            arff_type = None
            match str(column_type):
                case 'Int64' | 'Float64' | 'Boolean':
                    arff_type = "NUMERIC"
                case 'String':
                    arff_type = "STRING"
                case 'Utf8':
                    arff_type = "STRING"
                case _:
                    raise ValueError(f"Cannot convert column '{column_name}' with type '{column_type}' to ARFF format.")
            # Записываем атрибут в ARFF
            file.write(f'@ATTRIBUTE {column_name} {arff_type}\n')

        # Добавляем раздел @DATA
        file.write('\n@DATA\n')

        # Пишем данные без заголовков, заменяя `,,` на `,?,`
        csv_data = dataset.write_csv(include_header=False)
        csv_data = csv_data.replace(",,", ",?,")  # Заменяем отсутствующие значения
        csv_data = csv_data.replace(",,", ",?,")
        csv_data = csv_data.replace("refit STRING", "refit {euro, design, cosm, rnv}")
        csv_data = csv_data.replace("view_from_windows STRING", "view_from_windows {street, both, yard}")
        csv_data = csv_data.replace("utility_bills STRING", "utility_bills {no, yes, by_counters}")
        csv_data = csv_data.replace("walls STRING", "walls { mono, brick, block, ferroc, mixed, panel }")
        csv_data = csv_data.replace("ceiling_type STRING", "ceiling_type { mono, ferroc, mixed, wood, concr }")
        csv_data = csv_data.replace("prepayment STRING", "prepayment NUMERIC")

        file.write(csv_data)


if __name__ == '__main__':
    polars_df_to_arff(
        read_tsv_to_dataframe("/Users/m/RustroverProjects/ml_lab1/short.tsv"),
        "/Users/m/RustroverProjects/ml_lab1/flats.arff",
        "flats"
    )