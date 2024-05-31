import pandas as pd
from tqdm import tqdm


def transform(profit_table, date):
    """Собирает таблицу флагов активности по продуктам
    на основании прибыли и количеству совершёных транзакций

    :param profit_table: таблица с суммой и кол-вом транзакций
    :param date: дата расчёта флагоа активности

    :return df_tmp: pandas-датафрейм флагов за указанную дату
    """
    start_date = pd.to_datetime(date) - pd.DateOffset(months=2)
    end_date = pd.to_datetime(date) + pd.DateOffset(months=1)
    date_list = pd.date_range(start=start_date, end=end_date, freq="M").strftime(
        "%Y-%m-01"
    )

    df_tmp = (
        profit_table[profit_table["date"].isin(date_list)]
        .drop("date", axis=1)
        .groupby("id")
        .sum()
    )

    product_list = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]
    for product in tqdm(product_list):
        df_tmp[f"flag_{product}"] = df_tmp.apply(
            lambda x: x[f"sum_{product}"] != 0 and x[f"count_{product}"] != 0, axis=1
        ).astype(int)

    df_tmp = df_tmp.filter(regex="flag").reset_index()

    return df_tmp


def generate_activity_flags(profit_df, calc_date, target_product):
    """
    Генерирует таблицу флагов активности для заданного продукта
    на основе прибыли и количества транзакций.

    :param profit_df: DataFrame с информацией о прибыли и количестве транзакций
    :param calc_date: дата для расчета флагов активности
    :param target_product: продукт, для которого рассчитываются флаги

    :return activity_flags_df: pandas DataFrame с флагами активности для указанного продукта и даты
    """
    start_period = pd.to_datetime(calc_date) - pd.DateOffset(months=2)
    end_period = pd.to_datetime(calc_date) + pd.DateOffset(months=1)
    relevant_dates = pd.date_range(start=start_period, end=end_period, freq="M").strftime("%Y-%m-01")

    filtered_df = (
        profit_df[profit_df["date"].isin(relevant_dates)]
        .drop("date", axis=1)
        .groupby("id")
        .sum()
    )

    filtered_df[f"flag_{target_product}"] = filtered_df.apply(
        lambda row: row[f"sum_{target_product}"] != 0 and row[f"count_{target_product}"] != 0, axis=1
    ).astype(int)

    activity_flags_df = filtered_df[[f"flag_{target_product}"]].reset_index()

    return activity_flags_df


if __name__ == "__main__":
    profit_data = pd.read_csv("profit_table.csv")
    date = "2024-03-01"
    flags_activity = transform(profit_data, date)
    flags_activity.to_csv("flags_activity.csv", index=False)
    product = "a"
    flags_activity_product = generate_activity_flags(profit_data, date, product)
    flags_activity_product.to_csv(f"flags_activity_{product}.csv", index=False)
