import datetime
import pathlib

import pandas as pd

DATA_FILE = "./data/"


def aggregate_daily_stats_to_weekly(df):
    aggregated_df = pd.DataFrame(columns=df.columns)
    grouped = df.groupby("open_covid_region_code")
    for region_code, g_df in grouped:
        first_week_day = datetime.datetime.strptime(g_df.date.iloc[0], "%Y-%m-%d").weekday()
        idx = 0 if first_week_day == 0 else (7-first_week_day)
        week_data = g_df.iloc[idx].to_dict()
        week_day = 0
        for _, row in g_df.iloc[idx+1:].iterrows():
            if week_day < 6:
                week_data["hospitalized_new"] += row["hospitalized_new"]
                week_data["hospitalized_cumulative"] = row["hospitalized_cumulative"]
                week_day += 1
            else:
                aggregated_df = aggregated_df.append(week_data, ignore_index=True)
                week_day = 0
                week_data = row.to_dict()
    return aggregated_df


def clean_df_by_columns(df, threshold=0.5):
    # drop the columns where all entries are nan
    df.dropna(axis=1, inplace=True, how="all")
    # drop the columns less than 20% valid entries
    df.dropna(axis=1, inplace=True, thresh=df.shape[0]*threshold)


def clean_df_by_region(df, threshold=0.5, dropna=True):
    grouped = df.groupby("open_covid_region_code")
    df_res = pd.DataFrame(columns=df.columns)
    for region_code, df_grouped in grouped:
        # discard region with more than 60% rows that contains at least {threshold*100}% non-NaN entries
        if dropna:
            cleaned_df = df_grouped.dropna(thresh=df_grouped.shape[0]*threshold)
            if cleaned_df.shape[0] / df_grouped.shape[0] > 0.6:
                df_res = pd.concat([df_res, df_grouped], ignore_index=True, sort=False)
        # discard region with more than 60% 0f rows with effective data entries equal to 0
        else:
            zero_entries_per_row = (df_grouped == 0).astype(int).sum(axis=1)
            zero_entries_count = zero_entries_per_row[zero_entries_per_row == 2].count()
            if zero_entries_count / df_grouped.shape[0] < 0.6:
                df_res = pd.concat([df_res, df_grouped], ignore_index=True, sort=False)
    return df_res


def merge_dfs(df1, df2):
    # create id column used for df merging
    df1["id"] = df1["open_covid_region_code"] + df1["date"]
    df2["id"] = df2["open_covid_region_code"] + df2["date"]
    return pd.merge(df1, df2, on="id")


def load_raw_data(file_name):
    file_path = pathlib.Path(f"{DATA_FILE}/raw/{file_name}")
    if not (file_path.with_suffix('.pkl')).exists():
        df = pd.read_csv(file_path.with_suffix('.csv'))
        df.to_pickle(file_path.with_suffix('.pkl'))
    return pd.read_pickle(file_path.with_suffix('.pkl'))


def load_processed_data(file_name):
    file_path = pathlib.Path(f"{DATA_FILE}/processed/{file_name}_processed")
    if not (file_path.with_suffix('.pkl')).exists():
        raw_df = load_raw_data(file_name)
        # clean invalid columns
        clean_df_by_columns(raw_df)
        # clean invalid regions
        if "symptom" in file_name:
            df = clean_df_by_region(raw_df)
        else:
            df = clean_df_by_region(raw_df, dropna=False)
            # aggregate daily data to weekly
            df = aggregate_daily_stats_to_weekly(df)
        df.to_pickle(file_path.with_suffix('.pkl'))
    return pd.read_pickle(file_path.with_suffix('.pkl'))


if __name__ == "__main__":
    # select only us data for hospitalization df and save to a pickle file
    # hospitalization_cases_df = load_raw_data("aggregated_cc_by")
    # hospitalization_cases_us_df = hospitalization_cases_df[
    #     hospitalization_cases_df["open_covid_region_code"].str.contains("US-")]
    # hospitalization_cases_us_df.to_pickle(f"{DATA_FILE}/raw/aggregated_cc_by_us.pkl")

    # hospitalization_cases_us_weekly_df = load_processed_data("aggregated_cc_by_us")
    # search_trend_df = load_processed_data("2020_US_weekly_symptoms_dataset_v003")
    #
    # merged_df = merge_dfs(hospitalization_cases_us_weekly_df, search_trend_df)
    # merged_df.to_pickle(f"{DATA_FILE}/processed/merged.pkl")

    # merged_df = load_processed_data("merged")
    pass
