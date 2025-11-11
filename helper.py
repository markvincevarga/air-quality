import pandas as pd

def add_lagged_data(df: pd.DataFrame, col_to_shift: str, by_days: int) -> pd.DataFrame:
    """
    Adds a column to each row that contains the value of `col_to_shift`
    lagged by `by_days` days

    Args:
        df (pd.DataFrame): DataFrame containing rows with different dates and ids.
        col_to_shift (str): Column name to lag the values of.
        by_days (int): Number of days to lag the data for.
    """
    df = df.sort_values(by=['id', 'date'])
    df[f'{col_to_shift}_lagged_{by_days}d'] = df.groupby('id')[col_to_shift].shift(by_days)
    df = df.sort_values(by=['date', 'id'])
    return df
