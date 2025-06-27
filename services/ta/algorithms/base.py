import pandas as pd
import psycopg2


class BaseTAAlgorithm:
    """Base class for technical analysis algorithms."""

    name: str = "base"
    table_name: str

    def __init__(self, db_config: dict):
        self.db_config = db_config

    def get_latest_ts(self, ticker: str, interval: str):
        conn = psycopg2.connect(**self.db_config)
        cur = conn.cursor()
        cur.execute(
            f"SELECT MAX(ts) FROM {self.table_name} WHERE ticker = %s AND interval = %s",
            (ticker, interval),
        )
        result = cur.fetchone()[0]
        cur.close()
        conn.close()
        return result

    def process(self, ticker: str, interval: str, price_df: pd.DataFrame) -> int:
        if price_df is None or price_df.empty:
            return 0
        last_ts = self.get_latest_ts(ticker, interval)
        df = self.calculate(price_df)
        if last_ts:
            df = df[df["ts"] > last_ts]
        return self.insert_records(ticker, interval, df)

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError

    def insert_records(self, ticker: str, interval: str, df: pd.DataFrame) -> int:
        raise NotImplementedError
