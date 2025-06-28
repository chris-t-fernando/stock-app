import pandas as pd
import psycopg2
try:  # pragma: no cover - optional dependency
    import talib  # type: ignore
except Exception:  # pragma: no cover - allow missing C library
    talib = None

from .base import BaseTAAlgorithm


class RSI(BaseTAAlgorithm):
    name = "rsi"
    table_name = "stock_ta_rsi"

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()
        closes = pd.to_numeric(df["close"], errors="coerce").astype(float).to_numpy(dtype=float)
        rsi = talib.RSI(closes)
        return pd.DataFrame({"ts": df["ts"], "rsi": rsi})

    def insert_records(self, ticker: str, interval: str, df: pd.DataFrame) -> int:
        if df is None or df.empty:
            return 0
        conn = psycopg2.connect(**self.db_config)
        cur = conn.cursor()
        insert_query = """
            INSERT INTO stock_ta_rsi (ticker, interval, ts, rsi)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (ticker, interval, ts) DO UPDATE
            SET rsi = EXCLUDED.rsi;
        """
        data = [
            (
                ticker,
                interval,
                row.ts.to_pydatetime() if hasattr(row.ts, "to_pydatetime") else row.ts,
                float(row.rsi) if pd.notna(row.rsi) else None,
            )
            for row in df.itertuples(index=False)
        ]
        cur.executemany(insert_query, data)
        conn.commit()
        rows_inserted = cur.rowcount
        cur.close()
        conn.close()
        return rows_inserted
