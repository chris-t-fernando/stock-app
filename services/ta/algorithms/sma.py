import pandas as pd
import psycopg2
try:  # pragma: no cover - optional dependency
    import talib  # type: ignore
except Exception:  # pragma: no cover - allow missing C library
    from types import SimpleNamespace

    talib = SimpleNamespace()

from .base import BaseTAAlgorithm


class SMA(BaseTAAlgorithm):
    name = "sma"
    table_name = "stock_ta_sma"

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()
        if not hasattr(talib, "SMA"):
            raise ImportError("talib library is required to compute SMA")
        closes = pd.to_numeric(df["close"], errors="coerce").astype(float).to_numpy(dtype=float)
        sma = talib.SMA(closes)
        return pd.DataFrame({"ts": df["ts"], "sma": sma})

    def insert_records(self, ticker: str, interval: str, df: pd.DataFrame) -> int:
        if df is None or df.empty:
            return 0
        conn = psycopg2.connect(**self.db_config)
        cur = conn.cursor()
        insert_query = """
            INSERT INTO stock_ta_sma (ticker, interval, ts, sma)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (ticker, interval, ts) DO UPDATE
            SET sma = EXCLUDED.sma;
        """
        data = [
            (
                ticker,
                interval,
                row.ts.to_pydatetime() if hasattr(row.ts, "to_pydatetime") else row.ts,
                float(row.sma) if pd.notna(row.sma) else None,
            )
            for row in df.itertuples(index=False)
        ]
        cur.executemany(insert_query, data)
        conn.commit()
        rows_inserted = cur.rowcount
        cur.close()
        conn.close()
        return rows_inserted
