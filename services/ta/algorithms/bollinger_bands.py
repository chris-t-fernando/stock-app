import pandas as pd
import psycopg2
try:  # pragma: no cover - optional dependency
    import talib  # type: ignore
except Exception:  # pragma: no cover - allow missing C library
    from types import SimpleNamespace

    talib = SimpleNamespace()

from .base import BaseTAAlgorithm


class BollingerBands(BaseTAAlgorithm):
    name = "bollingerbands"
    table_name = "stock_ta_bollinger_bands"

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()
        if not hasattr(talib, "BBANDS"):
            raise ImportError("talib library is required to compute Bollinger Bands")
        closes = pd.to_numeric(df["close"], errors="coerce").astype(float).to_numpy(dtype=float)
        upper, middle, lower = talib.BBANDS(closes)
        return pd.DataFrame(
            {
                "ts": df["ts"],
                "bb_upper": upper,
                "bb_middle": middle,
                "bb_lower": lower,
            }
        )

    def insert_records(self, ticker: str, interval: str, df: pd.DataFrame) -> int:
        if df is None or df.empty:
            return 0
        conn = psycopg2.connect(**self.db_config)
        cur = conn.cursor()
        insert_query = """
            INSERT INTO stock_ta_bollinger_bands (ticker, interval, ts, bb_upper, bb_middle, bb_lower)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (ticker, interval, ts) DO UPDATE
            SET bb_upper = EXCLUDED.bb_upper,
                bb_middle = EXCLUDED.bb_middle,
                bb_lower = EXCLUDED.bb_lower;
        """
        data = [
            (
                ticker,
                interval,
                row.ts.to_pydatetime() if hasattr(row.ts, "to_pydatetime") else row.ts,
                float(row.bb_upper) if pd.notna(row.bb_upper) else None,
                float(row.bb_middle) if pd.notna(row.bb_middle) else None,
                float(row.bb_lower) if pd.notna(row.bb_lower) else None,
            )
            for row in df.itertuples(index=False)
        ]
        cur.executemany(insert_query, data)
        conn.commit()
        rows_inserted = cur.rowcount
        cur.close()
        conn.close()
        return rows_inserted
