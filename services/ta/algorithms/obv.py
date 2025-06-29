import pandas as pd
import psycopg2
try:  # pragma: no cover - optional dependency
    import talib  # type: ignore
except Exception:  # pragma: no cover - allow missing C library
    from types import SimpleNamespace

    talib = SimpleNamespace()

from .base import BaseTAAlgorithm


class OBV(BaseTAAlgorithm):
    name = "obv"
    table_name = "stock_ta_obv"

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()
        if not hasattr(talib, "OBV"):
            raise ImportError("talib library is required to compute OBV")
        closes = pd.to_numeric(df["close"], errors="coerce").astype(float).to_numpy(dtype=float)
        volumes = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype(float).to_numpy(dtype=float)
        obv = talib.OBV(closes, volumes)
        return pd.DataFrame({"ts": df["ts"], "obv": obv})

    def insert_records(self, ticker: str, interval: str, df: pd.DataFrame) -> int:
        if df is None or df.empty:
            return 0
        conn = psycopg2.connect(**self.db_config)
        cur = conn.cursor()
        insert_query = """
            INSERT INTO stock_ta_obv (ticker, interval, ts, obv)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (ticker, interval, ts) DO UPDATE
            SET obv = EXCLUDED.obv;
        """
        data = [
            (
                ticker,
                interval,
                row.ts.to_pydatetime() if hasattr(row.ts, "to_pydatetime") else row.ts,
                float(row.obv) if pd.notna(row.obv) else None,
            )
            for row in df.itertuples(index=False)
        ]
        cur.executemany(insert_query, data)
        conn.commit()
        rows_inserted = cur.rowcount
        cur.close()
        conn.close()
        return rows_inserted
