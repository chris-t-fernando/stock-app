import numpy as np
import pandas as pd
import psycopg2
try:  # pragma: no cover - optional dependency
    import talib  # type: ignore
except Exception:  # pragma: no cover - allow missing C library
    talib = None

from .base import BaseTAAlgorithm


class MACD(BaseTAAlgorithm):
    name = "macd"
    table_name = "stock_ta_macd"

    def calculate(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()
        if talib is None:
            raise ImportError("talib library is required to compute MACD")

        if df["close"].isna().any():
            df = df.dropna(subset=["close"]).reset_index(drop=True)
            if df.empty:
                return pd.DataFrame()

        closes = pd.to_numeric(df["close"], errors="raise").astype(float).to_numpy(dtype=float)
        macd, signal, hist = talib.MACD(closes)
        diff = macd - signal

        res = pd.DataFrame(
            {
                "ts": df["ts"],
                "macd": macd,
                "macd_signal": signal,
                "macd_hist": hist,
                "macd_diff": diff,
            }
        )

        res["macd_crossover"] = False
        res["macd_crossover_type"] = None

        bullish = (diff >= 0) & (pd.Series(diff).shift(1) < 0)
        bearish = (diff <= 0) & (pd.Series(diff).shift(1) > 0)
        res.loc[bullish, "macd_crossover"] = True
        res.loc[bullish, "macd_crossover_type"] = "bullish"
        res.loc[bearish, "macd_crossover"] = True
        res.loc[bearish, "macd_crossover_type"] = "bearish"

        return res

    def insert_records(self, ticker: str, interval: str, df: pd.DataFrame) -> int:
        if df is None or df.empty:
            return 0
        conn = psycopg2.connect(**self.db_config)
        cur = conn.cursor()
        insert_query = """
            INSERT INTO stock_ta_macd (
                ticker, interval, ts, macd, macd_signal, macd_hist, macd_diff,
                macd_crossover, macd_crossover_type
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ticker, interval, ts) DO UPDATE
            SET macd = EXCLUDED.macd,
                macd_signal = EXCLUDED.macd_signal,
                macd_hist = EXCLUDED.macd_hist,
                macd_diff = EXCLUDED.macd_diff,
                macd_crossover = EXCLUDED.macd_crossover,
                macd_crossover_type = EXCLUDED.macd_crossover_type;
        """
        data = [
            (
                ticker,
                interval,
                row.ts.to_pydatetime() if hasattr(row.ts, "to_pydatetime") else row.ts,
                float(row.macd) if pd.notna(row.macd) else None,
                float(row.macd_signal) if pd.notna(row.macd_signal) else None,
                float(row.macd_hist) if pd.notna(row.macd_hist) else None,
                float(row.macd_diff) if pd.notna(row.macd_diff) else None,
                bool(row.macd_crossover) if pd.notna(row.macd_crossover) else None,
                row.macd_crossover_type,
            )
            for row in df.itertuples(index=False)
        ]
        cur.executemany(insert_query, data)
        conn.commit()
        rows_inserted = cur.rowcount
        cur.close()
        conn.close()
        return rows_inserted
