import unittest
from unittest.mock import patch
import pandas as pd
import numpy as np

def load_ta_service():
    with patch(
        "pubsub_wrapper.load_config",
        return_value={
            "PGHOST": "",
            "PGUSER": "",
            "PGPASSWORD": "",
            "PGDATABASE": "",
            "PGPORT": "5432",
            "redis_url": "redis://localhost:6379",
        },
    ):
        if "services.ta.ta_service" in sys.modules:
            return importlib.reload(sys.modules["services.ta.ta_service"])
        return importlib.import_module("services.ta.ta_service")

import sys
import importlib

class TestCalculateMACD(unittest.TestCase):
    def test_macd_crossover_detection(self):
        ts = load_ta_service()
        df = pd.DataFrame({
            'ts': pd.date_range('2024-01-01', periods=3),
            'close': [1.0, 2.0, 3.0]
        })

        with patch.object(ts, 'talib') as mock_talib:
            mock_talib.MACD.return_value = (
                np.array([1.0, 2.0, 3.0]),
                np.array([2.0, 1.0, 1.0]),
                np.array([0.0, 0.0, 0.0])
            )
            result = ts.calculate_macd(df)

        assert result.loc[1, 'macd_crossover']
        assert result.loc[1, 'macd_crossover_type'] == 'bullish'
        assert not result.loc[2, 'macd_crossover']
