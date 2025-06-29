from .macd import MACD
from .rsi import RSI
from .sma import SMA
from .bollinger_bands import BollingerBands
from .obv import OBV

ALGORITHMS = {
    MACD.name: MACD,
    RSI.name: RSI,
    SMA.name: SMA,
    BollingerBands.name: BollingerBands,
    OBV.name: OBV,
}


def get_algorithm(name: str, db_config: dict):
    cls = ALGORITHMS.get(name)
    if cls is None:
        raise ValueError(f"unsupported TA algorithm: {name}")
    return cls(db_config)
