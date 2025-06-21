import yfinance as yf
import talib
import pandas as pd
import numpy as np

# --- Download historical data ---
ticker = "AAPL"
df = yf.download(ticker, interval="1d", period="6mo")

# --- Calculate MACD ---
close_prices = df["Close"].astype(float).values
if close_prices.ndim != 1:
    close_prices = close_prices.flatten()

macd, macdsignal, macdhist = talib.MACD(
    close_prices, fastperiod=12, slowperiod=26, signalperiod=9
)

df["MACD"] = macd
df["MACD_signal"] = macdsignal
df["MACD_hist"] = macdhist

# --- Calculate difference and detect crossover ---
df["MACD_diff"] = df["MACD"] - df["MACD_signal"]

# Only detect crossovers where both current and previous diff are valid
valid = df["MACD_diff"].notna() & df["MACD_diff"].shift(1).notna()
df["MACD_crossover"] = valid & (
    np.sign(df["MACD_diff"]) != np.sign(df["MACD_diff"].shift(1))
)

# --- Classify crossover direction ---
df["MACD_crossover_type"] = None
df.loc[df["MACD_crossover"] & (df["MACD_diff"] > 0), "MACD_crossover_type"] = "bullish"
df.loc[df["MACD_crossover"] & (df["MACD_diff"] < 0), "MACD_crossover_type"] = "bearish"

# --- Display most recent crossovers ---
crossovers = df[df["MACD_crossover_type"].notna()][
    ["Close", "MACD", "MACD_signal", "MACD_crossover_type"]
]

print(crossovers.tail())
