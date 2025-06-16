from stocklib.messaging import EventBus
import yfinance as yf
import time

bus = EventBus()


def fetch_stock_data(ticker, from_date=None):
    stock = yf.Ticker(ticker)
    hist = (
        stock.history(period="max") if not from_date else stock.history(start=from_date)
    )
    return hist.to_dict("records")


def run():
    ticker = "AAPL"
    last_known_date = None  # placeholder
    data = fetch_stock_data(ticker, last_known_date)
    bus.publish(
        "stock.updated", "stock.updated", {"ticker": ticker, "range": ["start", "end"]}
    )


if __name__ == "__main__":
    run()
