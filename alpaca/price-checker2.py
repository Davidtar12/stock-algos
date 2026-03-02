from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.enums import DataFeed
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from datetime import datetime, timedelta
import pytz
from dotenv import load_dotenv
import os
load_dotenv()


client = StockHistoricalDataClient(os.getenv('ALPACA_API_KEY'),os.getenv('ALPACA_SECRET_KEY'))

now = datetime.now(pytz.timezone("America/New_York"))
start = now - timedelta(minutes=30)

request = StockBarsRequest(
    symbol_or_symbols="orcl",
    timeframe=TimeFrame(1, TimeFrameUnit.Minute),
    start=start,
    end=now,
    feed=DataFeed.IEX
)

bars = client.get_stock_bars(request).df
print(bars)
