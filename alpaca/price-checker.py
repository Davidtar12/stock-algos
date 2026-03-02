from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestTradeRequest
from alpaca.data.enums import DataFeed
import os
from dotenv import load_dotenv
load_dotenv()


client = StockHistoricalDataClient(
    os.getenv('ALPACA_API_KEY'),
    os.getenv('ALPACA_SECRET_KEY')
)

request = StockLatestTradeRequest(
    symbol_or_symbols="AAPL",
    feed=DataFeed.IEX
)

result = client.get_stock_latest_trade(request)

trade = result["AAPL"]
print("Latest trade price (IEX, ~15 min delayed):", trade.price)
