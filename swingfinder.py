from ib_insync import IB, Stock, ScannerSubscription, util
import pandas as pd
import time
import ta
import sys
import os

# Debug: print interpreter and venv info when script starts
print(f"Python executable: {sys.executable}")
print(f"sys.prefix: {sys.prefix}")
print(f"VIRTUAL_ENV: {os.environ.get('VIRTUAL_ENV')}")

# Configure IBKR API client
ibkr_client = IB()

# Scan parameters
scan_params = {
    'instrument': 'STK',
    'locationCode': 'STK.US.MAJOR',
    'scanCode': 'TOP_PERC_GAIN',
    'minPrice': 5.0,
    'volumeAbove': 1000000,
    'rsiLowerBound': 30,
    'rsiUpperBound': 70,
    'macdLowerBound': -0.8,
    'macdUpperBound': 0.8
}

def connect_to_ibkr():
    """Establish connection to IBKR"""
    try:
        if not ibkr_client.isConnected():
            ibkr_client.connect('127.0.0.1', 7496, clientId=1)
            while not ibkr_client.isConnected():
                time.sleep(1)
        print("Connected to IBKR")
        return True
    except Exception as e:
        print(f"Failed to connect to IBKR: {str(e)}")
        return False

def scan_stocks():
    """Perform stock scanner operation"""
    try:
        # Put numeric constraints directly into the ScannerSubscription fields.
        # IB API expects these as part of the subscription (abovePrice, aboveVolume, etc.)
        scan_sub = ScannerSubscription(
            instrument=scan_params['instrument'],
            locationCode=scan_params['locationCode'],
            scanCode=scan_params['scanCode'],
            abovePrice=scan_params.get('minPrice'),
            aboveVolume=scan_params.get('volumeAbove')
        )

        print(f"Requesting scan for {scan_sub.scanCode} with abovePrice={scan_sub.abovePrice} aboveVolume={scan_sub.aboveVolume}")
        # Call reqScannerData with the subscription object only; do not pass a misc filters dict
        scan_data = ibkr_client.reqScannerData(scan_sub)
        time.sleep(2)

        if scan_data:
            print(f"Found {len(scan_data)} scanner results")
        else:
            print("No scanner results found")

        return scan_data

    except Exception as e:
        print(f"Error during scanner operation: {str(e)}")
        return []

def try_multiple_scans():
    """Try different scan codes to find more candidates"""
    scan_codes = ['TOP_PERC_GAIN', 'HOT_BY_VOLUME', 'MOST_ACTIVE']
    all_results = set()

    for code in scan_codes:
        scan_params['scanCode'] = code
        print(f"\nTrying scan code: {code}")
        results = scan_stocks()
        if results:
            all_results.update([scan_data.contractDetails.contract.symbol for scan_data in results if
                                hasattr(scan_data, 'contractDetails') and hasattr(scan_data.contractDetails,
                                                                                  'contract') and hasattr(
                                    scan_data.contractDetails.contract, 'symbol')])
        time.sleep(2)  # Delay between scan requests

    return list(all_results)

def fetch_data(symbol):
    """Fetch historical data for a given symbol"""
    try:
        contract = Stock(symbol, 'SMART', 'USD')
        ibkr_client.qualifyContracts(contract)

        bars = ibkr_client.reqHistoricalData(
            contract,
            endDateTime='',
            durationStr='1 W',
            barSizeSetting='1 hour',
            whatToShow='TRADES',
            useRTH=True,
            formatDate=1
        )

        if not bars:
            print(f"No historical data received for {symbol}")
            return None

        df = util.df(bars)
        df['datetime'] = pd.to_datetime(df['date'])
        df.set_index('datetime', inplace=True)
        return df

    except Exception as e:
        print(f"Error fetching data for {symbol}: {str(e)}")
        return None

def calculate_indicators(df):
    """Calculate technical indicators"""
    try:
        if df is None or df.empty:
            return None

        df['EMA10'] = ta.trend.ema_indicator(df['close'], window=10)
        df['EMA200'] = ta.trend.ema_indicator(df['close'], window=200)
        df['RSI'] = ta.momentum.rsi(df['close'], window=21)
        macd = ta.trend.MACD(df['close'], window_fast=12, window_slow=26, window_sign=9)
        df['MACD'] = macd.macd()
        df['MACD_signal'] = macd.macd_signal()

        return df

    except Exception as e:
        print(f"Error calculating indicators: {str(e)}")
        return None

def swing_trade_signal(df):
    """Check if stock meets swing trade criteria with more flexible conditions"""
    try:
        if df is None or df.empty:
            return False

        latest = df.iloc[-1]

        trend_aligned = (
                latest['close'] > latest['EMA10'] or
                (latest['close'] > latest['EMA200'] and
                 latest['close'] > df['close'].mean())
        )
        print(f"    Trend alignment: {'✓' if trend_aligned else '❌'}")

        rsi = latest['RSI']
        rsi_valid = (scan_params['rsiLowerBound'] <= rsi <= scan_params['rsiUpperBound'])
        print(f"    RSI ({rsi:.2f}): {'✓' if rsi_valid else '❌'}")

        macd_diff = latest['MACD'] - latest['MACD_signal']
        macd_valid = (scan_params['macdLowerBound'] <= macd_diff <= scan_params['macdUpperBound'])
        print(f"    MACD diff ({macd_diff:.3f}): {'✓' if macd_valid else '❌'}")

        avg_volume = df['volume'].mean()
        volume_valid = avg_volume >= scan_params['volumeAbove']
        print(f"    Volume ({avg_volume:.0f}): {'✓' if volume_valid else '❌'}")

        conditions_met = sum([trend_aligned, rsi_valid, macd_valid, volume_valid])
        return conditions_met >= 3

    except Exception as e:
        print(f"    ❌ Error in swing trade signal calculation: {str(e)}")
        return False

def check_conditions(symbol):
    """Check all conditions for a given stock with more detailed output"""
    try:
        print(f"\nAnalyzing {symbol}:")

        df = fetch_data(symbol)
        if df is None:
            print(f"  ❌ Could not fetch data for {symbol}")
            return None

        df = calculate_indicators(df)
        if df is None:
            print(f"  ❌ Could not calculate indicators for {symbol}")
            return None

        current_price = df['close'].iloc[-1]
        print(f"  Current Price: ${current_price:.2f}")
        print(f"  Average Volume: {df['volume'].mean():.0f}")

        if swing_trade_signal(df):
            print(f"  ✓ {symbol} passed technical conditions")
            print(f"  ✅ {symbol} MATCHED CRITERIA")
            return symbol
        else:
            print(f"  ❌ {symbol} failed technical conditions")

        return None

    except Exception as e:
        print(f"  ❌ Error checking conditions for {symbol}: {str(e)}")
        return None

def main():
    """Main execution function"""
    try:
        if not connect_to_ibkr():
            return

        print("\nStarting scanner with multiple scan codes...")
        all_results = try_multiple_scans()

        if not all_results:
            print("No scanner results received")
            return

        stocks_to_trade = []
        print(f"\nAnalyzing {len(all_results)} unique stocks...")

        for symbol in all_results:
            result = check_conditions(symbol)
            if result:
                stocks_to_trade.append(result)

        print("\n=== FINAL RESULTS ===")
        if stocks_to_trade:
            print(f"Found {len(stocks_to_trade)} stocks matching criteria:")
            for symbol in stocks_to_trade:
                print(f"✅ {symbol}")

            print("\nTrade Setup Summary:")
            for symbol in stocks_to_trade:
                df = fetch_data(symbol)
                if df is not None:
                    current_price = df['close'].iloc[-1]
                    avg_volume = df['volume'].mean()
                    rsi = ta.momentum.rsi(df['close'], window=21).iloc[-1]
                    print(f"\n{symbol}:")
                    print(f"  Price: ${current_price:.2f}")
                    print(f"  Avg Volume: {avg_volume:,.0f}")
                    print(f"  RSI: {rsi:.2f}")
        else:
            print("No stocks matched the criteria")

    except Exception as e:
        print(f"Error in main execution: {str(e)}")
    finally:
        if ibkr_client.isConnected():
            ibkr_client.disconnect()
            print("\nDisconnected from IBKR")

if __name__ == "__main__":
    print("=== Scanner Parameters ===")
    print(f"Minimum Volume: {scan_params['volumeAbove']:,}")
    print(f"Minimum Price: ${scan_params['minPrice']:.2f}")
    print(f"RSI Range: {scan_params['rsiLowerBound']} - {scan_params['rsiUpperBound']}")
    print(f"MACD Range: {scan_params['macdLowerBound']} to {scan_params['macdUpperBound']}")

    main()
