import os
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from pymongo import MongoClient
from datetime import datetime, timedelta, timezone
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.data import StockHistoricalDataClient, StockLatestQuoteRequest, StockBarsRequest, TimeFrame, StockQuotesRequest, StockTradesRequest

load_dotenv()
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
MONGODB_URI = os.getenv("MONGODB_URI")
mongo_client = MongoClient(MONGODB_URI)
trading_client = TradingClient(API_KEY, API_SECRET, paper=True)
historical_client = StockHistoricalDataClient(API_KEY, API_SECRET)

###########################################
def process_action(user_action):
    match user_action:
        case "info":
            get_account_info()
        case "quote":
            get_stock_info()
        case "order":
            place_order()
        case "status":
            get_order_status()
        case "historical":
            get_historical_data()
        case "enrich":
            EnrichIndicators()
        case "help":
            print('Possible actions:')
            print('"Info" = Show account buying power')
            print('"Quote" = Enter stock names to get current price')
            print('"Order" = Issue a Market Order')
            print('"Historical" = Get historical data for past 30 days')
            print('"Enrich" = Compute and store technical indicators')
            print('"Status" = Show status of Open Orders')
            print('"Help" = Show this menu')
            print('"Exit" = Exit program')
            menu()
        case "exit":
            print("Goodbye!")
        case _:
            print(f"Unknown command: {user_action}")
            process_action("help")

###########################################
def get_account_info():
    # Get our account information.
    account = trading_client.get_account()
    positions = trading_client.get_all_positions()

    # Check if our account is restricted from trading.
    if account.trading_blocked:
        print('Account is currently restricted from trading.')

    # Print Account Info
    print("Account Info: ")
    print(f'${account.buying_power} is available as buying power.')
    if positions:
        print("Positions:")
        for pos in positions:
            symbol = pos.symbol
            market_value = pos.market_value
            quantity = pos.qty
            avg_entry = pos.avg_entry_price
            change_today = pos.change_today
            curr_price = pos.current_price
            
            print(f"## Symbol: {symbol}\n----> Market Value: ${market_value}\n----> Quantity: {quantity}\n--> Average Entry Price: {avg_entry}\n--> Change Today: {change_today}\n--> Current Price: {curr_price}")
    else:
        print("Positions: None")
    menu()

###########################################
def get_stock_list():
    user_input = True
    stock_list = []
    while user_input:
        provided_stock = input("Enter Stocks Ticker Names, when Done enter 'Done': ")
        if provided_stock == "Done":
            user_input = False
        else:
            stock_list.append(provided_stock)
    
    return stock_list

###########################################
def convert_data(stock, data, action):
    if not data:
        print(f'No {action} data found for {stock} in the last 30 days.')
        return None
    if not isinstance(data[0], dict):
        data = [
            d.model_dump() if hasattr(d, "model_dump") else d.dict() if hasattr(d, "dict") else vars(d) for d in data
        ]
    df = pd.DataFrame(data)
    if df.empty or 'timestamp' not in df.columns:
        print(f'Data for {stock} is incomplete. Columns found: {df.columns}')
        return None
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df.set_index('timestamp', inplace=True)
    return df

###########################################
def get_stock_info():
    stock_list = get_stock_list()
    print(stock_list)

    # multi symbol request - single symbol is similar
    multisymbol_request_params = StockLatestQuoteRequest(symbol_or_symbols=stock_list)
    latest_multisymbol_quotes = historical_client.get_stock_latest_quote(multisymbol_request_params)

    for stock in stock_list:
        latest_ask_price = latest_multisymbol_quotes[stock].ask_price
        print(f'{stock} Latest Ask Price: ', latest_ask_price)
    
    menu()

###########################################
def place_order():
    get_type = True

    stock = input('Enter stock: ')
    quantity = input('Enter quantity: ')
    while get_type:
        order_type = input('Enter "Buy" or "Sell": ')
        if order_type.lower() == 'buy':
            order_type = OrderSide.BUY
            get_type = False
        elif order_type.lower() == 'sell':
            order_type = OrderSide.SELL
            get_type = False
        else:
            print(f'Unknown Order Type: {order_type}')
    # preparing orders
    market_order_data = MarketOrderRequest(
                        symbol=stock,
                        qty=quantity,
                        side=order_type,
                        time_in_force=TimeInForce.DAY
                        )

    # Market order
    trading_client.submit_order(order_data=market_order_data)
    print(f'Market Order Placed: \nSymbol: {stock}\nQuantity: {quantity}\nSide: {order_type}')
    menu()

###########################################
def get_order_status():
    orders = trading_client.get_orders()
    print(f'Open Orders: \n{orders}')
    menu()

###########################################
def add_row(db_row):
    db = mongo_client['TradingApp']
    collection = db['HistoricalData']
    result = collection.insert_many(db_row)
    print(f'Inserted document IDs: {result.inserted_ids}')

###########################################
def EnrichIndicators():
    db = mongo_client['TradingApp']
    collection = db['HistoricalData']

    # Get symbols that have OHLC data
    symbols = collection.distinct('stock', {'close': {'$exists': True}})
    if not symbols:
        print('No symbols with OHLC data found to enrich.')
        menu()
        return

    for symbol in symbols:
        print(f'Enriching indicators for {symbol} ...')
        cursor = collection.find(
            {
                'stock': symbol,
                'open': {'$exists': True},
                'high': {'$exists': True},
                'low': {'$exists': True},
                'close': {'$exists': True},
            },
            {'_id': 1, 'timestamp': 1, 'open': 1, 'high': 1, 'low': 1, 'close': 1}
        ).sort('timestamp', 1)

        docs = list(cursor)
        if not docs:
            print(f'No OHLC documents found for {symbol}.')
            continue

        df = pd.DataFrame(docs)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp').set_index('timestamp')

        # Ensure numeric types
        for col in ['open', 'high', 'low', 'close']:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        close = df['close']
        high = df['high']
        low = df['low']

        # Simple Moving Averages
        sma20 = close.rolling(window=20, min_periods=20).mean()
        sma50 = close.rolling(window=50, min_periods=50).mean()

        # Exponential Moving Averages
        ema12 = close.ewm(span=12, adjust=False, min_periods=12).mean()
        ema26 = close.ewm(span=26, adjust=False, min_periods=26).mean()

        # MACD
        macd = ema12 - ema26
        macdSignal = macd.ewm(span=9, adjust=False, min_periods=9).mean()
        macdHistogram = macd - macdSignal

        # RSI (14) using Wilder's smoothing via EWM(alpha=1/14)
        delta = close.diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.ewm(alpha=1/14, adjust=False, min_periods=14).mean()
        avg_loss = loss.ewm(alpha=1/14, adjust=False, min_periods=14).mean()
        rs = avg_gain / avg_loss
        rsi14 = 100 - (100 / (1 + rs))

        # ATR(14), DMI(14), ADX(14)
        prev_close = close.shift(1)
        high_diff = high.diff()
        low_diff = low.diff()

        plus_dm = pd.Series(np.where((high_diff > low_diff) & (high_diff > 0), high_diff, 0.0), index=high.index)
        minus_dm = pd.Series(np.where((low_diff > high_diff) & (low_diff > 0), low_diff, 0.0), index=low.index)

        tr_components = pd.concat([
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs()
        ], axis=1)
        tr = tr_components.max(axis=1)

        atr14 = tr.ewm(alpha=1/14, adjust=False, min_periods=14).mean()

        plus_di = 100 * (plus_dm.ewm(alpha=1/14, adjust=False, min_periods=14).mean() / atr14)
        minus_di = 100 * (minus_dm.ewm(alpha=1/14, adjust=False, min_periods=14).mean() / atr14)

        dx = 100 * (abs(plus_di - minus_di) / (plus_di + minus_di))
        adx14 = dx.ewm(alpha=1/14, adjust=False, min_periods=14).mean()

        # Bollinger Bands (20, 2)
        bb_middle = close.rolling(window=20, min_periods=20).mean()
        bb_std = close.rolling(window=20, min_periods=20).std(ddof=0)
        bollingerUpper = bb_middle + 2 * bb_std
        bollingerLower = bb_middle - 2 * bb_std

        ind_df = pd.DataFrame({
            'sma20': sma20,
            'sma50': sma50,
            'ema12': ema12,
            'ema26': ema26,
            'macd': macd,
            'macdSignal': macdSignal,
            'macdHistogram': macdHistogram,
            'rsi14': rsi14,
            'adx14': adx14,
            'plusDmi14': plus_di,
            'minusDmi14': minus_di,
            'atr14': atr14,
            'bollingerUpper': bollingerUpper,
            'bollingerMiddle': bb_middle,
            'bollingerLower': bollingerLower
        })

        updates = 0
        for ts, row in ind_df.iterrows():
            doc_id = df.at[ts, '_id']
            set_fields = {}
            for key, val in row.items():
                if pd.notna(val) and np.isfinite(val):
                    set_fields[key] = float(val)
            if set_fields:
                collection.update_one({'_id': doc_id}, {'$set': set_fields})
                updates += 1
        print(f'Updated {updates} documents for {symbol}.')

    menu()

###########################################
def get_historical_data():
    stock_list = get_stock_list()
    print(stock_list)
    current_datetime = datetime.now(timezone.utc)
    thirty_days_ago = current_datetime - timedelta(days=1)

    bar_request_params = StockBarsRequest(
        symbol_or_symbols=stock_list,
        timeframe=TimeFrame.Minute,
        start=thirty_days_ago,
        end=current_datetime,
        feed="iex"
    )

    quote_request_params = StockQuotesRequest(
        symbol_or_symbols=stock_list,
        start=thirty_days_ago,
        end=current_datetime,
        feed="iex",
        # limit=20 #REMOVE FOR PRODUCTION
    )

    trade_request_params = StockTradesRequest(
        symbol_or_symbols=stock_list,
        start=thirty_days_ago,
        end=current_datetime,
        feed="iex",
        # limit=20 # REMOVE FROM PROD
    )

    bars = historical_client.get_stock_bars(bar_request_params)
    quotes = historical_client.get_stock_quotes(quote_request_params)
    trades = historical_client.get_stock_trades(trade_request_params)

    for stock in stock_list:
        db_rows = {}

        # BAR DATA
        print(f'## BAR DATA FOR: {stock}')
        print(bars[stock])

        # QUOTES DATA
        quote_df = convert_data(stock, quotes.data.get(stock), 'quote')
        minute_summary = quote_df['ask_price'].resample('1min').mean()
        print(f'## HISTORICAL QUOTE DATA FOR: {stock}')
        for ts, ask in minute_summary.dropna().items():
            print(f'{stock} - {ts}: {ask}') 
            db_rows[ts] = ({"stock": stock, "timestamp": ts, "quote": ask})

        # TRADES DATA
        trade_df = convert_data(stock, trades.data.get(stock), 'trade')
        if trade_df is not None:
            minute_trades = trade_df.resample('1min').agg({'price': 'mean', 'size': 'sum'})
            print(f'## HISTORICAL TRADE DATA FOR: {stock}')
            for ts, row in minute_trades.dropna().iterrows():
                print(f'{stock} - {ts}: {row["price"]} ~ {row["size"]} shares')
                if ts in db_rows:
                    db_rows[ts]["trade_price"] = row['price']
                    db_rows[ts]["shares"] = row['size']
                else:
                    db_rows[ts] = {
                        "stock": stock,
                        "timestamp": ts,
                        "trade_price": row['price'],
                        "shares": row['size'],
                    }

        # Persist combined bar/quote/trade data for this stock
        if db_rows:
            add_row(list(db_rows.values()))
        
    mongo_client.close()
    menu()

###########################################
def menu():
    user_action = input('Enter action or type "help": ')
    process_action(user_action.lower())

print('#---------#')
print('| WELCOME |')
print('#---------#')
menu()
