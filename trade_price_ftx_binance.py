import arrow
import ccxt


def get_historical_prices(contract, start, finish=None, timeframe='1m', exchange='ftx'):
    """Return historical prices for exchanges"""
    accepted_limit = {'ftx': 5000, 'binance': 500}[exchange]
    interval = {'15s': 15, '1m': 60, '5m': 300, '15m': 900, '1h': 3600, '4h': 14400, '1d': 86400}[timeframe]
    exchange_api = {
        'ftx': ccxt.ftx({
            'enableRateLimit': True,
            'timeout': 20000,
        }),
        'binance': ccxt.binance()
    }[exchange]

    results = []
    start = arrow.get(start)
    finish = finish and arrow.get(finish) or arrow.get()
    while start < finish:
        since = start.timestamp() * 1000
        checkpoint = start.shift(seconds=accepted_limit * interval)
        checkpoint = checkpoint if checkpoint < finish else finish
        limit = int((checkpoint - start).total_seconds() / interval)
        x = exchange_api.fetch_ohlcv(contract, since=since, limit=limit, timeframe=timeframe)
        for i in x:
            results.append([arrow.get(i[0]).isoformat(), i[4]])

        start = checkpoint
    return results

get_historical_prices('BTC-PERP', '2021-03-01')
