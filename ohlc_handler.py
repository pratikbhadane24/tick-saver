import json
import threading
from collections import defaultdict
from datetime import datetime

from database import REDIS_DATA_STORE

# from clickhouse_connect import get_client

time_lower_limit = datetime.now().replace(hour=9, minute=14, second=0, microsecond=0)
time_upper_limit = datetime.now().replace(hour=15, minute=31, second=0, microsecond=0)

# clickhouse_client = get_client(host='localhost', port=8123)

ohlc_snaps = defaultdict(lambda: defaultdict(lambda: {
    "open": None, "high": float("-inf"), "low": float("inf"), "close": None,
    "atp": 0.0, "volume": 0, "oi": 0
}))


# def ensure_table_exists():
#     tables = clickhouse_client.query("SHOW TABLES").result_rows
#     if not any(row[0] == 'ohlc' for row in tables):
#         clickhouse_client.command("""
#                                   CREATE TABLE ohlc
#                                   (
#                                       token     String,
#                                       timestamp DateTime('Asia/Kolkata'),
#                                       open      Float32,
#                                       high      Float32,
#                                       low       Float32,
#                                       close     Float32,
#                                       atp       Nullable(Float32),
#                                       volume    Nullable(UInt64),
#                                       oi        Nullable(UInt64)
#                                   ) ENGINE = ReplacingMergeTree()
#         ORDER BY (token, timestamp)
#                                   """)
#         print("✅ ClickHouse OHLC table created.")
#
#
# ensure_table_exists()


def save_candles_to_storage():
    # ensure_table_exists()
    rows = []
    current_minute = datetime.now().replace(second=0, microsecond=0)

    pipe = REDIS_DATA_STORE.pipeline()
    for token, minutes in list(ohlc_snaps.items()):
        for minute, candle in list(minutes.items()):
            if current_minute <= minute:
                continue
            if candle["open"] == candle["high"] == candle["low"] == candle["close"]:
                del ohlc_snaps[token][minute]
                continue
            if not (time_lower_limit <= current_minute <= time_upper_limit):
                del ohlc_snaps[token][minute]
                print("⚠️ Skipping candle outside trading hours:", token, minute)
                continue
            rows.append((
                token,
                minute,
                candle["open"],
                candle["high"],
                candle["low"],
                candle["close"],
                candle.get("atp"),
                candle.get("volume"),
                candle.get("oi"),
            ))
            pipe.hset(f"MINUTE_CANDLES:{token}", minute.strftime("%H:%M"), json.dumps(candle))
            del ohlc_snaps[token][minute]

        if not ohlc_snaps[token]:
            del ohlc_snaps[token]

    if rows:
        pipe.execute()
        # clickhouse_client.insert(
        #     table='ohlc',
        #     data=rows,
        #     column_names=['token', 'timestamp', 'open', 'high', 'low', 'close', 'atp', 'volume', 'oi']
        # )

        print(f"✅ Inserted {len(rows)} unique candles to REDIS.")
    else:
        print("⚠️ No candles to insert.")


def process_tick(data: dict):
    token = data["token"]
    ltp = data.get("ltp", 0.0)
    atp = data.get("atp", 0.0)
    volume = data.get("volume", 0)
    oi = data.get("oi", 0)
    ts = datetime.fromtimestamp(data["timestamp"])
    minute_key = ts.replace(second=0, microsecond=0)

    if ltp == 0 and atp == 0 and volume == 0 and oi == 0:
        return

    candle = ohlc_snaps[token][minute_key]

    if ltp > 0:
        if candle["open"] is None:
            candle["open"] = ltp
        candle["high"] = max(candle["high"], ltp)
        candle["low"] = min(candle["low"], ltp)
        candle["close"] = ltp

    if atp > 0:
        candle["atp"] = atp
    if volume > 0:
        candle["volume"] = volume
    if oi > 0:
        candle["oi"] = oi


def threaded_save():
    thread = threading.Thread(target=save_candles_to_storage)
    thread.start()
