import json
import logging
import struct

import six

from database import REDIS_DB_CLIENT

EXCHANGE_MAP = {
    "nse": 1,
    "nfo": 2,
    "cds": 3,
    "bse": 4,
    "bfo": 5,
    "bcd": 6,
    "mcx": 7,
    "mcxsx": 8,
    "indices": 9,
    "bsecds": 6,
}


def fetch_all_tokens_for_zerodha(all_keys_list=None):
    if all_keys_list is None:
        all_keys_list = ["CT:*"]
    all_keys = []
    for keys_to_fetch in all_keys_list:
        keys = REDIS_DB_CLIENT.keys(keys_to_fetch)
        keys = [key.decode('utf-8') for key in keys]
        all_keys.extend(keys)

    data = REDIS_DB_CLIENT.json().mget(all_keys, path='.')
    keys_data = {}
    for key, result in zip(all_keys, data):
        if result is None:
            continue
        keys_data[key] = result

    symbol_list, symbol_dict = [], {}
    for key, value in keys_data.items():
        if value['exchange'] == '' or value['exchange'] == '-' or value['instrument'] == '' or value[
            'instrument'] == '-':
            continue
        instrument = value['instrument']
        if instrument == "OPTCUR" or instrument == "FUTCUR":
            continue
        broker_avail = value['broker_avail']
        if 'Z' not in broker_avail:
            continue

        token = value['zerodha_token']
        token = int(token)
        symbol_list.append(token)
        symbol_dict[token] = key
    print("LENGTH OF SYMBOL LIST", len(symbol_list))
    return symbol_list, symbol_dict


def parse_text_message(payload):
    if not six.PY2 and type(payload) == bytes:
        payload = payload.decode("utf-8")

    try:
        data = json.loads(payload)
    except ValueError:
        return
    logging.info(f"ZERODHA MESSAGE: {data}")


def parse_binary_ticks(bin):
    packets = _split_packets(bin)
    data = []

    for packet in packets:
        token = _unpack_int(packet, 0, 4)
        segment = token & 0xff

        if segment == EXCHANGE_MAP["cds"]:
            divisor = 10000000.0
        elif segment == EXCHANGE_MAP["bcd"]:
            divisor = 10000.0
        else:
            divisor = 100.0

        if len(packet) == 8:
            data.append({
                "token": token,
                "ltp": _unpack_int(packet, 4, 8) / divisor
            })
        elif len(packet) == 28 or len(packet) == 32:
            d = {
                "token": token,
                 "ltp": _unpack_int(packet, 4, 8) / divisor,
                #  "high": _unpack_int(packet, 8, 12) / divisor,
                #  "low": _unpack_int(packet, 12, 16) / divisor,
                #  "open": _unpack_int(packet, 16, 20) / divisor,
                #  "prev_close": _unpack_int(packet, 20, 24) / divisor
            }

            if len(packet) == 32:
                try:
                    timestamp = _unpack_int(packet, 28, 32)
                except Exception:
                    timestamp = None
                d["timestamp"] = timestamp
            data.append(d)

        elif len(packet) == 44 or len(packet) == 184:
            d = {"token": token,
                 "ltp": _unpack_int(packet, 4, 8) / divisor, 
                #  "ltq": _unpack_int(packet, 8, 12),
                 "atp": _unpack_int(packet, 12, 16) / divisor, 
                 "volume": _unpack_int(packet, 16, 20),
                #  "open": _unpack_int(packet, 28, 32) / divisor,
                #  "high": _unpack_int(packet, 32, 36) / divisor,
                #  "low": _unpack_int(packet, 36, 40) / divisor,
                #  "prev_close": _unpack_int(packet, 40, 44) / divisor
                 }

            if len(packet) == 184:
                try:
                    last_trade_time = _unpack_int(packet, 44, 48)
                except Exception:
                    last_trade_time = None
                try:
                    timestamp = _unpack_int(packet, 60, 64)
                except Exception:
                    timestamp = None

                # d["ltt"] = last_trade_time
                d["oi"] = _unpack_int(packet, 48, 52)
                # d["oi_high"] = _unpack_int(packet, 52, 56)
                # d["oi_low"] = _unpack_int(packet, 56, 60)
                d["timestamp"] = timestamp

                # depth = []

                # for i, p in enumerate(range(64, len(packet), 12)):
                #     qty = _unpack_int(packet, p, p + 4)
                #     price = _unpack_int(packet, p + 4, p + 8) / divisor

                #     level = i if i < 5 else i - 5
                #     while len(depth) <= level:
                #         depth.append({
                #             'bid_qty': 0, 'ask_qty': 0,
                #             'bid_price': '0.00', 'ask_price': '0.00'
                #         })
                #     if i < 5:
                #         depth[level]['bid_qty'] = qty
                #         depth[level]['bid_price'] = f"{price:.2f}"
                #     else:
                #         depth[level]['ask_qty'] = qty
                #         depth[level]['ask_price'] = f"{price:.2f}"
                # d['depth'] = depth
            data.append(d)
    return data


def _unpack_int(bin, start, end, byte_format="I"):
    return struct.unpack(">" + byte_format, bin[start:end])[0]


def _split_packets(bin):
    if len(bin) < 2:
        return []
    number_of_packets = _unpack_int(bin, 0, 2, byte_format="H")
    packets = []
    j = 2
    for i in range(number_of_packets):
        packet_length = _unpack_int(bin, j, j + 2, byte_format="H")
        packets.append(bin[j + 2: j + 2 + packet_length])
        j = j + 2 + packet_length
    return packets
