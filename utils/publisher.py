from redis import Redis
import json
from hashlib import blake2b
from collections import defaultdict

class MarketDataUpdater:
    """
    All fields (including depth) stored in a Redis HASH.
    Depth is serialized as a JSON string.
    Publishes actual changed values (except depth) to avoid redis fetch on readers.
    """
    def __init__(
            self,
            redis_client: Redis,
            threshold_ratio: float = 0.66,
            publish_channel: str = "TICK:CHANGES"
    ):
        self.redis = redis_client
        self.threshold = int(15 * threshold_ratio)
        self.hash_cache = defaultdict(dict)
        self.initialized = set()
        self.publish_channel = publish_channel

    @staticmethod
    def hash_field(value) -> str:
        if isinstance(value, (dict, list)):
            s = json.dumps(value, separators=(",", ":"), sort_keys=True)
        else:
            s = str(value)
        return blake2b(s.encode(), digest_size=4).hexdigest()
    def update_commands(self, pipe: Redis.pipeline, symbol: str, new_data: dict):
        key = f"TICKS:{symbol}"
        prev = self.hash_cache[symbol]
        new_hashes = {}
        changed = {}

        # 1) Detect changed fields
        for field, raw_val in new_data.items():
            h = self.hash_field(raw_val)
            new_hashes[field] = h
            if prev.get(field) != h:
                val = raw_val if field != "depth" else json.dumps(raw_val, separators=(",", ":"), sort_keys=True)
                changed[field] = val

        cnt = len(changed)
        is_new = symbol not in self.initialized
        update_type = None

        # 2) FULL replace
        if is_new or cnt >= self.threshold:
            mapping = {}
            for field, raw_val in new_data.items():
                mapping[field] = raw_val if field != "depth" else json.dumps(raw_val, separators=(",", ":"), sort_keys=True)
            pipe.hset(key, mapping=mapping)

            self.hash_cache[symbol] = new_hashes.copy()
            self.initialized.add(symbol)
            update_type = "full"

        # 3) PARTIAL patch
        elif cnt > 0:
            pipe.hset(key, mapping=changed)
            for f in changed:
                prev[f] = new_hashes[f]
            update_type = "partial"
        else:
            return {}, None

        msg = {
            "symbol": symbol,
            "full_replace": (update_type == "full")
        }

        if update_type == "partial":
            msg["changes"] = {
                k: v for k, v in changed.items()
                if k != "depth"
            }
            if not msg['changes']:
                msg = {}
        if msg:
            pipe.publish(self.publish_channel, json.dumps(msg))
        return changed, msg
