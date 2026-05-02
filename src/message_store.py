import json
import sys
import threading
import time
from datetime import datetime, timezone

import redis as redis_lib

from . import metrics
from .config import EXPIRY_CHECK_INTERVAL_SECONDS, MESSAGE_EXPIRY_SECONDS

_KEY_PREFIX = "wis2gc:origin:"
_EXPIRY_ZSET = "wis2gc:expiry_index"


class MessageStore:
    def __init__(self, host: str, port: int) -> None:
        self._redis = redis_lib.Redis(host=host, port=port, decode_responses=True)
        self._check_connection()
        self._start_expiry_worker()

    def _check_connection(self) -> None:
        try:
            self._redis.ping()
        except redis_lib.ConnectionError as exc:
            print(f"ERROR: Cannot connect to Redis ({self._redis.connection_pool.connection_kwargs}): {exc}", file=sys.stderr)
            raise

    def _compound_key(self, data_id: str, pubtime: str) -> str:
        return f"{data_id}|{pubtime}"

    def store_origin(self, data_id: str, pubtime: str, message: dict, arrival_time: float, centre_id: str) -> None:
        key = self._compound_key(data_id, pubtime)
        payload = json.dumps({"message": message, "arrival_time": arrival_time, "centre_id": centre_id})
        pipe = self._redis.pipeline()
        # TTL has a 30-second buffer so the expiry worker can still GETDEL the key
        pipe.set(f"{_KEY_PREFIX}{key}", payload, ex=MESSAGE_EXPIRY_SECONDS + 30)
        pipe.zadd(_EXPIRY_ZSET, {key: arrival_time + MESSAGE_EXPIRY_SECONDS})
        pipe.execute()

    def get_and_delete_origin(self, data_id: str, pubtime: str) -> dict | None:
        """Atomically retrieve and remove an origin message. Returns None if not found."""
        key = self._compound_key(data_id, pubtime)
        data = self._redis.getdel(f"{_KEY_PREFIX}{key}")
        if data is None:
            return None
        self._redis.zrem(_EXPIRY_ZSET, key)
        return json.loads(data)

    def _expiry_worker(self) -> None:
        while True:
            time.sleep(EXPIRY_CHECK_INTERVAL_SECONDS)
            try:
                now = time.time()
                expired_keys = self._redis.zrangebyscore(_EXPIRY_ZSET, 0, now)
                for key in expired_keys:
                    # GETDEL is atomic: if it returns data the message was unmatched;
                    # if None it was already claimed by a cache message handler.
                    data = self._redis.getdel(f"{_KEY_PREFIX}{key}")
                    self._redis.zrem(_EXPIRY_ZSET, key)
                    if data is None:
                        continue
                    payload = json.loads(data)
                    arrival_str = datetime.fromtimestamp(payload["arrival_time"], tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
                    props = payload["message"].get("properties", {})
                    data_id = props.get("data_id", "unknown")
                    pubtime = props.get("pubtime", "unknown")
                    centre_id = payload.get("centre_id", "unknown")
                    print(
                        f"No matching message from cache found after 600 seconds | "
                        f"origin arrival time: {arrival_str} | "
                        f"data-id: {data_id} | pubtime: {pubtime}",
                        file=sys.stderr,
                    )
                    metrics.missed_messages.labels(centre_id=centre_id).inc()
            except Exception as exc:
                print(f"ERROR in expiry worker: {exc}", file=sys.stderr)

    def _start_expiry_worker(self) -> None:
        thread = threading.Thread(target=self._expiry_worker, daemon=True, name="expiry-worker")
        thread.start()
