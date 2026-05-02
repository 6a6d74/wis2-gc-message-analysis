import json
import ssl
import sys
import time
from datetime import datetime, timezone
from urllib.parse import urlparse

import paho.mqtt.client as mqtt

from . import comparator, metrics
from .config import CACHE_BROKER_URL, ORIGIN_BROKER_URL
from .message_store import MessageStore


def _parse_topic(topic: str) -> dict | None:
    """Parse a WIS2 topic into its components.

    Expected format: {channel}/a/wis2/{centre-id}/{notification-type}/{data-policy}/...
    Returns None if the topic does not match the expected structure.
    """
    parts = topic.split("/")
    if len(parts) < 6:
        return None
    return {
        "channel": parts[0],
        "centre_id": parts[3],
        "notification_type": parts[4],
        "data_policy": parts[5],
    }


class MQTTHandler:
    def __init__(self, store: MessageStore, centre_id: str | None = None) -> None:
        self._store = store
        self._centre_id = centre_id

        if centre_id:
            self._origin_topic = f"origin/a/wis2/{centre_id}/#"
            self._cache_topic = f"cache/a/wis2/{centre_id}/#"
        else:
            self._origin_topic = "origin/a/wis2/#"
            self._cache_topic = "cache/a/wis2/#"

        self._origin_client = self._build_client(ORIGIN_BROKER_URL, "gc-analysis-origin", self._origin_topic, self._on_origin_message)
        self._cache_client = self._build_client(CACHE_BROKER_URL, "gc-analysis-cache", self._cache_topic, self._on_cache_message)

    def _build_client(self, broker_url: str, client_id: str, topic: str, on_message) -> mqtt.Client:
        parsed = urlparse(broker_url)

        client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id,
        )
        client.username_pw_set(parsed.username, parsed.password)
        client.tls_set(cert_reqs=ssl.CERT_REQUIRED)
        client.reconnect_delay_set(min_delay=1, max_delay=120)

        def on_connect(c, userdata, connect_flags, reason_code, properties):
            if reason_code.is_failure:
                print(f"ERROR: {client_id} failed to connect to {parsed.hostname}: {reason_code}", file=sys.stderr)
            else:
                print(f"{client_id} connected to {parsed.hostname}; subscribing to {topic}")
                c.subscribe(topic, qos=1)

        def on_disconnect(c, userdata, disconnect_flags, reason_code, properties):
            if reason_code.value != 0:
                print(f"WARNING: {client_id} disconnected unexpectedly: {reason_code}", file=sys.stderr)

        client.on_connect = on_connect
        client.on_disconnect = on_disconnect
        client.on_message = on_message

        client.connect_async(parsed.hostname, parsed.port or 8883)
        return client

    def start(self) -> None:
        print(f"Connecting to origin broker, topic: {self._origin_topic}")
        print(f"Connecting to cache broker,  topic: {self._cache_topic}")
        self._origin_client.loop_start()
        self._cache_client.loop_start()

    def stop(self) -> None:
        self._origin_client.loop_stop()
        self._cache_client.loop_stop()

    def _on_origin_message(self, client, userdata, message) -> None:
        topic_info = _parse_topic(message.topic)
        if not topic_info:
            return
        if topic_info["notification_type"] != "data" or topic_info["data_policy"] != "core":
            return

        try:
            payload = json.loads(message.payload)
        except json.JSONDecodeError as exc:
            print(f"ERROR: Invalid JSON in origin message on {message.topic}: {exc}", file=sys.stderr)
            return

        props = payload.get("properties", {})
        data_id = props.get("data_id")
        pubtime = props.get("pubtime")
        if not data_id or not pubtime:
            print(f"ERROR: Missing data_id or pubtime in origin message on {message.topic}", file=sys.stderr)
            return

        self._store.store_origin(
            data_id=data_id,
            pubtime=pubtime,
            message=payload,
            arrival_time=time.time(),
            centre_id=topic_info["centre_id"],
        )

    def _on_cache_message(self, client, userdata, message) -> None:
        topic_info = _parse_topic(message.topic)
        if not topic_info:
            return
        if topic_info["notification_type"] != "data" or topic_info["data_policy"] != "core":
            return

        try:
            payload = json.loads(message.payload)
        except json.JSONDecodeError as exc:
            print(f"ERROR: Invalid JSON in cache message on {message.topic}: {exc}", file=sys.stderr)
            return

        props = payload.get("properties", {})
        data_id = props.get("data_id")
        pubtime = props.get("pubtime")
        if not data_id or not pubtime:
            print(f"ERROR: Missing data_id or pubtime in cache message on {message.topic}", file=sys.stderr)
            return

        cache_arrival_time = time.time()
        centre_id = topic_info["centre_id"]
        origin_data = self._store.get_and_delete_origin(data_id, pubtime)

        if origin_data is None:
            arrival_str = datetime.fromtimestamp(cache_arrival_time, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
            print(
                f"No matching message from origin found, discarding message from cache | "
                f"cache arrival time: {arrival_str} | "
                f"data-id: {data_id} | pubtime: {pubtime}",
                file=sys.stderr,
            )
            return

        origin_arrival_time = origin_data["arrival_time"]
        origin_message = origin_data["message"]
        arrival_diff = int(cache_arrival_time - origin_arrival_time)

        print(
            f"Matching messages from origin and cache found | "
            f"arrival time difference: {arrival_diff} | "
            f"data-id: {data_id} | pubtime: {pubtime}"
        )
        metrics.matched_messages.labels(centre_id=centre_id).inc()

        allowed_only, diff_text = comparator.compare(origin_message, payload)
        if allowed_only:
            print("Messages contain permitted differences")
        else:
            metrics.illegal_differences.labels(centre_id=centre_id).inc()
            print("Messages contain illegal differences")
            print(diff_text)
