import argparse
import signal
import sys
import time

from . import metrics
from .config import METRICS_PORT, REDIS_HOST, REDIS_PORT
from .message_store import MessageStore
from .mqtt_handler import MQTTHandler


def _build_parser() -> argparse.ArgumentParser:
    return argparse.ArgumentParser(
        prog="gc-analysis",
        description=(
            "WIS2 Global Cache Message Analysis\n\n"
            "Subscribes to two MQTT brokers — the WIS2 Global Broker (origin) and a\n"
            "Global Cache — and monitors whether cache messages correctly mirror origin\n"
            "messages. Messages are matched by compound key (data_id + pubtime).\n\n"
            "Origin broker:  mqtts://globalbroker.meteo.fr:8883  topic: origin/a/wis2/#\n"
            "Cache broker:   mqtts://wis2cache.globaldata.nws.noaa.gov:8883  topic: cache/a/wis2/#\n\n"
            "Only messages where notification-type=data and data-policy=core are processed."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Permitted differences between matched origin and cache messages:\n"
            "  - 'id'                      always a unique UUID per message\n"
            "  - 'properties.global-cache' present only in cache messages\n"
            "  - 'links[rel=canonical/update].href'  points to different host\n\n"
            "Prometheus metrics (published at http://0.0.0.0:<metrics-port>/metrics):\n\n"
            "  matched_messages_total{centre_id}\n"
            "    Origin+cache pairs successfully matched by data_id and pubtime.\n\n"
            "  messages_with_illegal_differences_total{centre_id}\n"
            "    Matched pairs containing changes not permitted by the WIS2 spec.\n\n"
            "  missed_messages_total{centre_id}\n"
            "    Origin messages with no cache match received within 600 seconds.\n"
        ),
    )


def main() -> None:
    parser = _build_parser()
    parser.add_argument(
        "--centre-id",
        metavar="CENTRE_ID",
        help="Restrict processing to a specific WIS2 Node centre ID (e.g. ca-eccc-msc). "
             "When set, subscribes to origin/a/wis2/{centre-id}/# and cache/a/wis2/{centre-id}/#.",
    )
    parser.add_argument(
        "--metrics-port",
        type=int,
        default=METRICS_PORT,
        metavar="PORT",
        help=f"Port for the Prometheus metrics HTTP endpoint (default: {METRICS_PORT}).",
    )
    parser.add_argument(
        "--redis-host",
        default=REDIS_HOST,
        metavar="HOST",
        help=f"Redis hostname (default: {REDIS_HOST}).",
    )
    parser.add_argument(
        "--redis-port",
        type=int,
        default=REDIS_PORT,
        metavar="PORT",
        help=f"Redis port (default: {REDIS_PORT}).",
    )
    args = parser.parse_args()

    metrics.start(args.metrics_port)

    try:
        store = MessageStore(host=args.redis_host, port=args.redis_port)
    except Exception:
        sys.exit(1)

    handler = MQTTHandler(store=store, centre_id=args.centre_id)
    handler.start()

    def _shutdown(signum, frame):
        print("\nShutting down...", file=sys.stderr)
        handler.stop()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()
