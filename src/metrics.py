from prometheus_client import Counter, start_http_server

matched_messages = Counter(
    "matched_messages_total",
    "Origin and cache message pairs successfully matched by data_id and pubtime",
    ["centre_id"],
)

illegal_differences = Counter(
    "messages_with_illegal_differences_total",
    "Matched origin and cache message pairs containing changes not permitted by the WIS2 spec",
    ["centre_id"],
)

missed_messages = Counter(
    "missed_messages_total",
    "Origin messages for which no matching cache message was received within 600 seconds",
    ["centre_id"],
)


def start(port: int = 8000) -> None:
    start_http_server(port)
    print(f"Metrics server listening on port {port}")
