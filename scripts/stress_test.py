import argparse
import json
import os
import time
from datetime import datetime

from dotenv import load_dotenv
import pika

load_dotenv()

DEFAULT_EXCHANGE = os.getenv("RABBITMQ_EXCHANGE", "orders.direct")
DEFAULT_HOST = os.getenv("RABBITMQ_HOST", "localhost")
DEFAULT_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
DEFAULT_USER = os.getenv("RABBITMQ_USER", "guest")
DEFAULT_PASS = os.getenv("RABBITMQ_PASS", "guest")
DEFAULT_VHOST = os.getenv("RABBITMQ_VHOST", "/")


def _connect():
    credentials = pika.PlainCredentials(DEFAULT_USER, DEFAULT_PASS)
    params = pika.ConnectionParameters(
        host=DEFAULT_HOST,
        port=DEFAULT_PORT,
        virtual_host=DEFAULT_VHOST,
        credentials=credentials,
        heartbeat=60,
    )
    return pika.BlockingConnection(params)


def _declare_infrastructure(channel):
    channel.exchange_declare(exchange=DEFAULT_EXCHANGE, exchange_type="direct", durable=True)

    channel.queue_declare(queue="email_queue", durable=True)
    channel.queue_declare(queue="logger_queue", durable=True)

    channel.queue_bind(queue="email_queue", exchange=DEFAULT_EXCHANGE, routing_key="email")
    channel.queue_bind(queue="logger_queue", exchange=DEFAULT_EXCHANGE, routing_key="logger")


def _build_payload(order_id, email, item, quantity, notes):
    return {
        "order_id": order_id,
        "email": email,
        "item": item,
        "quantity": quantity,
        "notes": notes,
        "placed_at": datetime.utcnow().isoformat() + "Z",
    }


def main():
    parser = argparse.ArgumentParser(description="Stress test publisher for OrderPlaced events.")
    parser.add_argument("--count", type=int, default=1000, help="Total messages to publish")
    parser.add_argument("--rate", type=float, default=0.0, help="Messages per second (0 = max speed)")
    parser.add_argument("--order-prefix", default="order", help="Order ID prefix")
    parser.add_argument("--email", default="stress@example.com", help="Customer email address")
    parser.add_argument("--item", default="pizza", help="Item name")
    parser.add_argument("--quantity", type=int, default=1, help="Item quantity")
    parser.add_argument("--notes", default="", help="Optional notes")
    args = parser.parse_args()

    connection = _connect()
    channel = connection.channel()
    _declare_infrastructure(channel)

    properties = pika.BasicProperties(delivery_mode=2, content_type="application/json")
    interval = 0.0 if args.rate <= 0 else 1.0 / args.rate
    sent = 0
    start = time.time()

    for i in range(args.count):
        order_id = f"{args.order_prefix}-{i + 1}"
        payload = _build_payload(order_id, args.email, args.item, args.quantity, args.notes)
        body = json.dumps(payload)

        channel.basic_publish(exchange=DEFAULT_EXCHANGE, routing_key="email", body=body, properties=properties)
        channel.basic_publish(exchange=DEFAULT_EXCHANGE, routing_key="logger", body=body, properties=properties)
        sent += 1

        if interval:
            time.sleep(interval)

    elapsed = time.time() - start
    rate = sent / elapsed if elapsed > 0 else sent
    print(f"[stress] Published {sent} messages in {elapsed:.2f}s ({rate:.1f} msg/s)")

    connection.close()


if __name__ == "__main__":
    main()
