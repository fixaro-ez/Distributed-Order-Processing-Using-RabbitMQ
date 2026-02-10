import argparse
import json
import os
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
    parser = argparse.ArgumentParser(description="Publish an OrderPlaced event.")
    parser.add_argument("order_id", help="Order ID")
    parser.add_argument("email", help="Customer email address")
    parser.add_argument("item", nargs="?", default="pizza", help="Item name")
    parser.add_argument("quantity", nargs="?", type=int, default=1, help="Item quantity")
    parser.add_argument("notes", nargs="?", default="", help="Optional notes")
    args = parser.parse_args()

    payload = _build_payload(args.order_id, args.email, args.item, args.quantity, args.notes)
    body = json.dumps(payload)

    connection = _connect()
    channel = connection.channel()
    _declare_infrastructure(channel)

    properties = pika.BasicProperties(delivery_mode=2, content_type="application/json")

    channel.basic_publish(exchange=DEFAULT_EXCHANGE, routing_key="email", body=body, properties=properties)
    channel.basic_publish(exchange=DEFAULT_EXCHANGE, routing_key="logger", body=body, properties=properties)

    print(f"[producer] OrderPlaced published: {payload}")

    connection.close()


if __name__ == "__main__":
    main()
