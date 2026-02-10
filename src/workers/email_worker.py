import json
import os
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
    channel.queue_bind(queue="email_queue", exchange=DEFAULT_EXCHANGE, routing_key="email")


def _render_receipt(payload):
    return (
        "<html>"
        "<body>"
        f"<h2>Thanks for your order, {payload['email']}!</h2>"
        f"<p>Order ID: {payload['order_id']}</p>"
        f"<p>Item: {payload['quantity']} x {payload['item']}</p>"
        f"<p>Notes: {payload['notes'] or 'None'}</p>"
        f"<p>Placed at: {payload['placed_at']}</p>"
        "</body>"
        "</html>"
    )


def main():
    connection = _connect()
    channel = connection.channel()
    _declare_infrastructure(channel)

    def handle_message(ch, method, properties, body):
        payload = json.loads(body)
        html = _render_receipt(payload)
        print("[email] Sending receipt")
        print(html)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue="email_queue", on_message_callback=handle_message)

    print("[email] Waiting for messages. Press CTRL+C to exit.")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    finally:
        connection.close()


if __name__ == "__main__":
    main()
