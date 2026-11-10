"""Consumer untuk demonstrasi Fan-Out, Topic, dan Wildcard.
Jalankan script ini di terminal terpisah sebelum menjalankan routing_producer.py.
"""
import argparse
from kombu import Connection, Exchange, Queue, Consumer

BROKER = "amqp://guest:guest@localhost:5672//"

def build_queues():
    fanout = Exchange("demo_fanout", type="fanout", durable=True)
    topic = Exchange("demo_topic", type="topic", durable=True)
    return [
        Queue("fanout_queue_a", exchange=fanout, routing_key=""),
        Queue("fanout_queue_b", exchange=fanout, routing_key=""),
        Queue("topic_queue_order", exchange=topic, routing_key="order.*"),
        Queue("topic_queue_payment", exchange=topic, routing_key="payment.*"),
        # Wildcard * cocok tepat satu segmen; # cocok nol atau lebih segmen.
        Queue("wildcard_one_segment", exchange=topic, routing_key="sensor.*"),
        Queue("wildcard_many_segments", exchange=topic, routing_key="sensor.#"),
    ]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=int, default=60,
                        help="Lama consumer aktif dalam detik")
    args = parser.parse_args()

    queues = build_queues()
    with Connection(BROKER) as connection:
        channel = connection.channel()

        def callback(body, message):
            print(
                f"[RECEIVED] queue={message.delivery_info.get('routing_key')} "
                f"body={body}",
                flush=True,
            )
            message.ack()

        with Consumer(channel, queues=queues, callbacks=[callback],
                      accept=["json"], prefetch_count=20):
            print(f"Consumer aktif selama {args.duration} detik...", flush=True)
            import time
            deadline = time.time() + args.duration
            while time.time() < deadline:
                try:
                    connection.drain_events(timeout=1)
                except Exception as exc:
                    # Timeout saat tidak ada pesan adalah normal.
                    if exc.__class__.__name__ not in ("socket.timeout", "TimeoutError"):
                        print(f"[CONSUMER] {exc}", flush=True)

if __name__ == "__main__":
    main()
