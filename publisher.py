import logging
import random
import time

from config import get_connection, MQ_ROUTING_KEY, MQ_EXCHANGE
from pika.adapters.blocking_connection import BlockingChannel

def publish_message(channel: BlockingChannel, idx: int) -> None:
    msg = f"Hello World, idx: #{idx:02d} with numb - {random.randint(3, 9)}"
    channel.basic_publish(
            exchange=MQ_EXCHANGE,
            routing_key=MQ_ROUTING_KEY,
            body=msg
    )


def main():
    # Open TCP connection
    with get_connection() as connection:
        logging.info("Created connection")
        with connection.channel() as channel:
            channel.queue_declare(queue=MQ_ROUTING_KEY)
            for idx in range(1, 10):
                time.sleep(0.5)
                publish_message(channel, idx)

    while True:
        pass



if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.warning("Finish Program !")