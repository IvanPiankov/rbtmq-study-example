import logging
import random
import time
from datetime import datetime

from config import get_connection, MQ_ROUTING_KEY, MQ_EXCHANGE
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import BasicProperties, Basic


def process_message(
        channel: BlockingChannel,
        method: Basic.Deliver,
        properties: BasicProperties,
        body: bytes
) -> None:
    print(f"Start consuming message: {datetime.now().isoformat()}")
    print(f"Message: {body}")
    time.sleep(1)
    print(f"Finish consuming message: {datetime.now().isoformat()}")
    channel.basic_ack(delivery_tag=method.delivery_tag)
    print(f"Set message as ack: {datetime.now().isoformat()}")




def consume_message(channel: BlockingChannel) -> None:
    channel.basic_consume(
            queue=MQ_ROUTING_KEY,
            on_message_callback=process_message,
    )
    channel.start_consuming()


def main():
    # Open TCP connection
    with get_connection() as connection:
        logging.info("Created connection")
        with connection.channel() as channel:
            consume_message(channel)

    # while True:
    #     pass



if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.warning("Finish Program !")