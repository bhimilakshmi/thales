import argparse
import json
import sys
import time
import socket
from confluent_kafka import Consumer, KafkaError, KafkaException


def msg_process(msg):
    # Print the current time and the message.
    print(msg)
    time_start = time.strftime("%Y-%m-%d %H:%M:%S")
    val = msg.value()
    dval = json.loads(val)
    print(time_start, dval)


def main():
    conf = {'bootstrap.servers': 'localhost:9092',
            'default.topic.config': {'auto.offset.reset': 'earliest'},
            'group.id': socket.gethostname()}

    consumer = Consumer(conf)

    running = True

    try:
        while running:
            consumer.subscribe(topics=["events"])
            msg = consumer.poll(1)
            print(msg)
            msg_process(msg)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


if __name__ == "__main__":
    main()
