#!/usr/bin/env python3
import io
import logging
from argparse import ArgumentParser
from sys import argv
from typing import Any, List

import avro
from avro.datafile import DataFileReader
from confluent_kafka import Consumer

logging.basicConfig(level=logging.INFO)


def from_avro_bytes(b: bytes) -> Any:
    bytes_reader = io.BytesIO(b)
    data_file_reader = DataFileReader(bytes_reader, avro.io.DatumReader())
    return next(t for t in data_file_reader)


def consume(bootstrap_servers: List[str], topic: str, group_id, client_id) -> None:
    settings = {
        'bootstrap.servers': ','.join(bootstrap_servers),
        'group.id': group_id,
        'client.id': client_id,
        'enable.auto.commit': True,
        'session.timeout.ms': 6000,
        'default.topic.config': {'auto.offset.reset': 'smallest'}
    }
    c = Consumer(settings)
    c.subscribe([topic])

    try:
        while True:
            msg = c.poll(0.1)
            if msg is None:
                continue
            elif not msg.error():
                logging.debug(f'Consumed not deserialized: {msg.key()}: {msg.value()}')
                key = from_avro_bytes(msg.key())
                value = from_avro_bytes(msg.value())
                logging.info(f'Consumed: {key}: {value}')
            else:
                logging.error('Failed to consume: {0}'.format(msg.error().str()))
    except KeyboardInterrupt:
        pass
    finally:
        c.close()


def main(bootstrap_servers: List[str], topic: str, group_id, client_id) -> None:
    consume(bootstrap_servers, topic, group_id, client_id)


def parse_args(args):
    parser = ArgumentParser(
        description='Consumes Avro from Kafka topic')
    parser.add_argument(
        'topic',
        help='The Kafka topic',
    )
    parser.add_argument(
        'group_id',
        help='The Kafka group id of this consumer',
    )
    parser.add_argument(
        'client_id',
        help='The Kafka client id of this consumer',
    )
    parser.add_argument(
        '-bs',
        '--bootstrap_servers',
        nargs='*',
        default=['localhost:9092'],
        help='Kafka bootstrap servers, e.g. localhost:9092 localhost:9093',
    )
    return parser.parse_args(args)


if __name__ == '__main__':  # pragma: no cover
    main(**parse_args(argv[1:]).__dict__)
    # main(['localhost:9092'], 'test-topic', 'test-group', 'consumer1')
