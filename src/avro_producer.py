#!/usr/bin/env python3
import io
import json
import logging
from argparse import ArgumentParser
from os import PathLike
from pathlib import Path
from sys import argv
from typing import Callable, Union, Dict, Any, List
from uuid import uuid4

import avro
from avro.datafile import DataFileWriter
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO)


def produce_callback(err, msg):
    if err is not None:
        logging.error(f"Failed to produce: {msg.key()}: {msg.value()}, {err.str()}")
    else:
        logging.debug(f"Produced: {msg.key()}: {msg.value()}")


def to_avro_bytes(schema: str, obj: Union[str, Dict]) -> bytes:
    buf = io.BytesIO()
    data_file_writer = DataFileWriter(buf, avro.io.DatumWriter(), schema)
    data_file_writer.append(obj)
    data_file_writer.flush()
    buf.seek(0)
    the_bytes = buf.read()
    buf.close()
    return the_bytes


def produce(bootstrap_servers: str, topic: str, key_bytes: bytes, value_bytes: bytes,
            callback: Callable = None) -> None:
    p = Producer({'bootstrap.servers': bootstrap_servers})
    p.produce(topic, value_bytes, key=key_bytes, callback=callback)
    p.poll(0.5)
    p.flush(30)


def parse_schema(schema: Union[str, PathLike]) -> Any:
    if isinstance(schema, str):
        return avro.schema.parse(schema)
    else:
        with open(schema, 'rt', encoding='utf-8') as f:
            return avro.schema.parse(f.read())


def main(bootstrap_servers: List[str], topic: str, value_schema_path: str, value: str, key_schema_path: str = None,
         key: Any = None) -> None:
    key_schema = parse_schema(Path(key_schema_path)) if key_schema_path else parse_schema('{"type": "string"}')
    key = key if key else str(uuid4())

    logging.info(f"Producing: {key}: {value}")

    value_schema = parse_schema(Path(value_schema_path))

    value_dict = json.loads(value)
    produce(','.join(bootstrap_servers), topic, to_avro_bytes(key_schema, key), to_avro_bytes(value_schema, value_dict),
            callback=produce_callback)


def parse_args(args):
    parser = ArgumentParser(
        description='Produces Avro to Kafka topic')
    parser.add_argument(
        'topic',
        help='The Kafka topic',
    )
    parser.add_argument(
        'value_schema_path',
        help='Path to Avro schema for Kafka value',
    )
    parser.add_argument(
        'value',
        help='Kafka value',
    )
    parser.add_argument(
        '-ks',
        '--key_schema_path',
        default=None,
        help='Path to Avro schema for Kafka key, defaults to: {"type": "string"}',
    )
    parser.add_argument(
        '-k',
        '--key',
        default=None,
        help='Kafka key, defaults to: str(uuid4())',
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
