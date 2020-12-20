# Kafka consumer and producer using Avro for key and value

## Work on it

1. Create a virtual environment


       conda create -n python-kafka-avro python=3.8 autopep8 flake8 jedi more-itertools pytest pytest-cov requests yapf
       conda activate python-kafka-test 
       pip install avro-python3
       pip install confluent-kafka


## Usage

The files

       avro_consumer.sh
       avro_producer.sh

are provided as examples of usage. 

The precondition for them to work is a running Kafka Broker on localhost:9092.

## Reference

https://kafka.apache.org/
https://avro.apache.org/docs/current/gettingstartedpython.html

## Comment

If using Confluent Schema registry, see for instance 
https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/avro_producer.py
