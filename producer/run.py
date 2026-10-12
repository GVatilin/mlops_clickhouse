import argparse
import json
import os

import pandas as pd
from kafka import KafkaProducer


KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
TRANSACTIONS_TOPIC = os.getenv('KAFKA_TRANSACTIONS_TOPIC', 'transactions')
CHUNK_SIZE = 1000
PROGRESS_EVERY = 100000

parser = argparse.ArgumentParser()
parser.add_argument('--path', type=str, required=True)


def read_transactions(path):
    for chunk in pd.read_csv(path, chunksize=CHUNK_SIZE, dtype_backend='numpy_nullable'):
        chunk = chunk.astype(object).where(chunk.notna(), None)
        yield from chunk.to_dict(orient='records')


def send_to_kafka(path):
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
        security_protocol='PLAINTEXT'
    )

    errors = []
    sent = 0
    for transaction in read_transactions(path):
        producer.send(TRANSACTIONS_TOPIC, value=transaction).add_errback(errors.append)
        sent += 1
        if sent % PROGRESS_EVERY == 0:
            print(f'Отправлено сообщений: {sent}', flush=True)

    producer.flush()
    producer.close()

    if errors:
        raise errors[0]

    return sent


if __name__ == '__main__':
    args = parser.parse_args()
    sent = send_to_kafka(args.path)
    print(f'Скрипт успешно отработал, отправлено сообщений: {sent}')
