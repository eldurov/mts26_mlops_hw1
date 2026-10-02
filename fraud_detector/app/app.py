import os
import sys
import pandas as pd
import logging
import json
from confluent_kafka import Consumer, Producer

sys.path.append(os.path.abspath('./src'))
from preprocessing import load_train_data, run_preproc
from scorer import make_pred

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TRANSACTIONS_TOPIC = os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions")
SCORING_TOPIC = os.getenv("KAFKA_SCORING_TOPIC", "scoring")

class ProcessingService:
    def __init__(self):
        self.consumer = Consumer({
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'ml-scorer',
            'auto.offset.reset': 'earliest'
        })
        self.consumer.subscribe([TRANSACTIONS_TOPIC])
        self.producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
        self.train = load_train_data()
        logger.info("✅ Сервис инициализирован и готов к обработке сообщений.")

    def process_messages(self):
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue
            
            try:
                data = json.loads(msg.value().decode('utf-8'))
                transaction_id = str(data.get('transaction_id', 'unknown'))
                
                # Поддержка как вложенного формата {"data": {...}}, так и плоского
                if 'data' in data:
                    input_df = pd.DataFrame([data['data']])
                else:
                    input_df = pd.DataFrame([data])
                    if 'transaction_id' in input_df.columns:
                        input_df = input_df.drop(columns=['transaction_id'])

                processed_df = run_preproc(self.train, input_df)
                submission = make_pred(processed_df, "kafka_stream")
                
                result = {
                    'transaction_id': transaction_id,
                    'score': round(float(submission['score'].iloc[0]), 4),
                    'fraud_flag': int(submission['fraud_flag'].iloc[0])
                }
                
                self.producer.produce(SCORING_TOPIC, value=json.dumps(result).encode('utf-8'))
                self.producer.flush()
                logger.info(f"Обработано: {transaction_id} | score={result['score']} | flag={result['fraud_flag']}")
                
            except Exception as e:
                logger.error(f"Ошибка обработки сообщения: {e}")

if __name__ == "__main__":
    service = ProcessingService()
    service.process_messages()