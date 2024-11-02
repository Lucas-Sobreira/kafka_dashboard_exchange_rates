import time
from confluent_kafka import Consumer, KafkaError
import json
import os
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# Consumer Settings
def load_consumer_settings(): 
    """
    Seta as configurações do Consumer, utilizando o .env
    """
    consumer_conf = {
        'bootstrap.servers': os.environ['BOOTSTRAP_SERVERS'],
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': os.environ['SASL_USERNAME'],
        'sasl.password': os.environ['SASL_PASSWORD'],
        'group.id': 'exchange_rates_consumer_group',
        'auto.offset.reset': 'earliest'
    }

    topic_name = os.environ['TOPIC']

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic_name])

    return consumer

# PostgreSQL Settings
def load_postgres_settings(): 
    """
    Carrega as configurações a partir de variáveis de ambiente.
    """
    settings = {
        "db_host": os.environ["POSTGRES_HOST"],
        "db_user": os.environ["POSTGRES_USER"],
        "db_pass": os.environ["POSTGRES_PASSWORD"],
        "db_name": os.environ["POSTGRES_DB"],
        "db_port": os.environ["POSTGRES_PORT"],
    }
    return settings

# Postgres String Connection
def postgres_connection():
    """
    Cria a string de conexão com o Banco de Dados Postgres do Render
    """
    settings = load_postgres_settings()
    connection_string = f"postgresql://{settings['db_user']}:{settings['db_pass']}@{settings['db_host']}:{settings['db_port']}/{settings['db_name']}"
    return connection_string

# Consumer Data Function
def consume_messages(consumer):
    """
    Consome os dados do Broker Kafka e as armazena no Banco de Dados Postgres do Render.
    """
    batch_size = 2  # Number of messages to process each batch
    batch_interval = 5  # Time interval (seconds) between the batches
    messages = []
    start_time = time.time()  # Initialize start_time inside the function

    try:
        while True:
            print('Esperando mensagem')
            msg = consumer.poll(1.0)  # Wait until 1 second for each new message
            if msg is None:
                continue
            if msg.error():
                continue
            
            value = json.loads(msg.value().decode('utf-8'))  # Convert the value to JSON format
            # Denormalize the message
            data = {
                'uuid': value['id'],
                'exr_date': value['date'],
                'exr_usd_brl': value['exr_usd_brl'],
                'exr_eur_brl': value['exr_eur_brl'],
                'run_time': datetime.now()  # Currently time of message
            }
            messages.append(data)

            print(f"printando mensagens: {messages}")

            # Process the batch when reach the size or time interval
            if len(messages) >= batch_size or (time.time() - start_time) >= batch_interval:
                df = pd.DataFrame(messages)
                df.to_sql('exchange_rates_kafka', engine, if_exists='append', index=False)
                messages = []  # Clean the list of messages after to save in DB (Postgres)
                start_time = time.time()  # Reset the time counter

    except KeyboardInterrupt:
        pass
    finally:
        print("Closing consumer.")
        consumer.close()

if __name__ == "__main__":
    # Postgres Render String Connection
    SQLALCHEMY_DATABASE_URL = postgres_connection()
    engine = create_engine(SQLALCHEMY_DATABASE_URL)

    # Read the data from Confluent Kafka and Load to Postgres Render
    consumer = load_consumer_settings()
    consume_messages(consumer)