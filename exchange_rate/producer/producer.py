from confluent_kafka import Producer
import currencyapicom
import os
from dotenv import load_dotenv 
import requests
from datetime import datetime, timedelta
import uuid
import time
import json

def generate_unique_id():
    """
    Gera um ID único para utilizar no Kafka Producer
    """
    return int(str(uuid.uuid4().int)[:8])

def set_producer():
    """
    Seta as configurações do Producer, utilizando o .env
    """
    # Configuração do Producer
    producer_conf = {
        'bootstrap.servers': os.environ['BOOTSTRAP_SERVERS'],
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': os.environ['SASL_USERNAME'],
        'sasl.password': os.environ['SASL_PASSWORD'],
        'client.id': os.environ['CLIENT_ID']
    }

    producer = Producer(producer_conf)
    return producer

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def get_currencies(date) -> dict: 
    """
    Coleta a API_KEY do arquivo '.env' e retorna as currencies exchanges da API. 
    Moeda contra o valor do Real, exemplo: Euro frente ao Real
    """
    client = currencyapicom.Client(os.getenv('API_KEY'))
    result = client.historical(date,currencies=['EUR','USD'], base_currency='BRL')
    return result

def date_range(initial_date):
    """
    Gera as datas da Initial_Date até a Final_Date
    """
    # Yesterday date
    final_date = datetime.now() - timedelta(days=1)

    # List of Dates
    dates = []

    # Converting initial date from string to datetime 
    initial_date = datetime.strptime(initial_date, '%Y-%m-%d')

    # Generate and append de Dates to list
    while initial_date <= final_date:
        dates.append(initial_date.strftime('%Y-%m-%d'))
        initial_date += timedelta(days=1)

    return dates

if __name__ == '__main__': 
    load_dotenv()   # Carrega as variáveis do .env
    topic = os.environ['TOPIC']

    producer_id = generate_unique_id()
    producer = set_producer()

    initial_date = '2024-10-31'
    dates = date_range(initial_date)

    for date in dates: 
        api_result = get_currencies(date)
        
        # Collect date
        collect_date = api_result['meta']['last_updated_at']
        # Exchange Rate EUR to BRL
        eur_brl = 1/api_result['data']['EUR']['value']
        # Exchange Rate USD to BRL
        usd_brl = 1/api_result['data']['USD']['value']
        # Create a dict
        dict_exr = {'id': producer_id,'date': collect_date, 'exr_usd_brl': usd_brl, 'exr_eur_brl': eur_brl}

        # Send messages to Kafka Broker
        producer.produce(topic, key=str(producer_id), value=json.dumps(dict_exr), callback=delivery_report)
        producer.poll(0)
        time.sleep(6) # Garantindo 60seg. A API tem limite de 10reqs/min