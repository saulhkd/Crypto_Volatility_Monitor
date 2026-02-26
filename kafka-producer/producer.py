import time
import json
import requests
from kafka import KafkaProducer
import os

# Configuración (Leyendo variables de entorno)
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:29092')
TOPIC = 'crypto_prices'
API_URL = 'https://api.coincap.io/v2/assets'

def create_producer():
    # Esperamos un poco a que Kafka arranque completamente
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
            print("✅ Conectado a Kafka exitosamente")
            return producer
        except Exception as e:
            print(f"⚠️ Esperando a Kafka... ({e})")
            time.sleep(5)

def get_crypto_data():
    try:
        response = requests.get(API_URL)
        if response.status_code == 200:
            data = response.json()['data']
            # Filtramos solo Bitcoin y Ethereum para no saturar
            target_coins = ['bitcoin', 'ethereum', 'solana']
            return [coin for coin in data if coin['id'] in target_coins]
        else:
            print(f"Error API: {response.status_code}")
            return []
    except Exception as e:
        print(f"Error Request: {e}")
        return []

if __name__ == '__main__':
    producer = create_producer()
    
    while True:
        cryptos = get_crypto_data()
        
        for coin in cryptos:
            # Enviamos el mensaje a Kafka
            producer.send(TOPIC, value=coin)
            print(f"🚀 Enviado: {coin['id']} - ${coin['priceUsd']}")
        
        # Forzar el envío inmediato
        producer.flush()
        
        # Esperar 10 segundos antes de la siguiente consulta
        time.sleep(10)