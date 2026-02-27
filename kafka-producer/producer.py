import time
import json
import requests
from kafka import KafkaProducer
import os

# Configuración (Leyendo variables de entorno)
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:29092')
TOPIC = 'crypto_prices'
API_KEY = os.getenv('API_KEY')
API_URL = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest'

def create_producer():
    # Esperamos un poco a que Kafka arranque completamente
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
            print("Conectado a Kafka exitosamente")
            return producer
        except Exception as e:
            print(f"Esperando a Kafka... ({e})")
            time.sleep(5)

def get_crypto_data():
    parameters = {
      'symbol':'BTC,ETH,SOL',
      'convert':'USD'
    }
    try:
        response = requests.get(API_URL, headers={'X-CMC_PRO_API_KEY': API_KEY}, params=parameters)
        if response.status_code == 200:
            data = response.json()['data']
            clean_data = []
            # Usamos .values() para iterar sobre los valores del diccionario, que son los objetos de las criptomonedas
            for coin_info in data.values():
                clean_data.append({
                    'id': coin_info['slug'],               # Ej: 'bitcoin'
                    'symbol': coin_info['symbol'],         # Ej: 'BTC'
                    'priceUsd': coin_info['quote']['USD']['price'], # Buscamos el precio anidado
                    'last_updated': coin_info['quote']['USD']['last_updated']
                })
            
            return clean_data
            
        else:
            # Añadimos response.text para ver exactamente por qué se queja la API si falla
            print(f"Error API: {response.status_code} - {response.text}")
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
            print(f"Enviado: {coin['id']} - ${coin['priceUsd']}")
        
        # Forzar el envío inmediato
        producer.flush()
        
        # Esperar 30 segundos antes de la siguiente consulta
        time.sleep(30)