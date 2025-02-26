from kafka import KafkaProducer
import json
import requests
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

# Configuración de Kafka
producer = KafkaProducer(
    bootstrap_servers=['0.0.0.0:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# URLs de la API de OpenSky
API_URL_STATE_VECTORS = 'https://opensky-network.org/api/states/all'
API_URL_FLIGHTS = 'https://opensky-network.org/api/flights/all'


def fetch_data():
    """Obtiene los datos de vectores de estado de la API de OpenSky."""
    try:
        response = requests.get(API_URL_STATE_VECTORS)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error al obtener datos de estado: {response.status_code}")
            return None
    except Exception as e:
        print(f'Error al obtener vectores de estado: {e}')
        return None

MAX_BATCH_SIZE = 3000  # Cantidad máxima de estados por mensaje

def fetch_all_flight_data(start_time, end_time):
    """Obtiene todos los vuelos en un rango de tiempo."""
    url = f'{API_URL_FLIGHTS}?begin={start_time}&end={end_time}'
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error {response.status_code} al obtener vuelos: {response.text}")
            return None
    except Exception as e:
        print(f'Error al obtener datos de vuelos: {e}')
        return None



def send_to_kafka(data, topic):
    """Envía los datos a Kafka en el tópico especificado."""
    if data is None:
        print(f"[WARNING] No hay datos para enviar al tópico '{topic}'")
        return

    try:
        # Si los datos son de estados y son demasiado grandes, dividirlos
        if topic == 'Air_Traffic_States' and "states" in data:
            total_states = len(data["states"])
            print(f"[INFO] Enviando {total_states} estados en lotes de {MAX_BATCH_SIZE}")

            for i in range(0, total_states, MAX_BATCH_SIZE):
                batch = {
                    "time": data["time"],
                    "states": data["states"][i:i + MAX_BATCH_SIZE]
                }
                producer.send(topic, value=batch)
                producer.flush()

            print(f"[INFO] Todos los estados enviados en {total_states // MAX_BATCH_SIZE + 1} lotes.")

        else:
            # Enviar toda la lista como un solo mensaje JSON
            producer.send(topic, value=data)
            producer.flush()
            print(f"[INFO] Datos enviados al tópico '{topic}' exitosamente.")

    except Exception as e:
        print(f"[ERROR] No se pudo enviar datos a Kafka: {e}")



def fetch_and_send_parallel(a):
    """Obtiene datos de ambas APIs en paralelo y los envía a Kafka."""
    start=int(time.time())
    while True:
        # Definir el rango de tiempo de los últimos 10 minutos
        end_time = int(time.time())
        start_time = end_time - 7200 
        with ThreadPoolExecutor(max_workers=3) as executor:
            # Obtener los datos en paralelo
            if a:
                future_flights_recent = executor.submit(fetch_all_flight_data, start_time, end_time)
                flights_recent = future_flights_recent.result()
                a=False
                if flights_recent:
                    executor.submit(send_to_kafka, flights_recent, 'Air_Traffic_Flights')
            elif start+7200<=end_time:
                future_flights_recent = executor.submit(fetch_all_flight_data, start_time, end_time)
                flights_recent = future_flights_recent.result()
                if flights_recent:
                    executor.submit(send_to_kafka, flights_recent, 'Air_Traffic_Flights')
            future_states = executor.submit(fetch_data)
            states = future_states.result()
            
            # Enviar los datos a Kafka
            if states:
                executor.submit(send_to_kafka, states, 'Air_Traffic_States')

        # Espera 10 segundos antes de la siguiente iteración
        time.sleep(10)


if __name__ == '__main__':
    # Permitir al usuario ingresar una fecha específica
    try:
            a=True
            fetch_and_send_parallel(a)
    except ValueError:
            print("Formato de fecha inválido. Se ejecutará sin fecha específica.")
            fetch_and_send_parallel()

