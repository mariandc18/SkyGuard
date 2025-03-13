from confluent_kafka import Producer
import json
import requests
import time
from concurrent.futures import ThreadPoolExecutor

# Configuración del productor de Confluent Kafka
producer_config = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'air_traffic_producer'
}
producer = Producer(producer_config)

API_URL_STATE_VECTORS = 'https://opensky-network.org/api/states/all'
API_URL_FLIGHTS = 'https://opensky-network.org/api/flights/all'


def fetch_data():
    """Obtiene datos de tráfico aéreo y vuelos desde OpenSky Network."""
    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            future_states = executor.submit(requests.get, API_URL_STATE_VECTORS)
            future_flights = executor.submit(requests.get, API_URL_FLIGHTS, {"begin": int(time.time()) - 7200, "end": int(time.time())})
            
            response_states = future_states.result()
            response_flights = future_flights.result()
            
            if response_states.status_code == 200 and response_flights.status_code == 200:
                return response_states.json(), response_flights.json()
            else:
                print(f"[ERROR] Estado {response_states.status_code} o {response_flights.status_code} al obtener datos")
    except Exception as e:
        print(f"[ERROR] Error al obtener datos: {e}")
    return None, None


def send_to_kafka(states_data, flights_data, topic='Air_Traffic'):
    """Envía los datos de tráfico aéreo y vuelos a Kafka."""
    if not states_data or "states" not in states_data:
        print(f"[WARNING] No hay datos de estado para enviar al tópico '{topic}'")
        return
    
    flights_dict = {vuelo.get("icao24"): vuelo for vuelo in flights_data} if flights_data else {}
    
    for state in states_data["states"]:
        flight_info = flights_dict.get(state[0], {})
        state.extend([
            flight_info.get("firstSeen"),
            flight_info.get("lastSeen"),
            flight_info.get("estDepartureAirport"),
            flight_info.get("estArrivalAirport")
        ])
        message = json.dumps(state).encode('utf-8')
        producer.produce(topic, value=message)
    
    producer.flush()
    print("[INFO] Datos enviados a Kafka exitosamente")


if __name__ == '__main__':
    while True:
        states, flights = fetch_data()
        if states:
            send_to_kafka(states, flights)
        time.sleep(10)
