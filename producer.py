from confluent_kafka import Producer
import json
import requests
import time
from concurrent.futures import ThreadPoolExecutor
import requests  

# Configuración del productor de Confluent Kafka
producer_config = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'air_traffic_producer'
}
producer = Producer(producer_config)

API_URL_STATE_VECTORS = 'https://opensky-network.org/api/states/all'
API_URL_FLIGHTS = 'https://opensky-network.org/api/flights/all'
API_URL_PREDICT = 'http://localhost:8000/predict'  


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


def get_delay_from_fastapi(flight_data):
    """Obtiene la predicción del retraso desde el modelo FastAPI"""
    try:
        response = requests.post(API_URL_PREDICT, json=flight_data)
        prediction = response.json()
        return prediction.get('delay', 0)  # Retorna 0 si no se encuentra la predicción
    except Exception as e:
        print(f"[ERROR] Error al obtener la predicción: {e}")
        return 0  # Valor por defecto si hay error


def send_to_kafka(states_data, flights_data, topic='Air_Traffic'):
    """Envía los datos de tráfico aéreo y vuelos a Kafka, incluyendo la predicción de retraso"""
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
        
        # Crear el diccionario con los datos que se enviarán
        flight_data = {
            'icao24': state[0],
            'callsign': state[1],
            'origin_country': state[2],
            'time_position': state[3],
            'last_contact': state[4],
            'latitude': state[6],
            'longitude': state[5],
            'altitude': state[7],
            'velocity': state[9],
            'heading': state[10],
            'vertical_rate': state[11],
            'firstSeen': state[12],
            'lastSeen': state[13],
            'estDepartureAirport': state[14],
            'estArrivalAirport': state[15]
        }
        
        # Obtener la predicción de retraso desde el modelo de FastApi
        delay = get_delay_from_fastapi(flight_data)
        
        # Agregar el campo 'delay' a los datos del vuelo
        state.append(delay)
        
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