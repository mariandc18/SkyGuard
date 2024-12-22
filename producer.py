from kafka import KafkaProducer
import json 
import requests

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

API_URL = 'https://opensky-network.org/api/states/all'
def fetch_data():
    try:
            response = requests.get(API_URL)
            if response.status_code == 200:
                data = response.json()
                return data
            else:
                print("Error al intentar obtener los datos")
    except Exception as e:
        print(f'Error: {e}')
        return None 

def send_to_kafka(data):
    if data is not None:
        try:
            producer.send('Air_Traffic', value=data)  
            producer.flush()  
            print("Los datos se han enviado a Kafka exitosamente")
        except Exception as e:
            print(f"Error al enviar los datos a Kafka: {e}")
            
if __name__ == '__main__':
    data = fetch_data()
    send_to_kafka(data)