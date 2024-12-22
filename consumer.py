from kafka import KafkaConsumer
import json
import time
import os
import signal
import sys

consumer = KafkaConsumer(
    'Air_Traffic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

DATA_LAKE_DIR = "./Trafico_Aereo"
def save_to_local(data, file_name="traffic_data.json"):
    os.makedirs(DATA_LAKE_DIR, exist_ok=True)
    file_path = os.path.join(DATA_LAKE_DIR, file_name)
    try:
        with open(file_path, 'a') as f:
            for record in data: 
                json.dump(record, f)
                f.write('\n')  
        print(f"Datos guardados en {file_path}")
    except Exception as e:
        print(f"Error al guardar datos: {e}")

def signal_handler(sig, frame):
    print("\nInterrupción detectada. Cerrando consumidor...")
    consumer.close()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

if __name__ == "__main__":
    buffer = []
    buffer_size = 10
    timeout = 5  
    print("Esperando mensajes de Kafka...")
    
    last_flush_time = time.time()
    try:
        while True: 
            message = consumer.poll(timeout_ms=1000)  
            if message:
                for tp, messages in message.items():
                    for msg in messages:
                        buffer.append(msg.value)
                        print(f'Mensaje recibido: {msg.value}')

                if len(buffer) >= buffer_size or (time.time() - last_flush_time > timeout):
                    save_to_local(buffer)
                    buffer = [] 
                    last_flush_time = time.time()

    except Exception as e:
        print(f"Error en el consumidor: {e}")
    finally:
        consumer.close()
