from confluent_kafka import Consumer
from elasticsearch import Elasticsearch
import json
import time
import os
import signal
import sys
from hdfs import InsecureClient
from datetime import datetime, timezone
from rich.console import Console
from rich.table import Table
from rich.live import Live

# Configuración de Elasticsearch
ES_HOST = "http://localhost:9200"
INDEX_NAME = "air_traffic"

es = Elasticsearch([ES_HOST])
if not es.indices.exists(index=INDEX_NAME):
    mapping = {
        "mappings": {
            "properties": {
                "location": {
                    "type": "geo_point"
                },
                "delay": {
                    "type": "integer"
                }
            }
        }
    }
    es.indices.create(index=INDEX_NAME, body=mapping)
    #console.print(f"[green]Índice '{INDEX_NAME}' creado con mapeo geo_point[/green]")

# Configuración de HDFS
HDFS_URL = 'http://172.26.0.4:9870'
HDFS_DIR = '/user/data/Trafico_Aereo/'

console = Console()

def setup_hdfs():
    """Configura la conexión con HDFS"""
    try:
        client = InsecureClient(HDFS_URL, user='root')
        client.makedirs(HDFS_DIR)
        return client
    except Exception as e:
        console.print(f"[red]Error de conexión con HDFS: {str(e)}[/red]")
        return None

hdfs_client = setup_hdfs()

# Configuración de Kafka
consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'air_traffic_group',
    'auto.offset.reset': 'latest'
}
consumer = Consumer(consumer_config)
consumer.subscribe(['Air_Traffic'])

def save_data(data, timestamp):
    """Guarda los datos en HDFS"""
    filename = f"Trafico_Aereo_{timestamp}.json"
    
    if hdfs_client:
        try:
            file_path = f"{HDFS_DIR}{filename}"
            with hdfs_client.write(file_path, encoding='utf-8') as writer:
                json.dump(data, writer)
            console.print(f"[green]Archivo guardado en HDFS: {filename}[/green]")
            return "HDFS", filename
        except Exception as e:
            console.print(f"[red]Error al escribir en HDFS: {str(e)}[/red]")

    return "None", "Error"

def send_to_elasticsearch(data):
    """Envía datos a Elasticsearch"""
    if isinstance(data, list):  # Ahora cada mensaje es una lista de un vuelo individual
        flight = data 
        if len(flight) >= 6:
            latitude = flight[6]
            longitude = flight[5]
            delay = flight[-1] 

            if latitude is None or longitude is None:
                console.print(f"[red]Latitude or Longitude is None, skipping this flight[/red]")
                return
            
            try:
                latitude = float(latitude)
                longitude = float(longitude)
            except ValueError:
                console.print(f"[red]Invalid latitude or longitude value[/red]")
                return
            
            doc = {
                "icao24": flight[0],
                "callsign": flight[1].strip(),
                "origin_country": flight[2],
                "time_position": flight[3],
                "last_contact": flight[4],
                "location": {
                    "lat": latitude,
                    "lon": longitude
                },
                "altitude": flight[7],
                "velocity": flight[9],
                "heading": flight[10],
                "vertical_rate": flight[11],
                "firstSeen": flight[12],
                "lastSeen": flight[13],
                "estDepartureAirport": flight[14],
                "estArrivalAirport": flight[15],
                "delay": delay,  
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            es.index(index=INDEX_NAME, body=doc)
    else:
        console.print(f"[red]Formato inesperado del mensaje recibido[/red]")


def process_message(msg):
    try:
        data = json.loads(msg.value().decode('utf-8'))
        timestamp = int(time.time())

        # Guardar en HDFS como respaldo
        storage_type, filename = save_data(data, timestamp)

        # Enviar datos a Elasticsearch
        send_to_elasticsearch(data)
        console.print(f"[blue]Datos enviados a Elasticsearch[/blue]")

    except Exception as e:
        console.print(f"[red]Error procesando mensaje: {e}[/red]")

def signal_handler(sig, frame):
    console.print("\n[red]Interrupción detectada. Cerrando consumidor...[/red]")
    consumer.close()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

if __name__ == "__main__":
    console.print("[green]Iniciando la captura de datos de tráfico aéreo...[/green]")
    
    try:
        with Live(Table(title="Trafico Aereo"), refresh_per_second=1) as live:
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    console.print(f"[red]Error en consumidor: {msg.error()}[/red]")
                    continue
                
                process_message(msg)

    except KeyboardInterrupt:
        console.print("[red]Streaming detenido por el usuario[/red]")
    finally:
        consumer.close()
