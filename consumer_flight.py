from kafka import KafkaConsumer
import json
import time
import os
import signal
import sys
from hdfs import InsecureClient
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from rich.console import Console
from rich.live import Live
from rich.table import Table

# Configuración de Rich
console = Console()

# Configuración de HDFS y Kafka
HDFS_URL = 'http://172.24.0.4:9870'
HDFS_DIR = '/user/data/Trafico_Aereo_vuelos_y_estados/'
LOCAL_DIR = "./Trafico_Aereo_conjunto"
os.makedirs(LOCAL_DIR, exist_ok=True)

TOPICS = ['Air_Traffic_Flights', 'Air_Traffic_States']
KAFKA_SERVERS = ['localhost:9092']

def setup_hdfs():
    """Configura la conexión HDFS y crea el directorio si no existe"""
    try:
        client = InsecureClient(HDFS_URL, user='root')
        try:
            client.status(HDFS_DIR)
        except:
            client.makedirs(HDFS_DIR)
        return client
    except Exception as e:
        console.print(f"[red]Error de conexión con HDFS: {str(e)}[/red]")
        return None

hdfs_client = setup_hdfs()

def save_data_batch(batch_data, timestamp, source):
    """Guarda los datos en HDFS o localmente como un solo JSON"""
    filename = f"{source}_Trafico_Aereo_{timestamp}.json"
    
    if hdfs_client:
        try:
            file_path = f"{HDFS_DIR}{filename}"
            # Verificar si el archivo ya existe en HDFS
            if hdfs_client.status(file_path, strict=False):
                console.print(f"[yellow]Archivo ya existe en HDFS: {filename}[/yellow]")
                return
            # Guardar el archivo en HDFS
            with hdfs_client.write(file_path, encoding='utf-8') as writer:
                json.dump(batch_data, writer)
            console.print(f"[green]Archivo guardado en HDFS: {filename}[/green]")
        except Exception as e:
            console.print(f"[red]Error al escribir en HDFS: {str(e)}[/red]")
    
    # Si falla, guardar localmente
    local_path = os.path.join(LOCAL_DIR, filename)
    with open(local_path, 'w', encoding='utf-8') as f:
        json.dump(batch_data, f)
    console.print(f"[yellow]Archivo guardado localmente: {filename}[/yellow]")

def process_messages(consumer, topic):
    """Procesa los mensajes de Kafka, agrupa por timestamp y guarda en lote"""
    table = Table(title=f"Mensajes de {topic}")
    table.add_column("Timestamp", justify="center", style="cyan")
    table.add_column("Último Archivo Guardado", justify="left", style="magenta")
    
    batch_data = []  # Lista para almacenar mensajes del mismo timestamp
    last_timestamp = None  # Último timestamp procesado
    source = "Estados" if topic == "Air_Traffic_States" else "Vuelos"  # Determinar la fuente
    
    with Live(table, refresh_per_second=1) as live:
        for message in consumer:
            data = message.value
            current_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")  # Agrupación por segundoto
            
            # Si el timestamp cambia, guardar el lote anterior
            if last_timestamp and current_timestamp != last_timestamp:
                save_data_batch(batch_data, last_timestamp, source)  # Guardar lote anterior
                batch_data = []  # Reiniciar el lote
            
            # Agregar el mensaje al lote actual
            if isinstance(data, dict):  # Verificar si data es un diccionario
                data['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                batch_data.append(data)
            elif isinstance(data, list):  # Si data es una lista, iterar sobre sus elementos
                for item in data:
                    if isinstance(item, dict):
                        item['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        batch_data.append(item)
            
            last_timestamp = current_timestamp
            
            # Actualizar la tabla visual
            table.add_row(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), f"{source}_Trafico_Aereo_{last_timestamp}.json")
            live.update(table)

def start_consumers():
    """Inicia los consumidores en paralelo"""
    consumers = {topic: KafkaConsumer(
        topic, bootstrap_servers=KAFKA_SERVERS, auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    ) for topic in TOPICS}
    
    with ThreadPoolExecutor(max_workers=len(TOPICS)) as executor:
        futures = [executor.submit(process_messages, consumer, topic) for topic, consumer in consumers.items()]
        try:
            for future in futures:
                future.result()
        except KeyboardInterrupt:
            console.print("[red]Streaming detenido por el usuario[/red]")
        finally:
            for consumer in consumers.values():
                consumer.close()

def signal_handler(sig, frame):
    console.print("\n[red]Interrupción detectada. Cerrando consumidores...[/red]")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

if __name__ == "__main__":
    console.print("[blue]Iniciando consumidores para los tópicos de Kafka...[/blue]")
    start_consumers()