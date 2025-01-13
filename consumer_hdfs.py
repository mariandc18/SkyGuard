from kafka import KafkaConsumer
import json
import time
import os
import signal
import sys
from hdfs import InsecureClient
from datetime import datetime
from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.panel import Panel
from rich.layout import Layout

# Configuración de Rich
console = Console()
layout = Layout()

# Configuración de HDFS y Kafka
HDFS_URL = 'http://172.26.0.4:9870'
HDFS_DIR = '/user/data/Trafico_Aereo/'

def setup_hdfs():
    """Configura la conexión HDFS y crea el directorio si no existe"""
    try:
        client = InsecureClient(HDFS_URL, user='root')
        
        # Verificar si el directorio existe
        try:
            client.status(HDFS_DIR)
            console.print(f"[green]Directorio HDFS {HDFS_DIR} existe[/green]")
        except:
            # Crear el directorio y sus padres
            client.makedirs(HDFS_DIR)
            console.print(f"[yellow]Directorio HDFS {HDFS_DIR} creado[/yellow]")
        
        # Verificar permisos escribiendo un archivo de prueba
        test_file = f"{HDFS_DIR}test.txt"
        try:
            with client.write(test_file, encoding='utf-8') as writer:
                writer.write("test")
            client.delete(test_file)
            console.print("[green]Permisos de escritura verificados[/green]")
        except Exception as e:
            console.print(f"[red]Error de permisos: {str(e)}[/red]")
            return None
        
        return client
    except Exception as e:
        console.print(f"[red]Error de conexión con HDFS: {str(e)}[/red]")
        return None

# Configurar HDFS
hdfs_client = setup_hdfs()

LOCAL_DIR = "./Trafico_Aereo"
os.makedirs(LOCAL_DIR, exist_ok=True)

consumer = KafkaConsumer(
    'Air_Traffic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def save_data(data, timestamp):
    """Guarda los datos en HDFS o localmente"""
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
    
    # Fallback a almacenamiento local
    local_path = os.path.join(LOCAL_DIR, filename)
    with open(local_path, 'w', encoding='utf-8') as f:
        json.dump(data, f)
    #console.print(f"[yellow]Archivo guardado localmente: {filename}[/yellow]")
    return "Local", filename

def generate_table(messages, files):
    """Genera la tabla de mensajes y archivos"""
    table = Table(title="Trafico Aereo")
    table.add_column("Timestamp", justify="center", style="cyan")
    table.add_column("Informacion del Trafico Aereo", justify="right", style="green")
    table.add_column("Last File Saved", justify="left", style="magenta")
    
    for msg in messages[-5:]:
        table.add_row(
            msg['timestamp'],
            #f"${msg['price']:,.2f}",
            msg.get('last_file', 'Not saved yet')
        )
    return table

def signal_handler(sig, frame):
    print("\nInterrupción detectada. Cerrando consumidor...")
    consumer.close()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

if __name__ == "__main__":
    console.print("Iniciando el flujo de mensajes...")
    # Crea una tabla global
    table = Table(title="Trafico Aereo")
    table.add_column("Timestamp", justify="center", style="cyan")
    table.add_column("Last File Saved", justify="left", style="magenta")
    
    try:
        with Live(layout, refresh_per_second=1) as live:
            while True:
                # Se obtiene el mensaje con un timeout de 1000ms
                messages = consumer.poll(timeout_ms=1000)
                
                # Itera sobre todos los mensajes obtenidos
                for message in messages.values():
                    # Kafka devuelve los mensajes como una lista, así que iteramos sobre cada mensaje
                    for msg in message:
                        data = msg.value  # Ahora accedemos a 'value' correctamente
                        data['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        
                        storage_type, filename = save_data(data, int(time.time()))
                        data['last_file'] = f"{storage_type}: {filename}"
                        
                        #table = generate_table([data], [filename])
                        #table.rows.clear()
                        table.add_row(
                            data['timestamp'],
                            data['last_file']
                            )
                        live.update(table)
                        live.refresh()
    except KeyboardInterrupt:
        console.print("[red]Streaming detenido por el usuario[/red]")
    finally:
        consumer.close()