
from hdfs import InsecureClient
import json
import os
import pandas as pd
import requests
import numpy as np
from geopy.distance import geodesic
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder

# Configuración de HDFS
HDFS_URL = 'http://172.18.0.4:9870'
HDFS_DIR = '/user/data/Trafico_Aereo_historico/'
todas_las_filas = []

client = InsecureClient(HDFS_URL, user='root')

try:
    archivos = client.list(HDFS_DIR)
    print("Archivos que comienzan con 'Estados':")
    
    for archivo in archivos:
        if archivo.startswith("Estados"):
            #print(f"-> {archivo}")
            ruta_archivo = os.path.join(HDFS_DIR, archivo)
            
            with client.read(ruta_archivo, encoding='utf-8') as reader:
                data = json.load(reader)
                data=data[0]
                
                if "states" in data:
                   for sublista in data["states"]:
                    todas_las_filas.append(sublista)
except Exception as e:
    print(f"Error al acceder a HDFS: {e}")
    
df = pd.DataFrame(todas_las_filas)

df.columns = [f"col_{i}" for i in range(df.shape[1])]
df.columns =['icao24','callsign','origin_country', 'time_position','last_contact','longitude', 'latitude','baro_altitude', 'on_ground', 'velocity' , 'vertical_rate' , 'sensors','geo_altitude','squawk','spi','position_source','category']

vuelos={}
try:
    archivos = client.list(HDFS_DIR)
    print("Archivos que comienzan con 'Vuelos':")
    
    for archivo in archivos:
        if archivo.startswith("Vuelos"):
            #print(f"-> {archivo}")
            ruta_archivo = os.path.join(HDFS_DIR, archivo)
            
            with client.read(ruta_archivo, encoding='utf-8') as reader:
                data = json.load(reader)

                for vuelo in data:
                    icao24 = vuelo.get("icao24")
                    if icao24:
                        #if icao24 not in vuelos:
                            vuelos[icao24] = vuelo  # Guardamos el vuelo usando el icao24 como clave
                        #else:
                            #print(f"El vuelo con icao24 '{icao24}' ya existe, no se añade.")
                    else:
                        print(f"Advertencia: vuelo sin 'icao24', se ignora: {vuelo}")

except Exception as e:
    print(f"Error al acceder a HDFS: {e}")

df_vuelos = pd.DataFrame(vuelos)
df_vuelos=df_vuelos.T

df=pd.merge(df, df_vuelos, on='icao24', how='inner')

# Función para calcular la distancia entre dos coordenadas usando la fórmula de Haversine
def calcular_distancia(lat1, lon1, lat2, lon2):
    if np.isnan(lat1) or np.isnan(lon1) or np.isnan(lat2) or np.isnan(lon2):
        return np.nan  # Si faltan datos, devolver NaN
    return geodesic((lat1, lon1), (lat2, lon2)).km  # Retorna la distancia en km

# Función para estimar el tiempo de llegada
def estimar_tiempo_llegada(distancia_km, velocidad_mps):
    if velocidad_mps > 0:
        return distancia_km / (velocidad_mps * 3.6)  # Convierte m/s a km/h y calcula el tiempo en horas
    return np.nan

def detectar_retraso(row):
    
    actual_flight_time = row['lastSeen'] - row['firstSeen']  
    
    if row['firstSeen'] and row['lastSeen']:
        flight_duration_minutes = round(actual_flight_time / 60, 2)
        
        arrival_distance = row['estArrivalAirportHorizDistance']  
        departure_distance = row.get('estDepartureAirportHorizDistance', 0) 
        
        if arrival_distance is not None and departure_distance is not None:
            
            total_distance = departure_distance + arrival_distance
            
            if not row["velocity"]!=0:
                return None
            expected_flight_time = total_distance / row["velocity"] 
            
            time_difference = actual_flight_time - expected_flight_time
            delay_minutes = round(time_difference / 60, 2)
            
            if delay_minutes <= 15:
               return 0
            else:
                return 1
        else:
            return None

df["status"]=0
df['status'] = df.apply(detectar_retraso, axis=1)

df = df.dropna(subset=['status'])
#df = df.dropna(subset=["icao24","longitude","latitude","baro_altitude","on_ground","velocity","vertical_rate","geo_altitude","firstSeen","lastSeen","estDepartureAirport","estArrivalAirport"])

df_predecir=df[["icao24","longitude","latitude","baro_altitude","on_ground","velocity","vertical_rate","geo_altitude","firstSeen","lastSeen","estDepartureAirport","estArrivalAirport"]]
y=df["status"]
print(df_predecir.dtypes)
df_predecir = df_predecir.copy()

label_encoder = LabelEncoder()
df_predecir["lastSeen"] = df_predecir["lastSeen"].astype(int)
df_predecir["firstSeen"] = df_predecir["firstSeen"].astype(int)
df_predecir["icao24"] = label_encoder.fit_transform(df_predecir["icao24"])
df_predecir["geo_altitude"] = label_encoder.fit_transform(df_predecir["geo_altitude"])
df_predecir["on_ground"] = label_encoder.fit_transform(df_predecir["on_ground"])
df_predecir["estDepartureAirport"] = label_encoder.fit_transform(df_predecir["estDepartureAirport"])
df_predecir["estArrivalAirport"] = label_encoder.fit_transform(df_predecir["estArrivalAirport"])

X_train, X_test, y_train, y_test = train_test_split(df_predecir, y, test_size=0.2, random_state=42)

print(len(X_train),len(X_test))

import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

# Crear y entrenar el modelo XGBoost
modelo = xgb.XGBClassifier(n_estimators=100, max_depth=5, learning_rate=0.1)
modelo.fit(X_train, y_train)

# Hacer predicciones
y_pred = modelo.predict(X_test)

# Evaluar el modelo
accuracy = accuracy_score(y_test, y_pred)
print(f"Precisión del modelo: {accuracy:.2f}")

import joblib

# Guardar el modelo entrenado en un archivo
joblib.dump(modelo, "modelo_xgboost.pkl")

# Cargar el modelo desde el archivo más tarde
modelo_cargado = joblib.load("modelo_xgboost.pkl")