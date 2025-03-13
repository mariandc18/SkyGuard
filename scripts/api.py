from fastapi import FastAPI
from pydantic import BaseModel
import joblib
import numpy as np

app = FastAPI()

# Cargar el modelo guardado
modelo = joblib.load("../model/modelo_xgboost.pkl")

# Definir un esquema para la entrada
class InputData(BaseModel):
    data: list

@app.post("/predict/")
def predict(input_data: InputData):
    X = np.array(input_data.data).reshape(1, -1)
    prediction = modelo.predict(X)
    return {"prediction": int(prediction[0])}
