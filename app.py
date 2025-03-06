# Algunas librerias
import streamlit as st
import joblib
import numpy as np
import pandas as pd

# Cargar el modelo entrenado
model = joblib.load("modelo_rf.pkl")

st.title("Predicción de Enfermedades Cardíacas")

# Entradas del usuario en la app
age = st.number_input("Edad", min_value=1, max_value=120, value=55)
sex = st.selectbox("Sexo", options=["Mujer", "Hombre"])
cp = st.selectbox("Tipo de dolor en el pecho (cp)", options=[0, 1, 2, 3])
trestbps = st.number_input("Presión arterial en reposo (trestbps)", min_value=80, max_value=200, value=130)
chol = st.number_input("Colesterol (chol)", min_value=100, max_value=600, value=250)
fbs = st.selectbox("Azúcar en sangre en ayunas >120 mg/dl (fbs)", options=[0, 1])
restecg = st.selectbox("Electrocardiograma en reposo (restecg)", options=[0, 1, 2])
thalach = st.number_input("Frecuencia cardiaca máxima (thalach)", min_value=60, max_value=220, value=150)
exang = st.selectbox("Angina inducida por ejercicio (exang)", options=[0, 1])
oldpeak = st.number_input("Depresión del ST (oldpeak)", min_value=0.0, max_value=10.0, value=1.0, step=0.1)
slope = st.selectbox("Pendiente del segmento ST (slope)", options=[0, 1, 2])
ca = st.number_input("Número de vasos principales (ca)", min_value=0, max_value=4, value=0)
thal = st.selectbox("Tipo de talasemia (thal)", options=[1, 2, 3])

sex_val = 1 if sex == "Hombre" else 0

feature_names = ["age", "sex", "cp", "trestbps", "chol", "fbs", "restecg",
                 "thalach", "exang", "oldpeak", "slope", "ca", "thal"]

input_df = pd.DataFrame([[age, sex_val, cp, trestbps, chol, fbs, restecg,
                           thalach, exang, oldpeak, slope, ca, thal]],
                        columns=feature_names)

st.write("Datos ingresados:", input_df)

if st.button("Predecir"):
    prediction = model.predict(input_df)  
    if prediction[0] == 1:
        st.error("El paciente tiene un ALTO riesgo de enfermedad cardíaca.")
    else:
        st.success("El paciente tiene un BAJO riesgo de enfermedad cardíaca.")