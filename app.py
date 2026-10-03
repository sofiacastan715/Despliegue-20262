import streamlit as st
import pandas as pd
import joblib
import io

st.title("Predicción de Aprobación de Curso")

# 1. Sección para subir archivos
st.header("1. Subir Archivos Necesarios")

archivo_excel = st.file_uploader("Sube el archivo Excel (AprobacionCurso.xlsx)", type=["xlsx"])

if archivo_excel is not None:
    try:
        df = pd.read_excel(archivo_excel)
        st.success("¡Archivo Excel cargado con éxito!")
        st.subheader("Vista previa de los datos:")
        st.write(df.head())
    except Exception as e:
        st.error(f"Error al cargar el archivo Excel: {e}")

st.info("También puedes permitir la subida de los archivos de transformación o modelos si no están en el servidor:")
scaler_file = st.file_uploader("Sube el MinMaxScaler (min_max_scaler.joblib) [Opcional]", type=["joblib"])
model_file = st.file_uploader("Sube el Modelo (bagging_optimizado.joblib) [Opcional]", type=["joblib"])
