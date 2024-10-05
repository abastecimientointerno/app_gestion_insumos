# Importar librerías necesarias
import pandas as pd
from prophet import Prophet

def realizar_proyeccion(df_pesca):
    # Convertir la columna de fecha a tipo datetime
    df_pesca['ds'] = pd.to_datetime(df_pesca['FIDES'], dayfirst=True)

    # Asegurarse de que la columna CNPDS sea numérica, convirtiendo errores a NaN
    df_pesca['CNPDS'] = pd.to_numeric(df_pesca['CNPDS'], errors='coerce')

    # Rellenar NaN con 0
    df_pesca['CNPDS'].fillna(0, inplace=True)

    # Totalizar por día (sumar todas las descargas en cada día)
    df_daily = df_pesca.groupby(df_pesca['ds'].dt.date)['CNPDS'].sum().reset_index()

    # Renombrar columnas para que Prophet pueda trabajar con ellas ('ds' para fecha y 'y' para la variable objetivo)
    df_daily.columns = ['ds', 'y']

    # Convertir la columna 'ds' a datetime
    df_daily['ds'] = pd.to_datetime(df_daily['ds'])

    # Crear y ajustar el modelo Prophet
    model = Prophet()
    model.fit(df_daily)

    # Crear un DataFrame con las fechas a futuro para predecir (por ejemplo, 30 días hacia adelante)
    future = model.make_future_dataframe(periods=30)

    # Hacer las predicciones
    forecast = model.predict(future)

    # Asegurarse de que no haya valores negativos
    forecast['yhat'] = forecast['yhat'].apply(lambda x: max(0, x))
    forecast['yhat_lower'] = forecast['yhat_lower'].apply(lambda x: max(0, x))
    forecast['yhat_upper'] = forecast['yhat_upper'].apply(lambda x: max(0, x))

    # Agregar los datos reales a la proyección
    # Unir los datos reales y las predicciones por fecha
    forecast = forecast.merge(df_daily, on='ds', how='left')
    
    # Renombrar la columna 'y' para que refleje que son datos reales
    forecast.rename(columns={'y': 'real_data'}, inplace=True)

    # Devolver el DataFrame de proyección con los datos reales
    return forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper', 'real_data']]
