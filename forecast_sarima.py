import pandas as pd
from statsmodels.tsa.statespace.sarimax import SARIMAX
import matplotlib.pyplot as plt
from datetime import timedelta

df = pd.read_excel('forecast.xlsx')  # Cambia 'ruta_a_tu_archivo.xlsx' por la ubicación de tu archivo

# Convertir la columna de fecha y hora a tipo datetime
df['fecha_hora'] = pd.to_datetime(df['FIDES'])

# Totalizar por día (sumar todas las descargas en cada día)
df_daily = df.groupby(df['fecha_hora'].dt.date)['CNPDS'].sum().reset_index()

df_daily.columns = ['ds', 'y']
# Convertir la columna 'ds' a datetime y establecerla como índice
df_daily['ds'] = pd.to_datetime(df_daily['ds'])
df_daily.set_index('ds', inplace=True)

# Si faltan días en la serie, rellenar con ceros (necesario para SARIMA)
idx = pd.date_range(df_daily.index.min(), df_daily.index.max())
df_daily = df_daily.reindex(idx, fill_value=0)

# Crear y ajustar el modelo SARIMA
# order=(p,d,q) y seasonal_order=(P,D,Q,S) deben ajustarse con base en los datos.
sarima_model = SARIMAX(df_daily['y'],
                       order=(1, 1, 1),  # Parámetros ARIMA: p,d,q
                       seasonal_order=(1, 1, 1, 7),  # Parámetros estacionales: P,D,Q,S
                       enforce_stationarity=False, 
                       enforce_invertibility=False)

# Ajustar el modelo
sarima_result = sarima_model.fit()

# Hacer predicciones a 30 días hacia adelante
future_steps = 30
forecast_sarima = sarima_result.get_forecast(steps=future_steps)

# Obtener las predicciones y los intervalos de confianza
forecast_sarima_mean = forecast_sarima.predicted_mean
forecast_sarima_ci = forecast_sarima.conf_int()

# Asegurarse de que no haya valores negativos en las predicciones
forecast_sarima_mean = forecast_sarima_mean.apply(lambda x: max(0, x))
forecast_sarima_ci['lower y'] = forecast_sarima_ci['lower y'].apply(lambda x: max(0, x))
forecast_sarima_ci['upper y'] = forecast_sarima_ci['upper y'].apply(lambda x: max(0, x))

# Crear un índice de fechas para las predicciones
forecast_index = pd.date_range(df_daily.index[-1] + timedelta(1), periods=future_steps)

# Graficar las predicciones y los intervalos de confianza
plt.figure(figsize=(10, 6))

# Graficar los valores reales
plt.plot(df_daily.index, df_daily['y'], label='Actuales', marker='o')

# Graficar las predicciones
plt.plot(forecast_index, forecast_sarima_mean, label='Predicción', color='orange', marker='o')

# Añadir el intervalo de confianza
plt.fill_between(forecast_index, 
                 forecast_sarima_ci['lower y'], forecast_sarima_ci['upper y'], 
                 color='orange', alpha=0.2, label='Intervalo de confianza')

# Configurar títulos y ejes
plt.title('Predicción de descargas de pesca (SARIMA)')
plt.xlabel('Fecha')
plt.ylabel('Cantidad de descarga')
plt.legend()
plt.grid(True)
plt.show()
