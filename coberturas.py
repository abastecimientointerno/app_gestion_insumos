import pandas as pd
from generar_ids import generar_ids_y_stock, filtrar_por_tipo_posicion


ruta_mb52 = 'MB52.xlsx'
df_mb52 = pd.read_excel(ruta_mb52, sheet_name='Sheet1')

df_mb52 = generar_ids_y_stock(df_mb52)

#df_mb52 = df_mb52.groupby('id_localidad_insumo')['stock_libre_mas_calidad'].sum().reset_index()

print(df_mb52.head())