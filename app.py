import pandas as pd
from api_portal import consultar_pesca
from generar_ids import generar_ids_y_stock, filtrar_por_tipo_posicion
# Definir la ruta de los archivos
ruta_datasets = 'datasets.xlsx'
ruta_mb51 = 'MB51.xlsx'
ruta_mb52 = 'MB52.xlsx'
ruta_me2n = 'ME2N.xlsx'

# 1. Desde el archivo "datasets"
df_capacidad_instalada = pd.read_excel(ruta_datasets, sheet_name='db_capacidad_instalada')
df_cuota = pd.read_excel(ruta_datasets, sheet_name='db_cuota')
df_ratios_planta_insumo = pd.read_excel(ruta_datasets, sheet_name='db_ratios_planta_insumo')
df_insumos = pd.read_excel(ruta_datasets, sheet_name='db_insumos')
# 2. Desde MB51 (asumo que se guarda en Excel y la hoja es "Sheet1")
df_mb51 = pd.read_excel(ruta_mb51, sheet_name='Sheet1')
# 3. Desde MB52 (asumo que se guarda en Excel y la hoja es "Sheet1")
df_mb52 = pd.read_excel(ruta_mb52, sheet_name='Sheet1')
# 4. Desde ME2N (asumo que se guarda en Excel y la hoja es "Sheet1")
df_me2n = pd.read_excel(ruta_me2n, sheet_name='Sheet1')

df_mb51 = generar_ids_y_stock(df_mb51)
df_mb52 = generar_ids_y_stock(df_mb52)
df_me2n = generar_ids_y_stock(df_me2n)
df_me2n_pt, df_me2n_oc = filtrar_por_tipo_posicion(df_me2n)
del df_me2n
df_base = pd.merge(df_ratios_planta_insumo, df_capacidad_instalada[['id_localidad', 'cip', 'rendimiento', 'cobertura_ideal']],
                   on='id_localidad', how='left')
# Calculamos el nuevo campo 'stock_cobertura_ideal' basado en las columnas ya presentes en df_base
df_base['stock_cobertura_ideal'] = (
    (df_base['ratio_nominal'] * df_base['cip']) / df_base['rendimiento']
) * df_base['cobertura_ideal']

df_base['id_localidad_insumo'] = df_base['id_localidad'] + df_base['id_insumo'].astype(str)

# Paso 2: Unir df_base con df_mb52 usando id_localidad_insumo como clave
# Solo traemos la columna 'stock_libre_mas_calidad' de df_mb52
df_base = pd.merge(df_base, df_mb52[['id_localidad_insumo', 'stock_libre_mas_calidad']],
                   on='id_localidad_insumo', how='left')

# Paso 3: Calcular el campo 'excedentes_o_faltante'
# Condicional: Si 'stock_libre_mas_calidad' >= 'cobertura_ideal', entonces 0, si no, la diferencia.
df_base['excedentes_o_faltante'] = df_base.apply(
    lambda row: 0 if row['stock_libre_mas_calidad'] >= row['cobertura_ideal'] else (row['cobertura_ideal'] - row['stock_libre_mas_calidad']),
    axis=1
)

df_base['cobertura_teorica_con_stock'] = (
    (df_base['stock_libre_mas_calidad'] * df_base['cobertura_ideal']) / df_base['stock_cobertura_ideal']
)

df_consumo = df_mb51.copy()

df_consumo_total = df_consumo.groupby(['id_localidad', 'id_insumo']).agg({'Cantidad': 'sum'}).reset_index()
# Convertir la columna 'Cantidad' a valores absolutos
df_consumo_total['Cantidad'] = df_consumo_total['Cantidad'].abs()



inicio = "20240101"
final = "20240130"
df_datos, df_dias_produccion = consultar_pesca(inicio, final)

df_consumo_total = pd.merge(df_consumo_total, df_dias_produccion[['id_localidad', 'dias_de_pesca']],
                            on='id_localidad', how='left')

# Calcular el consumo diario
df_consumo_total['consumo_diario'] = df_consumo_total['Cantidad'] / df_consumo_total['dias_de_pesca']

# Opcional: Manejar divisiones por cero
df_consumo_total['consumo_diario'] = df_consumo_total['consumo_diario'].fillna(0)
# Crear un nuevo ID concatenando id_localidad y id_insumo
df_consumo_total['id_localidad_insumo'] = df_consumo_total['id_localidad'].astype(str) + df_consumo_total['id_insumo'].astype(str)


df_base = pd.merge(df_base, df_consumo_total[['id_localidad_insumo', 'consumo_diario']],
                   on='id_localidad_insumo', how='left')

df_base['cobertura_real'] = df_base['stock_libre_mas_calidad']* df_base['consumo_diario']
# Verificación del resultado
df_resultado = pd.merge(
    df_base,
    df_insumos[['id_insumo', 'nombre_insumo', 'id_final', 'valor_redondeo']],
    on='id_insumo',
    how='inner'
)

print(df_resultado.head())

#print("DataFrame final df_consumo_total con 'dias_de_pesca' añadido:")
#print(df_base.head())
df_base.to_excel('resultados.xlsx', index=False)