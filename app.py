import pandas as pd
from generar_ids import generar_ids_y_stock, filtrar_por_tipo_posicion
# Definir la ruta de los archivos
ruta_datasets = 'datasets.xlsx'  # Asumo que están en un solo archivo llamado 'datasets.xlsx'
ruta_mb51 = 'MB51.xlsx'
ruta_mb52 = 'MB52.xlsx'
ruta_me2n = 'ME2N.xlsx'

# Cargar los DataFrames desde cada archivo y hoja

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

# Verificamos si la unión se hizo correctamente
""" print("DataFrame base con columnas adicionales de capacidad instalada:")
print(df_base.head()) """

# Calculamos el nuevo campo 'stock_cobertura_ideal' basado en las columnas ya presentes en df_base
df_base['stock_cobertura_ideal'] = (
    (df_base['ratio_nominal'] * df_base['cip']) / df_base['rendimiento']
) * df_base['cobertura_ideal']

# Verificación del resultado: Mantendremos los campos originales de df_ratios_planta_insumo y el campo calculado
""" print("DataFrame final con 'stock_cobertura_ideal' añadido:")
print(df_base.head())  # Muestra todas las columnas incluyendo las nuevas
print(df_base[['id_insumo', 'id_localidad', 'stock_cobertura_ideal']].head())  """ #
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

# Peticion al portal de pesca tasa
inicio = "20240101"
final = "20240430"
df_datos, df_dias_produccion = consultar_pesca(inicio, final)

df_consumo = df_mb51.copy()

# Paso 2: Asegurarnos de que la columna 'Fe.contabilización' esté en formato de fecha
df_consumo['Fe.contabilización'] = pd.to_datetime(df_consumo['Fe.contabilización'])

# Paso 3: Agrupar por id_localidad_insumo y sumar la cantidad por cada día
df_consumo_diario = df_consumo.groupby(['id_localidad_insumo', 'Fe.contabilización'])['Cantidad'].sum().reset_index()

# Paso 4: Calcular la diferencia de días (rango de días) para cada id_localidad_insumo
# Agrupamos por id_localidad_insumo para obtener el rango de días
df_rango_dias = df_consumo_diario.groupby('id_localidad_insumo').agg(
    fecha_min=('Fe.contabilización', 'min'),
    fecha_max=('Fe.contabilización', 'max'),
    total_consumo=('Cantidad', 'sum')
).reset_index()

# Calculamos el rango en días para cada id_localidad_insumo
df_rango_dias['dias_rango'] = (df_rango_dias['fecha_max'] - df_rango_dias['fecha_min']).dt.days + 1  # Añadimos +1 para contar el día mínimo también

# Paso 5: Calcular el consumo diario para cada id_localidad_insumo
df_rango_dias['consumo_diario'] = df_rango_dias['total_consumo'] / df_rango_dias['dias_rango']

# Paso 6: Convertir a valor absoluto el consumo diario
df_rango_dias['consumo_diario_absoluto'] = df_rango_dias['consumo_diario'].abs()

# Paso 7: Verificación del resultado final
print("DataFrame con el consumo diario absoluto:")
print(df_rango_dias.head())