import polars as pl

# Definir la ruta de los archivos
ruta_datasets = 'datasets.xlsx'  # Ajusta la ruta si los archivos tienen nombres diferentes
ruta_mb51 = 'MB51.xlsx'
ruta_mb52 = 'MB52.xlsx'
ruta_me2n = 'ME2N.xlsx'

# Cargar los DataFrames desde cada archivo y hoja

# 1. Desde el archivo "datasets"
df_capacidad_instalada = pl.read_excel(ruta_datasets, sheet_name='db_capacidad_instalada')
df_cuota = pl.read_excel(ruta_datasets, sheet_name='bd_cuota')
df_ratios_planta_insumo = pl.read_excel(ruta_datasets, sheet_name='db_ratios_planta_insumo')
df_insumos = pl.read_excel(ruta_datasets, sheet_name='db_insumos')

# 2. Desde MB51 (asumo que se guarda en Excel y la hoja es "Sheet1")
df_mb51 = pl.read_excel(ruta_mb51, sheet_name='Sheet1')

# 3. Desde MB52 (asumo que se guarda en Excel y la hoja es "Sheet1")
df_mb52 = pl.read_excel(ruta_mb52, sheet_name='Sheet1')

# 4. Desde ME2N (asumo que se guarda en Excel y la hoja es "Sheet1")
df_me2n = pl.read_excel(ruta_me2n, sheet_name='Sheet1')

# Verificación de los datos cargados
print("Capacidad Instalada:")
print(df_capacidad_instalada.head(), "\n")

print("Cuota:")
print(df_cuota.head(), "\n")

print("Ratios Planta-Insumo:")
print(df_ratios_planta_insumo.head(), "\n")

print("Insumos:")
print(df_insumos.head(), "\n")

print("MB51:")
print(df_mb51.head(), "\n")

print("MB52:")
print(df_mb52.head(), "\n")

print("ME2N:")
print(df_me2n.head(), "\n")
