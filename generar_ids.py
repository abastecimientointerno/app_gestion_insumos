import pandas as pd

# Función para generar el "id_localidad"
def generar_id_localidad(centro, almacen):
    """
    Genera un ID de localidad basado en el Centro y el Almacén.
    Si Centro es 'TCNO' y Almacén es 'HUB', devuelve 'TCNO-HUB'.
    Caso contrario, devuelve 'Centro-Almacén'.
    """
    if centro == 'TCNO' and almacen == 'HUB':
        return 'TCNO-HUB'
    return f"{centro}"

# Función para generar un DataFrame con las columnas de ID y stock
def generar_ids_y_stock(df, tipo='general'):
    """
    Genera las columnas necesarias para un DataFrame específico:
    
    - "id_localidad": basado en Centro y Almacén.
    - "id_insumo": renombrado de la columna Material.
    - "stock_libre_mas_calidad": suma de Libre utilización + Inspecc.de calidad (si las columnas existen).
    - "id_localidad_insumo": concatenación de id_localidad y id_insumo.

    Parámetros:
    - df: DataFrame al que se le van a agregar las nuevas columnas.
    - tipo: Si el tipo es 'general' incluye la columna stock_libre_mas_calidad.

    Retorna:
    - df: DataFrame modificado con las nuevas columnas.
    """
    # Generar "id_localidad"
    df['id_localidad'] = df.apply(lambda row: generar_id_localidad(row['Centro'], row['Almacén']), axis=1)

    # Renombrar "Material" a "id_insumo"
    df['id_insumo'] = df['Material']

    # Solo para ciertos DataFrames, generar "stock_libre_mas_calidad"
    if 'Libre utilización' in df.columns and 'Inspecc.de calidad' in df.columns and tipo == 'general':
        df['stock_libre_mas_calidad'] = df['Libre utilización'] + df['Inspecc.de calidad']
    
    # Generar "id_localidad_insumo" concatenando "id_localidad" e "id_insumo"
    df['id_localidad_insumo'] = df['id_localidad'] + df['id_insumo'].astype(str)
    
    return df

# Función para filtrar por Tipo de posición en ME2N
def filtrar_por_tipo_posicion(df, tipo_col='Tipo de posición'):
    """
    Filtra el DataFrame ME2N en dos DataFrames separados:

    - df_me2n_pt: filas donde Tipo de posición es 'V'.
    - df_me2n_oc: filas donde Tipo de posición es distinto de 'V'.

    Parámetros:
    - df: DataFrame ME2N completo.
    - tipo_col: Nombre de la columna que contiene el tipo de posición.

    Retorna:
    - df_me2n_pt: DataFrame con filas donde Tipo de posición es 'V'.
    - df_me2n_oc: DataFrame con filas donde Tipo de posición es distinto de 'V'.
    """
    df_me2n_pt = df[df[tipo_col] == 'V'].copy()
    df_me2n_oc = df[df[tipo_col] != 'V'].copy()
    
    return df_me2n_pt, df_me2n_oc
