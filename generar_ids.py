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

def generar_y_separar_mb52(df, tipo='general'):
    """
    Genera las columnas necesarias para el DataFrame MB52 y lo separa en cuatro DataFrames según el almacén.
    
    - Aplica las transformaciones de "id_localidad", "id_insumo", "stock_libre_mas_calidad" y "id_localidad_insumo".
    - Separa el DataFrame en cuatro partes según el valor de la columna 'Almacén':
        - Almacén == "PI01" -> df_mb52_produccion
        - Almacén == "" -> df_mb52_transito
        - Almacén == "L003" -> df_mb52_hub
        - Cualquier otro valor de almacén -> df_mb52 (resto de filas)
    
    Parámetros:
    - df: DataFrame al que se le van a agregar las nuevas columnas y que se separará en cuatro.
    - tipo: Si el tipo es 'general', se incluye la columna stock_libre_mas_calidad.

    Retorna:
    - df_mb52_produccion: DataFrame filtrado para el almacén "PI01".
    - df_mb52_transito: DataFrame filtrado para el almacén vacío "".
    - df_mb52_hub: DataFrame filtrado para el almacén "L003".
    - df_mb52: DataFrame con el resto de los almacenes.
    """
    # Generar "id_localidad"
    df['id_localidad'] = df.apply(lambda row: generar_id_localidad(row['Centro'], row['Almacén']), axis=1)

    # Renombrar "Material" a "id_insumo"
    df['id_insumo'] = df['Material']

    # Generar "stock_libre_mas_calidad" si las columnas existen y el tipo es 'general'
    if 'Libre utilización' in df.columns and 'Inspecc.de calidad' in df.columns and tipo == 'general':
        df['stock_libre_mas_calidad'] = df['Libre utilización'] + df['Inspecc.de calidad']
    
    # Generar "id_localidad_insumo" concatenando "id_localidad" e "id_insumo"
    df['id_localidad_insumo'] = df['id_localidad'] + df['id_insumo'].astype(str)

    # Separar en tres DataFrames según el valor de la columna 'Almacén'
    df_mb52_produccion = df[df['Almacén'] == 'PI01'].copy()
    df_mb52_transito = df[df['Almacén'] == ''].copy()
    df_mb52_hub = df[df['Almacén'] == 'L003'].copy()

    # DataFrame para el resto de las filas (que no cumplen ninguna de las condiciones anteriores)
    df_mb52 = df[~df['Almacén'].isin(['PI01', '', 'L003'])].copy()

    # Retornar los cuatro DataFrames
    return df_mb52_produccion, df_mb52_transito, df_mb52_hub, df_mb52



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
