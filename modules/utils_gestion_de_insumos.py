import pandas as pd
import numpy as np
from typing import Tuple, List
import requests
import json
from prophet import Prophet

def generar_id_localidad(centro: str, almacen: str) -> str:
    """
    Genera un ID de localidad basado en el Centro y el Almacén.
    
    Args:
        centro (str): Código del centro de producción
        almacen (str): Código del almacén
    
    Returns:
        str: ID de localidad generado
    """
    return 'TCNO-HUB' if centro == 'TCNO' and almacen == 'HUB' else centro

def generar_ids_y_stock(df: pd.DataFrame, tipo: str = 'general') -> pd.DataFrame:
    """
    Genera columnas adicionales en el DataFrame para identificación y stock.
    
    Args:
        df (pd.DataFrame): DataFrame de entrada
        tipo (str, optional): Tipo de procesamiento. Defaults to 'general'.
    
    Returns:
        pd.DataFrame: DataFrame con columnas adicionales
    """
    # Generar ID de localidad
    df['id_localidad'] = df.apply(lambda row: generar_id_localidad(row['Centro'], row['Almacén']), axis=1)
    
    # Establecer id_insumo
    if 'id_insumo' not in df.columns:
        df['id_sap'] = df['Material']  # Guardar id_sap original
        df['id_insumo'] = df['Material']
    
    # Calcular stock libre más calidad
    if 'Libre utilización' in df.columns and 'Inspecc.de calidad' in df.columns and tipo == 'general':
        df['stock_libre_mas_calidad'] = df['Libre utilización'] + df['Inspecc.de calidad']
    
    # Generar IDs compuestos
    df['id_localidad_insumo'] = df['id_localidad'] + df['id_insumo'].astype(str)
    df['id_localidad_sap'] = df['id_localidad'] + df['id_sap'].astype(str) if 'id_sap' in df.columns else df['id_localidad_insumo']
    
    return df

def generar_ids_y_stock_valor(df: pd.DataFrame, tipo: str = 'general') -> pd.DataFrame:
    """
    Genera columnas de identificación y calcula valores de stock.
    
    Args:
        df (pd.DataFrame): DataFrame de entrada
        tipo (str, optional): Tipo de procesamiento. Defaults to 'general'.
    
    Returns:
        pd.DataFrame: DataFrame agrupado con valores de stock
    """
    df = generar_ids_y_stock(df, tipo)
    
    # Calcular valor de stock libre más calidad
    if 'Valor libre util.' in df.columns and 'Valor en insp.cal.' in df.columns and tipo == 'general':
        df['valor_libre_mas_calidad'] = df['Valor libre util.'] + df['Valor en insp.cal.']
    
    return df.groupby('id_localidad')[['stock_libre_mas_calidad', 'valor_libre_mas_calidad']].sum().reset_index()

def generar_y_separar_mb52(df: pd.DataFrame, tipo: str = 'general') -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Genera columnas para DataFrame MB52 y lo separa en cuatro DataFrames según almacén.
    
    Args:
        df (pd.DataFrame): DataFrame de entrada
        tipo (str, optional): Tipo de procesamiento. Defaults to 'general'.
    
    Returns:
        Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]: DataFrames separados por almacén
    """
    df = generar_ids_y_stock(df, tipo)
    
    # Agrupar por id_localidad_insumo
    df_intermedio = df.groupby(['id_localidad', 'Almacén', 'id_insumo', 'id_localidad_insumo'])['stock_libre_mas_calidad'].sum().reset_index()
    
    def filter_and_rename(df, almacen, suffix):
        filtered = df[df['Almacén'] == almacen].copy()
        filtered = filtered.rename(columns={'stock_libre_mas_calidad': f'stock_libre_mas_calidad_{suffix}'})
        return filtered
    
    # Separar por almacenes
    df_mb52_produccion = filter_and_rename(df_intermedio, 'PI01', 'produccion')
    df_mb52_transito = filter_and_rename(df_intermedio, '', 'transito')
    df_mb52_hub = filter_and_rename(df_intermedio, 'L003', 'hub')
    df_mb52_general = df_intermedio[~df_intermedio['Almacén'].isin(['PI01', '', 'L003'])].copy()
    df_mb52_general = df_mb52_general.rename(columns={'stock_libre_mas_calidad': 'stock_libre_mas_calidad_general'})
    
    return df_mb52_produccion, df_mb52_transito, df_mb52_hub, df_mb52_general

def calcular_cobertura(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula métricas de cobertura para el DataFrame.
    
    Args:
        df (pd.DataFrame): DataFrame de entrada
    
    Returns:
        pd.DataFrame: DataFrame con columnas de cobertura
    """
    # Calcular stock de cobertura ideal
    df['stock_cobertura_ideal'] = (df['ratio_nominal'] * df['maxima_descarga']) / df['rendimiento'] * df['cobertura_ideal']
    
    # Calcular coberturas para diferentes tipos de stock
    for tipo in ['general', 'hub', 'transito', 'produccion']:
        col = f'stock_libre_mas_calidad_{tipo}'
        if tipo == 'general':
            df[col] = df[col].fillna(0)
        
        # Calcular stock acumulado
        acumulado = df[[c for c in df.columns if c.startswith('stock_libre_mas_calidad_') and c <= col]].sum(axis=1)
        
        # Calcular coberturas teóricas y reales
        df[f'cobertura_teorica_con_stock_{tipo}'] = np.where(
            df['ratio_nominal'] != 0,
            (acumulado * df['cobertura_ideal']) / df['stock_cobertura_ideal'].replace(0, 1),
            0
        )
        
        df[f'cobertura_real_{tipo}'] = np.where(
            df['ratio_nominal'] != 0,
            acumulado / df['consumo_diario'],
            0
        )
    
    # Manejar valores infinitos
    cobertura_cols = [col for col in df.columns if col.startswith('cobertura_real_')]
    df[cobertura_cols] = df[cobertura_cols].replace([np.inf, -np.inf], 0)
    
    return df

def procesar_datos(df_base: pd.DataFrame, df_mb52_produccion: pd.DataFrame, df_mb52_transito: pd.DataFrame, 
                   df_mb52_hub: pd.DataFrame, df_mb52_general: pd.DataFrame, df_consumo_total: pd.DataFrame, 
                   df_insumos: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Procesa los datos de entrada generando métricas de stock y cobertura.
    
    Args:
        df_base (pd.DataFrame): DataFrame base de insumos
        df_mb52_produccion (pd.DataFrame): Stock de producción
        df_mb52_transito (pd.DataFrame): Stock en tránsito
        df_mb52_hub (pd.DataFrame): Stock en hub
        df_mb52_general (pd.DataFrame): Stock general
        df_consumo_total (pd.DataFrame): Consumo total
        df_insumos (pd.DataFrame): Datos de insumos
    
    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: DataFrame procesado y vista por insumo
    """
    # Crear identificador único de localidad e insumo
    df_base['id_localidad_insumo'] = df_base['id_localidad'] + df_base['id_insumo'].astype(str)
    
    stock_columns = [
        'stock_libre_mas_calidad_produccion',
        'stock_libre_mas_calidad_transito',
        'stock_libre_mas_calidad_hub',
        'stock_libre_mas_calidad_general'
    ]
    
    # Hacer merge de stocks
    for df, col in zip([df_mb52_produccion, df_mb52_transito, df_mb52_hub, df_mb52_general], stock_columns):
        df_base = pd.merge(df_base, df[['id_localidad_insumo', col]], on='id_localidad_insumo', how='left')
    
    # Llenar valores nulos
    df_base = df_base.fillna(0)
    
    # Calcular stock total
    df_base['stock_libre_mas_calidad'] = df_base[stock_columns].sum(axis=1)
    
    # Calcular excedentes y faltantes
    df_base['excedentes'] = np.maximum(df_base['stock_libre_mas_calidad'] - df_base['stock_cobertura_ideal'], 0)
    df_base['faltantes'] = np.maximum(df_base['stock_cobertura_ideal'] - df_base['stock_libre_mas_calidad'], 0)
    
    # Merge con consumo total
    df_base = pd.merge(df_base, df_consumo_total[['id_localidad_insumo', 'consumo_diario', 'Cantidad', 'dias_de_pesca']], 
                       on='id_localidad_insumo', how='left')
    
    # Calcular cobertura
    df_base = calcular_cobertura(df_base)
    
    # Definir columnas para agregación
    columnas_a_sumar = [
        'stock_libre_mas_calidad', 'stock_cobertura_ideal', 'excedentes', 'faltantes', 
        'Cantidad', 'consumo_diario'
    ] + stock_columns
    
    # Filtrar columnas existentes
    columnas_a_sumar = [col for col in columnas_a_sumar if col in df_base.columns]
    
    # Columnas para promedios
    columnas_promedio = ['ratio_nominal', 'rendimiento', 'cobertura_ideal', 'maxima_descarga']
    columnas_promedio = [col for col in columnas_promedio if col in df_base.columns]
    
    # Preparar diccionario de agregación
    agg_dict = {col: 'sum' for col in columnas_a_sumar}
    agg_dict.update({col: 'mean' for col in columnas_promedio})
    
    # Campos descriptivos
    campos_primero = ['descripcion', 'nombre_insumo', 'familia', 'familia_2']
    campos_primero = [col for col in campos_primero if col in df_base.columns]
    if campos_primero:
        agg_dict.update({col: 'first' for col in campos_primero})
    
    # Agrupar por insumo
    df_vista_por_insumo = df_base.groupby(['id_insumo']).agg(agg_dict).reset_index()
    
    # Recalcular cobertura para vista por insumo
    if 'stock_cobertura_ideal' in df_vista_por_insumo.columns and all(col in df_vista_por_insumo.columns for col in ['ratio_nominal', 'maxima_descarga', 'rendimiento', 'cobertura_ideal']):
        df_vista_por_insumo = calcular_cobertura(df_vista_por_insumo)
    
    return df_base, df_vista_por_insumo

def consultar_pesca(inicio, final):
    """
    Consulta el reporte de pesca descargada a través de una API.
    
    Args:
        inicio (str): Fecha de inicio en formato YYYYMMDD
        final (str): Fecha de cierre en formato YYYYMMDD
    
    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: DataFrames de datos de pesca y días de producción
    """
    # Configuración de la solicitud a la API
    url = "https://node-flota-prd.cfapps.us10.hana.ondemand.com/api/reportePesca/ConsultarPescaDescargada"
    
    headers = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "es-ES,es;q=0.9",
        "Content-Type": "application/json;charset=UTF-8",
        "Origin": "https://tasaproduccion.launchpad.cfapps.us10.hana.ondemand.com",
    }
    
    payload = {
        "p_options": [],
        "options": [
            {
                "cantidad": "10",
                "control": "MULTIINPUT",
                "key": "FECCONMOV",
                "valueHigh": final,
                "valueLow": inicio
            }
        ],
        "p_rows": "",
        "p_user": "JHUAMANCIZA"
    }
    
    # Realizar solicitud
    response = requests.post(url, headers=headers, json=payload)
    
    if response.status_code == 200:
        # Procesar respuesta
        response_dict = response.json()
        
        # Crear DataFrame de datos
        df_datos = pd.DataFrame(response_dict['str_des'])
        df_datos['FCSAZ'] = pd.to_datetime(df_datos['FCSAZ'], format='%d/%m/%Y')
        
        # Calcular días únicos de pesca por planta
        unique_days_per_plant = df_datos[['WERKS', 'FCSAZ']].drop_duplicates()
        df_dias_produccion = unique_days_per_plant['WERKS'].value_counts().reset_index()
        df_dias_produccion.columns = ['id_localidad', 'dias_de_pesca']
        
        return df_datos, df_dias_produccion
    
    else:
        print(f"Error: {response.status_code}")
        return None, None

def realizar_proyeccion(df_pesca):
    """
    Realiza proyección de pesca usando el modelo Prophet.
    
    Args:
        df_pesca (pd.DataFrame): DataFrame con datos de pesca
    
    Returns:
        pd.DataFrame: DataFrame con proyección de pesca
    """
    # Preparar datos para Prophet
    df_pesca['ds'] = pd.to_datetime(df_pesca['FIDES'], dayfirst=True)
    df_pesca['CNPDS'] = pd.to_numeric(df_pesca['CNPDS'], errors='coerce').fillna(0)
    
    # Totalizar por día
    df_daily = df_pesca.groupby('ds')['CNPDS'].sum().reset_index()
    df_daily.columns = ['ds', 'y']
    
    # Crear y ajustar modelo Prophet
    model = Prophet()
    model.fit(df_daily)
    
    # Generar predicciones
    future = model.make_future_dataframe(periods=15)
    forecast = model.predict(future)
    
    # Ajustar valores negativos
    forecast[['yhat', 'yhat_lower', 'yhat_upper']] = forecast[['yhat', 'yhat_lower', 'yhat_upper']].apply(lambda x: x.clip(lower=0))
    
    # Agregar datos reales
    forecast = forecast.merge(df_daily, on='ds', how='left')
    forecast.rename(columns={'y': 'real_data'}, inplace=True)
    
    return forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper', 'real_data']]