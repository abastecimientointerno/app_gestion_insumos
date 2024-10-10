import pandas as pd
import numpy as np
from typing import Tuple, List
from streamlit_echarts import st_echarts


def generar_id_localidad(centro: str, almacen: str) -> str:
    """Genera un ID de localidad basado en el Centro y el Almacén."""
    return 'TCNO-HUB' if centro == 'TCNO' and almacen == 'HUB' else centro

def generar_ids_y_stock(df: pd.DataFrame, tipo: str = 'general') -> pd.DataFrame:
    """Genera las columnas necesarias para un DataFrame específico."""
    df['id_localidad'] = df.apply(lambda row: generar_id_localidad(row['Centro'], row['Almacén']), axis=1)
    df['id_insumo'] = df['Material']
    
    if 'Libre utilización' in df.columns and 'Inspecc.de calidad' in df.columns and tipo == 'general':
        df['stock_libre_mas_calidad'] = df['Libre utilización'] + df['Inspecc.de calidad']
    
    df['id_localidad_insumo'] = df['id_localidad'] + df['id_insumo'].astype(str)
    return df

def generar_ids_y_stock_valor(df: pd.DataFrame, tipo: str = 'general') -> pd.DataFrame:
    """Genera las columnas necesarias y calcula valores para un DataFrame específico."""
    df = generar_ids_y_stock(df, tipo)
    
    if 'Valor libre util.' in df.columns and 'Valor en insp.cal.' in df.columns and tipo == 'general':
        df['valor_libre_mas_calidad'] = df['Valor libre util.'] + df['Valor en insp.cal.']
    
    return df.groupby('id_localidad')[['stock_libre_mas_calidad', 'valor_libre_mas_calidad']].sum().reset_index()

def generar_y_separar_mb52(df: pd.DataFrame, tipo: str = 'general') -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Genera las columnas necesarias para el DataFrame MB52 y lo separa en cuatro DataFrames según el almacén."""
    df = generar_ids_y_stock(df, tipo)
    
    df = df.groupby(['id_localidad', 'Almacén', 'id_insumo', 'id_localidad_insumo'])['stock_libre_mas_calidad'].sum().reset_index()
    
    def filter_and_rename(df, almacen, suffix):
        filtered = df[df['Almacén'] == almacen].copy()
        filtered = filtered.rename(columns={'stock_libre_mas_calidad': f'stock_libre_mas_calidad_{suffix}'})
        return filtered
    
    df_mb52_produccion = filter_and_rename(df, 'PI01', 'produccion')
    df_mb52_transito = filter_and_rename(df, '', 'transito')
    df_mb52_hub = filter_and_rename(df, 'L003', 'hub')
    df_mb52_general = df[~df['Almacén'].isin(['PI01', '', 'L003'])].copy()
    df_mb52_general = df_mb52_general.rename(columns={'stock_libre_mas_calidad': 'stock_libre_mas_calidad_general'})
    
    return df_mb52_produccion, df_mb52_transito, df_mb52_hub, df_mb52_general

def filtrar_por_tipo_posicion(df: pd.DataFrame, tipo_col: str = 'Tipo de posición') -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Filtra el DataFrame ME2N en dos DataFrames separados."""
    return df[df[tipo_col] == 'V'].copy(), df[df[tipo_col] != 'V'].copy()

def calcular_cobertura(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula varias métricas de cobertura para el DataFrame."""
    df['stock_cobertura_ideal'] = (df['ratio_nominal'] * df['maxima_descarga']) / df['rendimiento'] * df['cobertura_ideal']
    
    for tipo in ['general', 'hub', 'transito', 'produccion']:
        col = f'stock_libre_mas_calidad_{tipo}'
        if tipo == 'general':
            df[col] = df[col].fillna(0)
        acumulado = df[[c for c in df.columns if c.startswith('stock_libre_mas_calidad_') and c <= col]].sum(axis=1)
        
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
    
    cobertura_cols = [col for col in df.columns if col.startswith('cobertura_real_')]
    df[cobertura_cols] = df[cobertura_cols].replace([np.inf, -np.inf], 0)
    
    return df

def procesar_datos(df_base: pd.DataFrame, df_mb52_produccion: pd.DataFrame, df_mb52_transito: pd.DataFrame, 
                   df_mb52_hub: pd.DataFrame, df_mb52_general: pd.DataFrame, df_consumo_total: pd.DataFrame, 
                   df_insumos: pd.DataFrame) -> pd.DataFrame:
    """Procesa y combina los diferentes DataFrames para generar el resultado final."""
    df_base['id_localidad_insumo'] = df_base['id_localidad'] + df_base['id_insumo'].astype(str)
    
    # Asegúrate de que todas las columnas necesarias estén presentes
    stock_columns = [
        'stock_libre_mas_calidad_produccion',
        'stock_libre_mas_calidad_transito',
        'stock_libre_mas_calidad_hub',
        'stock_libre_mas_calidad_general'
    ]
    
    for df, col in zip([df_mb52_produccion, df_mb52_transito, df_mb52_hub, df_mb52_general], stock_columns):
        df_base = pd.merge(df_base, df[['id_localidad_insumo', col]], on='id_localidad_insumo', how='left')
    
    df_base = df_base.fillna(0)
    
    # Calcula el stock_libre_mas_calidad total
    df_base['stock_libre_mas_calidad'] = df_base[stock_columns].sum(axis=1)
    
    df_base['excedentes'] = np.maximum(df_base['stock_libre_mas_calidad'] - df_base['stock_cobertura_ideal'], 0)
    df_base['faltantes'] = np.maximum(df_base['stock_cobertura_ideal'] - df_base['stock_libre_mas_calidad'], 0)
    
    df_base = pd.merge(df_base, df_consumo_total[['id_localidad_insumo', 'consumo_diario', 'Cantidad', 'dias_de_pesca']], 
                       on='id_localidad_insumo', how='left')
    
    df_base = calcular_cobertura(df_base)
    
    return pd.merge(df_base, df_insumos[['id_insumo', 'nombre_insumo', 'id_final', 'valor_redondeo']], 
                    on='id_insumo', how='left')
    


def graficar_proyeccion_pesca(df_proyeccion_pesca):
    # Limpiar los datos: reemplazar NaN con un valor (por ejemplo, 0)
    df_proyeccion_pesca = df_proyeccion_pesca.fillna(0)
    
    # Convertir las fechas a string para Echarts
    fechas = df_proyeccion_pesca['ds'].dt.strftime('%Y-%m-%d').tolist()
    
    # Extraer y redondear los datos para las proyecciones y los límites de confianza
    proyeccion = df_proyeccion_pesca['yhat'].round(2).tolist()
    limite_inferior = df_proyeccion_pesca['yhat_lower'].round(2).tolist()
    limite_superior = df_proyeccion_pesca['yhat_upper'].round(2).tolist()
    
    # Extraer y redondear los datos reales
    pesca_real = df_proyeccion_pesca['real_data'].round(2).tolist()

    # Configuración del gráfico Echarts
    options = {
        "title": {
            "text": "Proyección de Pesca vs Real",
            "left": "center",
            "textStyle": {
                "fontSize": 20,
                "fontWeight": "bold",
                "color": "#333"
            }
        },
        "tooltip": {
            "trigger": "axis",
            "axisPointer": {
                "type": "cross"
            },
            "formatter": [
                "{a0} <br/>{b0}: {c0} <br/>",
                "{a1} <br/>{b1}: {c1} <br/>",
                "{a2} <br/>{b2}: {c2} <br/>",
                "{a3} <br/>{b3}: {c3} <br/>"
            ]
        },
        "grid": {
            "left": "3%",
            "right": "4%",
            "bottom": "3%",
            "containLabel": True
        },
        "xAxis": {
            "type": "category",
            "data": fechas,
            "axisLabel": {
                "rotate": 45
            }
        },
        "yAxis": {
            "type": "value",
            "name": "Cantidad de Pesca",
            "nameLocation": "middle",
            "nameGap": 30,
            "axisLabel": {
                "formatter": "{value}",
                "color": "#666"
            }
        },
        "legend": {
            "data": ["Proyección", "Límite Inferior", "Límite Superior", "Pesca Real"],
            "top": "bottom",
            "textStyle": {
                "color": "#666"
            }
        },
        "series": [
            {
                "name": "Proyección",
                "type": "line",
                "data": proyeccion,
                "lineStyle": {"color": "#5470C6", "width": 3},
                "symbol": "circle",
                "symbolSize": 8
            },
            {
                "name": "Límite Inferior",
                "type": "line",
                "data": limite_inferior,
                "lineStyle": {"color": "#91CC75", "type": "dashed"},
                "symbol": "none"
            },
            {
                "name": "Límite Superior",
                "type": "line",
                "data": limite_superior,
                "lineStyle": {"color": "#91CC75", "type": "dashed"},
                "symbol": "none"
            },
            {
                "name": "Pesca Real",
                "type": "line",
                "data": pesca_real,
                "lineStyle": {"color": "#EE6666", "width": 3},
                "symbol": "circle",
                "symbolSize": 8
            }
        ],
        "animationDuration": 1000,  # Duración de la animación al mostrar el gráfico
        "height": "600px"  # Aumentar la altura del gráfico
    }

    # Renderizar el gráfico en la app
    st_echarts(options=options, height="600px")

# Ajustar el formato del tooltip
def get_tooltip_data(params):
    return [
        f"{params[0]['seriesName']} <br/>{params[0]['name']}: {params[0]['value']:.2f} <br/>",
        f"{params[1]['seriesName']} <br/>{params[1]['name']}: {params[1]['value']:.2f} <br/>",
        f"{params[2]['seriesName']} <br/>{params[2]['name']}: {params[2]['value']:.2f} <br/>",
        f"{params[3]['seriesName']} <br/>{params[3]['name']}: {params[3]['value']:.2f} <br/>"
    ]

# Asegúrate de llamar a esta función desde tu archivo principal