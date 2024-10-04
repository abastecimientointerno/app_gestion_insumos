from functools import reduce
import pandas as pd
from api_portal import consultar_pesca
from generar_ids import generar_ids_y_stock, filtrar_por_tipo_posicion, generar_y_separar_mb52
import numpy as np
# Definir la ruta de los archivos
ruta_datasets = 'datasets.xlsx'
ruta_mb51 = 'MB51.xlsx'
ruta_mb52 = 'MB52.xlsx'
ruta_me2n = 'ME2N.xlsx'
df_capacidad_instalada = pd.read_excel(ruta_datasets, sheet_name='db_capacidad_instalada')
df_cuota = pd.read_excel(ruta_datasets, sheet_name='db_cuota')
df_ratios_planta_insumo = pd.read_excel(ruta_datasets, sheet_name='db_ratios_planta_insumo')
df_insumos = pd.read_excel(ruta_datasets, sheet_name='db_insumos')
df_mb51 = pd.read_excel(ruta_mb51, sheet_name='Sheet1')
df_mb52 = pd.read_excel(ruta_mb52, sheet_name='Sheet1')
df_me2n = pd.read_excel(ruta_me2n, sheet_name='Sheet1')
# Homologacion de ids
df_mb51 = generar_ids_y_stock(df_mb51)
df_mb52_produccion, df_mb52_transito, df_mb52_hub, df_mb52_general = generar_y_separar_mb52(df_mb52)
dfs_stocks = [df_mb52_produccion, df_mb52_transito, df_mb52_hub, df_mb52_general]
df_me2n = generar_ids_y_stock(df_me2n)
df_me2n_pt, df_me2n_oc = filtrar_por_tipo_posicion(df_me2n)
del df_me2n
df_base = pd.merge(df_ratios_planta_insumo, df_capacidad_instalada[['id_localidad', 'cip', 'rendimiento', 'cobertura_ideal','maxima_descarga','cobertura_meta']],
                   on='id_localidad', how='left')
df_base['stock_cobertura_ideal'] = (
    (df_base['ratio_nominal'] * df_base['maxima_descarga']) / df_base['rendimiento'] * df_base['cobertura_ideal'])
df_base['id_localidad_insumo'] = df_base['id_localidad'] + df_base['id_insumo'].astype(str)
df_base = pd.merge(df_base, df_mb52_produccion[['id_localidad_insumo', 'stock_libre_mas_calidad_produccion']], on='id_localidad_insumo', how='left') \
              .merge(df_mb52_transito[['id_localidad_insumo', 'stock_libre_mas_calidad_transito']], on='id_localidad_insumo', how='left') \
              .merge(df_mb52_hub[['id_localidad_insumo', 'stock_libre_mas_calidad_hub']], on='id_localidad_insumo', how='left') \
              .merge(df_mb52_general[['id_localidad_insumo', 'stock_libre_mas_calidad_general']], on='id_localidad_insumo', how='left') \
                .fillna(0)     

df_base = pd.merge(df_base, df_mb52[['id_localidad_insumo', 'stock_libre_mas_calidad']],
                   on='id_localidad_insumo', how='left')

df_base['stock_libre_mas_calidad'] = df_base['stock_libre_mas_calidad'].fillna(0)
df_base['excedentes'] = df_base.apply(
    lambda row: 0 if row['stock_libre_mas_calidad'] < row['stock_cobertura_ideal'] else row['stock_libre_mas_calidad'] - row['stock_cobertura_ideal'],
    axis=1
)
df_base['faltantes'] = df_base.apply(
    lambda row: 0 if row['stock_libre_mas_calidad'] > row['stock_cobertura_ideal'] else row['stock_cobertura_ideal'] - row['stock_libre_mas_calidad'],
    axis=1
)
#Cobertura
df_base['cobertura_teorica_con_stock_general'] = (df_base['stock_libre_mas_calidad_general'] * df_base['cobertura_ideal']) \
                                                 / df_base['stock_cobertura_ideal'].replace(0, 1)

df_base['cobertura_teorica_con_stock_general_hub'] = ((df_base['stock_libre_mas_calidad_general'] + df_base['stock_libre_mas_calidad_hub']) \
                                                      * df_base['cobertura_ideal']) \
                                                      / df_base['stock_cobertura_ideal'].replace(0, 1)

df_base['cobertura_teorica_con_stock_general_hub_transito'] = ((df_base['stock_libre_mas_calidad_general'] + df_base['stock_libre_mas_calidad_hub'] + \
                                                                df_base['stock_libre_mas_calidad_transito']) * df_base['cobertura_ideal']) \
                                                                / df_base['stock_cobertura_ideal'].replace(0, 1)

df_base['cobertura_teorica_con_stock_general_hub_transito_produccion'] = ((df_base['stock_libre_mas_calidad_general'] + df_base['stock_libre_mas_calidad_hub'] + \
                                                                          df_base['stock_libre_mas_calidad_transito'] + df_base['stock_libre_mas_calidad_produccion']) * df_base['cobertura_ideal']) \
                                                                          / df_base['stock_cobertura_ideal'].replace(0, 1)
df_consumo = df_mb51.copy()
df_consumo_total = df_consumo.groupby(['id_localidad', 'id_insumo']).agg({'Cantidad': 'sum'}).reset_index()
df_consumo_total['Cantidad'] = df_consumo_total['Cantidad'].abs()
inicio = "20240415"
final = "20240706"
df_datos, df_dias_produccion = consultar_pesca(inicio, final)
df_consumo_total = pd.merge(df_consumo_total, df_dias_produccion[['id_localidad', 'dias_de_pesca']],
                            on='id_localidad', how='left')
df_consumo_total['consumo_diario'] = df_consumo_total['Cantidad'] / df_consumo_total['dias_de_pesca']
df_consumo_total['consumo_diario'] = df_consumo_total['consumo_diario'].fillna(0)
df_consumo_total['id_localidad_insumo'] = df_consumo_total['id_localidad'].astype(str) + df_consumo_total['id_insumo'].astype(str)
df_base = pd.merge(df_base, df_consumo_total[['id_localidad_insumo', 'consumo_diario','Cantidad','dias_de_pesca']],
                   on='id_localidad_insumo', how='left')
df_base['cobertura_real_general'] = df_base['stock_libre_mas_calidad_general'] / df_base['consumo_diario']
df_base['cobertura_real_general'] = df_base['cobertura_real_general'].replace([np.inf, -np.inf], 0)
df_base['cobertura_real_general_hub'] = (df_base['stock_libre_mas_calidad_general'] + df_base['stock_libre_mas_calidad_hub']) / df_base['consumo_diario']
df_base['cobertura_real_general_hub'] =df_base['cobertura_real_general_hub'].replace([np.inf, -np.inf], 0)
df_base['cobertura_real_general_hub_transito'] = (df_base['stock_libre_mas_calidad_general'] + df_base['stock_libre_mas_calidad_hub']+ df_base['stock_libre_mas_calidad_transito']) / df_base['consumo_diario']
df_base['cobertura_real_general_hub_transito'] = df_base['cobertura_real_general_hub_transito'].replace([np.inf, -np.inf], 0)
df_base['cobertura_real_general_hub_transito_produccion'] = (df_base['stock_libre_mas_calidad_general'] + df_base['stock_libre_mas_calidad_hub']+ df_base['stock_libre_mas_calidad_transito']+df_base['stock_libre_mas_calidad_produccion']) / df_base['consumo_diario']
df_base['cobertura_real_general_hub_transito_produccion'] = df_base['cobertura_real_general_hub_transito_produccion'].replace([np.inf, -np.inf], 0)
df_resultado = pd.merge(
    df_base,
    df_insumos[['id_insumo', 'nombre_insumo', 'id_final', 'valor_redondeo']],
    on='id_insumo',
    how='left'
)
df_resultado = df_resultado[['id_localidad','id_insumo','nombre_insumo','stock_libre_mas_calidad_general','stock_cobertura_ideal','excedentes','faltantes','familia','cobertura_meta','cobertura_teorica_con_stock_general','cobertura_teorica_con_stock_general_hub','cobertura_teorica_con_stock_general_hub_transito','cobertura_teorica_con_stock_general_hub_transito_produccion','consumo_diario','dias_de_pesca','ratio_nominal','cip','rendimiento','cobertura_ideal','maxima_descarga','id_localidad_insumo','Cantidad','id_final','valor_redondeo','cobertura_real_general','cobertura_real_general_hub','cobertura_real_general_hub_transito','cobertura_real_general_hub_transito_produccion']]
with pd.ExcelWriter('resultados.xlsx') as writer:
    df_resultado.to_excel(writer, sheet_name='seguimiento_insumos', index=False)
    df_datos.to_excel(writer, sheet_name='seguimiento_pesca', index=False)