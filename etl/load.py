import pandas as pd
from pandas import DataFrame
from sqlalchemy.engine import Engine

def load_data_customer(dimCustomer : DataFrame, wh_aw):
    # Asignar 'date' como índice
    dimCustomer.set_index('CustomerID', inplace=True)

    dimCustomer.to_sql('DimCustomer', wh_aw, if_exists='append', index_label='CustomerKey')

def load_data_currency(dimCurrency : DataFrame, wh_aw):
    dimCurrency.to_sql('DimCurrency', wh_aw, if_exists='append', index_label='CurrencyKeys')

def load_data_geography(dimGeography : DataFrame, wh_aw):
    dimGeography.to_sql('DimGeography', wh_aw, if_exists='append', index_label='GeographyKey')

def load_data_sales_territory(dimSalesTerritory : DataFrame, wh_aw):
    dimSalesTerritory.to_sql('DimSalesTerritory', wh_aw, if_exists='append', index_label='SalesTerritoryKey')

def load_data_date(dimDate:DataFrame, wh_aw:Engine):
    # Asignar 'date' como índice
    dimDate.set_index('Date', inplace=True)

    # Guardar el DataFrame en la base de datos
    dimDate.to_sql('DimDate', wh_aw, if_exists='append', index_label='DateKey')





















#
#
# def load_data_ips(dim_ips : DataFrame,etl_conn):
#     dim_ips.to_sql('dim_ips', etl_conn, if_exists='append', index_label='key_dim_ips')
#
# def load_data_medico(dim_medico:DataFrame,etl_conn:Engine):
#     dim_medico.to_sql('dim_medico', etl_conn, if_exists='append', index_label='key_dim_medico')
#
#
# def load_data_persona(dim_persona:DataFrame,etl_conn:Engine):
#     dim_persona.to_sql('dim_persona',con=etl_conn,index_label='key_dim_persona',if_exists='append')
#
# def load_data_servicio(dim_servicio:DataFrame,etl_conn:Engine):
#     dim_servicio.to_sql('dim_servicio', etl_conn, if_exists='append', index_label='key_dim_servicio')
# def load_data_fecha(dim_fecha:DataFrame,etl_conn:Engine):
#     dim_fecha.to_sql('dim_fecha', etl_conn, if_exists='append', index_label='key_dim_fecha')
#
# def load_data_trans_servicio(trans_servicio:DataFrame,etl_conn:Engine):
#     trans_servicio.to_sql('trans_servicio', etl_conn, if_exists='append', index_label='key_trans_servicio')
#
# def load_hecho_atencion(hecho_atencion:DataFrame,etl_conn:Engine):
#     hecho_atencion.to_sql('hecho_atencion', etl_conn, if_exists='append', index_label='key_hecho_atencion')
#
# def load(table:DataFrame,etl_conn:Engine):
#     table.to_sql(f'dim_{table}', etl_conn, if_exists='append', index_label=f'key_{table}')