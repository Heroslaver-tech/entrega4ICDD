import pandas as pd
from pandas import DataFrame
from sqlalchemy.engine import Engine


def load_data_product(dimProduct: DataFrame, wh_aw: Engine):
    print(dimProduct.head())
    dimProduct.set_index('ProductKey', inplace=True)
    dimProduct.to_sql('DimProduct', wh_aw, if_exists='append', index_label='ProductKey')

def load_data_promotion(dimPromotion: DataFrame, wh_aw: Engine):
    dimPromotion.set_index('PromotionKey', inplace=True)
    dimPromotion.to_sql('DimPromotion', wh_aw, if_exists='append', index_label='PromotionKey')

def load_data_subcategory(dimProductSubcategory: DataFrame, wh_aw: Engine):
    dimProductSubcategory.set_index('ProductSubcategoryID', inplace=True)
    dimProductSubcategory.to_sql('DimProductSubcategory', wh_aw, if_exists='append',
                                 index_label='ProductSubcategoryKey')

def load_data_customer(dimCustomer: DataFrame, wh_aw):
    # Asignar 'date' como índice
    dimCustomer.set_index('CustomerID', inplace=True)
    # Guardar el DataFrame en la base de datos
    dimCustomer.to_sql('DimCustomer', wh_aw, if_exists='append', index_label='CustomerKey')

def load_data_currency(dimCurrency: DataFrame, wh_aw):
    dimCurrency.to_sql('DimCurrency', wh_aw, if_exists='append', index_label='CurrencyKeys')

def load_data_geography(dimGeography: DataFrame, wh_aw):
    dimGeography.to_sql('DimGeography', wh_aw, if_exists='append', index_label='GeographyKey')

def load_data_sales_territory(dimSalesTerritory: DataFrame, wh_aw):
    dimSalesTerritory.to_sql('DimSalesTerritory', wh_aw, if_exists='append', index_label='SalesTerritoryKey')

def load_data_date(dimDate: DataFrame, wh_aw: Engine):
    # Asignar 'date' como índice
    dimDate.set_index('Date', inplace=True)
    # Guardar el DataFrame en la base de datos
    dimDate.to_sql('DimDate', wh_aw, if_exists='append', index_label='DateKey')

def load_data_reseller(dimReseller: DataFrame, wh_aw: Engine):
    # Asignar 'date' como índice
    dimReseller.set_index('BusinessEntityID', inplace=True)
    # Guardar el DataFrame en la base de datos
    dimReseller.to_sql('DimReseller', wh_aw, if_exists='append', index_label='ResellerKey')



