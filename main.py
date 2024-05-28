import pandas as pd
from sqlalchemy import create_engine, inspect
import yaml
import urllib.parse
from etl import extract, transform, load
import psycopg2

with open('config.yml', 'r') as f:
    config = yaml.safe_load(f)
    config_db = config['DB_AW']
    config_wh = config['WH_AW']

# Construct the database URL
url_db = (f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + config_db['host'] + ';PORT=' + str(config_db['port']) + ';DATABASE=' + config_db['dbname'] + ';Trusted_Connection=yes')}")

url_wh = (f"{config_wh['drivername']}://{config_wh['user']}:{config_wh['password']}@{config_wh['host']}:"
           f"{config_wh['port']}/{config_wh['dbname']}")


# Create the SQLAlchemy Engine
db_aw = create_engine(url_db)

wh_aw = create_engine(url_wh)

# Crear dos inspectores, uno para cada base de datos
inspector_db_aw = inspect(db_aw)
inspector_wh_aw = inspect(wh_aw)

#specific_schema = 'Production'
#table_names = inspector_db_aw.get_table_names(schema=specific_schema)
#print(table_names)

tnames = inspector_wh_aw.get_table_names()

# extract
# dimPromotion = extract.extractPromotion(db_aw)
# dimProduct = extract.extractProduct(db_aw)
# dimSupplier = extract.extractSupplier(db_aw)
# dimEmployee = extract.extractEmployee(db_aw)
#

#dimReseller
dimReseller = extract.extractReseller(db_aw, wh_aw)
dimReseller = transform.transformReseller(dimReseller)
load.load_data_reseller(dimReseller,wh_aw)

#dimCurrency
dimCurrency = extract.extractCurrency(db_aw)
dimCurrency = transform.transformCurrency(dimCurrency)
load.load_data_currency(dimCurrency,wh_aw)

#dimDate
dimDate = transform.transformDate()
load.load_data_date(dimDate,wh_aw)

#dimSalesTerritory
dimSalesTerritory = extract.extractSalesTerritory(db_aw)
dimSalesTerritory = transform.transformSalesTerritory(dimSalesTerritory)
load.load_data_sales_territory(dimSalesTerritory,wh_aw)

#dimGepraphy
dimGeography = extract.extractGeography(db_aw)
dimGeography = transform.transformGeography(dimGeography)
load.load_data_geography(dimGeography, wh_aw)

#dimCustomer
dimCustomer = extract.extractCustomer(db_aw, wh_aw)
dimCustomer = transform.transformCustomer(dimCustomer)
load.load_data_customer(dimCustomer, wh_aw)


# #hecho
# hecho_atencion = extract.extract_hehco_atencion(etl_conn)
# hecho_atencion = transform.transform_hecho_atencion(hecho_atencion)
# load.load_hecho_atencion(hecho_atencion,etl_conn)