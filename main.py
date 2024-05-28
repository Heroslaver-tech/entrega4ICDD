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


#dimProduct
dimProduct = extract.extractProduct(db_aw)
dimProduct = transform.transformProduct(dimProduct)
load.load_data_product(dimProduct, wh_aw)

#dimPromotion
dimPromotion = extract.extractPromotion(db_aw)
dimPromotion = transform.transformPromotion(dimPromotion)
load.load_data_promotion(dimPromotion, wh_aw)

#dimProductSubcategory
dimProductSubcategory = extract.extractSubcategory(db_aw)
dimProductSubcategory = transform.transformSubCategory(dimProductSubcategory)
load.load_data_subcategory(dimProductSubcategory, wh_aw)



# dimEmployee

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

#dimReseller
dimReseller = extract.extractReseller(db_aw, wh_aw)
dimReseller = transform.transformReseller(dimReseller)
load.load_data_reseller(dimReseller,wh_aw)


#Hechos
#Hecho Internet Sales
#hechoInternetSales = extract.extractHechoInternet(wh_aw)
#hechoInternetSales = transform.transformHechoInternet(hechoInternetSales)
#load.load_hecho_internet(hechoInternetSales,wh_aw)

#Hecho Reseller Sales
#hechoResellerSales = extract.extractHechoReseller(wh_aw)
#hechoResellerSales = transform.transformHechoReseller(hechoResellerSales)
#load.load_hecho_reseller(hechoResellerSales,wh_aw)
