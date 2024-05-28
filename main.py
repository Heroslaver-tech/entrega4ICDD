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

tnames = inspector_wh_aw.get_table_names()

