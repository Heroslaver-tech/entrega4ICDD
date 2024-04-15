import pandas as pd
from sqlalchemy import create_engine
import yaml
from etl import extract, transform, load





with open('config.yml', 'r') as f:
    config_co = yaml.safe_load(f)['CO_SA']
    config_etl = yaml.safe_load(f)['ETL_PRO']

# Construct the database URL
url_co = (f"{config_co['drivername']}://{config_co['user']}:{config_co['password']}@{config_co['host']}:"
          f"{config_co['port']}/{config_co['dbname']}")
url_etl = (f"{config_etl['drivername']}://{config_etl['user']}:{config_etl['password']}@{config_etl['host']}:"
           f"{config_etl['port']}/{config_etl['dbname']}")
# Create the SQLAlchemy Engine
co_sa = create_engine(url_co)
etl_conn = create_engine(url_etl)

# extract
dim_ips = extract.extract_ips(co_sa)
dim_persona = extract.extract_persona(co_sa)
dim_medico = extract.extract_medico(co_sa)
trans_servicio = extract.extract_trans_servicio(co_sa)

# transform
dim_ips = transform.transform_ips(dim_ips)
dim_persona = transform.transform_persona(dim_persona)
dim_medico = transform.transform_medico(dim_medico)
trans_servicio = transform.transform_trans_servicio(trans_servicio)
dim_fecha = transform.transform_fecha()
dim_servicio = transform.transform_servicio()

#load
load.load_data_ips(dim_ips,etl_conn)
load.load_data_fecha(dim_fecha,etl_conn)
load.load_data_servicio(dim_servicio,etl_conn)
load.load_data_persona(dim_persona,etl_conn)
load.load_data_medico(dim_medico,etl_conn)
load.load_data_trans_servicio(trans_servicio,etl_conn)

#hecho
hecho_atencion = extract.extract_hehco_atencion(etl_conn)
hecho_atencion = transform.transform_hecho_atencion(hecho_atencion)
load.load_hecho_atencion(hecho_atencion,etl_conn)
