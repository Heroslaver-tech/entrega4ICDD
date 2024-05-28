from datetime import timedelta, date

import holidays
import numpy as np
import pandas as pd
from pandas import DataFrame
from googletrans import Translator
import calendar
from babel.dates import format_date


import pandas as pd


def load_country_translations(csv_file):
    translations = pd.read_csv(csv_file)
    # Convertir los códigos de país a minúsculas (o mayúsculas)
    translations['alpha2'] = translations['alpha2'].str.lower()
    return translations


def translate_country_names(country_codes, translations, target_lang):
    translated_names = []
    for code in country_codes:
        # Convertir el código de país a minúsculas (o mayúsculas)
        code_lower = code.lower()
        translation = translations.loc[translations['alpha2'] == code_lower, target_lang]
        translated_names.append(translation.values[0] if not translation.empty else "")
    return translated_names


def transformCurrency(dimCurrency: DataFrame) -> DataFrame:
    # Renombrar columnas
    dimCurrency.rename(columns={'CurrencyCode': 'CurrencyAlternateKey'}, inplace=True)
    dimCurrency.rename(columns={'Name': 'CurrencyName'}, inplace=True)

    # Eliminar la columna modifiedDate
    dimCurrency.drop(columns=['ModifiedDate'], inplace=True)

    # print(dimCurrency.head())
    return dimCurrency


def transformCustomer(args) -> DataFrame:
    customer, person, territory = args
    # Renombrar columnas
    customer.rename(columns={'AccountNumber': 'CustomerAlternateKey'}, inplace=True)

    # Eliminar la columna modifiedDate
    customer.drop(columns=['ModifiedDate'], inplace=True)
    customer.drop(columns=['rowguid'], inplace=True)
    person.drop(columns=['ModifiedDate'], inplace=True)
    person.drop(columns=['rowguid'], inplace=True)
    territory.drop(columns=['ModifiedDate'], inplace=True)
    territory.drop(columns=['rowguid'], inplace=True)

    customer = customer.merge(territory, left_on='TerritoryID', right_on='TerritoryID', how='left')
    print(customer.head())
    print(person.head())
    print(territory.head())


def transformGeography(args) -> DataFrame:
    address, stateProvince, countryRegion = args

    # Eliminar la columna modifiedDate
    stateProvince.drop(columns=['ModifiedDate', 'rowguid', 'IsOnlyStateProvinceFlag'], inplace=True)
    countryRegion.drop(columns=['ModifiedDate'], inplace=True)
    dimGeography = address.merge(stateProvince, left_on='StateProvinceID', right_on='StateProvinceID', how='left')
    dimGeography.rename(columns={'Name': 'StateProvinceName', 'TerritoryID': 'SalesTerritoryKey'}, inplace=True)
    dimGeography = dimGeography.merge(countryRegion, left_on='CountryRegionCode', right_on='CountryRegionCode',
                                      how='left')
    dimGeography.rename(columns={'Name': 'EnglishCountryName'}, inplace=True)

    # Cargar las traducciones de los países desde un archivo CSV
    country_translations_es = load_country_translations('utils/es.csv')
    country_translations_fr = load_country_translations('utils/fr.csv')

    dimGeography['SpanishCountryName'] = translate_country_names(dimGeography['CountryRegionCode'],
                                                                 country_translations_es, 'name')
    dimGeography['FrenchCountryName'] = translate_country_names(dimGeography['CountryRegionCode'],
                                                                country_translations_fr, 'name')

    # Reorganizar el orden de las columnas en dimGeography si es necesario
    desired_column_order = [
        'City', 'StateProvinceCode', 'StateProvinceName', 'CountryRegionCode', 'EnglishCountryName',
        'SpanishCountryName', 'FrenchCountryName', 'PostalCode', 'SalesTerritoryKey'
    ]

    # Verificar que todas las columnas deseadas están en dimGeography
    for column in desired_column_order:
        if column not in dimGeography.columns:
            print(f"Warning: Column '{column}' not found in dimGeography. It will be skipped in reordering.")

    # Reorganizar las columnas
    dimGeography = dimGeography[[col for col in desired_column_order if col in dimGeography.columns]]

    return dimGeography


def transformSalesTerritory(args) -> DataFrame:
    salesTerritory, countryRegion = args

    salesTerritory.rename(columns={'Name': 'SalesTerritoryRegion', 'TerritoryID': 'SalesTerritoryAlternateKey',
                                   'Group': 'SalesTerritoryGroup'}, inplace=True)
    dimSalesTerritory = salesTerritory.merge(countryRegion, left_on='CountryRegionCode', right_on='CountryRegionCode',
                                             how='left')
    dimSalesTerritory.drop(columns=['ModifiedDate', 'CountryRegionCode'], inplace=True)
    dimSalesTerritory.rename(columns={'Name': 'SalesTerritoryCountry'}, inplace=True)
    desired_column_order = [
        'SalesTerritoryAlternateKey', 'SalesTerritoryRegion', 'SalesTerritoryCountry', 'SalesTerritoryGroup'
    ]

    # Verificar que todas las columnas deseadas están en dimGeography
    for column in desired_column_order:
        if column not in dimSalesTerritory.columns:
            print(f"Warning: Column '{column}' not found in dimSalesTerritory. It will be skipped in reordering.")

    # Reorganizar las columnas
    dimSalesTerritory = dimSalesTerritory[[col for col in desired_column_order if col in dimSalesTerritory.columns]]

    return dimSalesTerritory


def transformDate() -> pd.DataFrame:
    dimDate = pd.DataFrame({"date": pd.date_range(start='1/1/2005', end='1/1/2009', freq='D').normalize()})
    dimDate["Date"] = dimDate["date"].dt.strftime('%Y%m%d')
    # Nueva columna FullDateAlternativeKey
    dimDate["FullDateAlternativeKey"] = dimDate["date"].dt.strftime('%Y-%m-%d')

    dimDate["DayNumberOfWeek"] = dimDate['date'].dt.dayofweek
    dimDate["EnglishDayNameOfWeek"] = dimDate['date'].dt.day_name()
    dimDate["SpanishDayNameOfWeek"] = dimDate['date'].apply(lambda x: format_date(x, format='EEEE', locale='es_ES'))
    dimDate["FrenchDayNameOfWeek"] = dimDate['date'].apply(lambda x: format_date(x, format='EEEE', locale='fr_FR'))

    dimDate["DayNumberOfMonth"] = dimDate["date"].dt.day
    dimDate["DayNumberOfYear"] = dimDate["date"].dt.day_of_year
    dimDate["WeekNumberOfYear"] = dimDate["date"].dt.isocalendar().week
    dimDate["EnglishMonthName"] = dimDate["date"].dt.month_name()
    dimDate["SpanishMonthName"] = dimDate['date'].apply(lambda x: format_date(x, format='MMMM', locale='es_ES'))
    dimDate["FrenchMonthName"] = dimDate['date'].apply(lambda x: format_date(x, format='MMMM', locale='fr_FR'))
    dimDate["MonthNumberOfYear"] = dimDate["date"].dt.month
    dimDate["CalendarQuarter"] = dimDate["date"].dt.quarter
    dimDate["CalendarYear"] = dimDate["date"].dt.year
    dimDate["CalendarSemester"] = dimDate["date"].dt.month.map(lambda x: 1 if x <= 6 else 2)

    fiscal_start_month = 10  # Supongamos que el año fiscal empieza en octubre
    dimDate["FiscalQuarter"] = dimDate["date"].apply(lambda x: ((x.month - fiscal_start_month) % 12) // 3 + 1)
    dimDate["FiscalYear"] = dimDate["date"].apply(lambda x: x.year if x.month >= fiscal_start_month else x.year - 1)
    dimDate["FiscalSemester"] = dimDate["FiscalQuarter"].apply(lambda x: 1 if x in [1, 2] else 2)

    # Eliminar la columna 'date'
    dimDate.drop(columns=["date"], inplace=True)
    return dimDate


import pandas as pd


def transformProduct( args) -> pd.DataFrame:

    producto, subcategoriaProducto,modeloProducto = args
    # Renombrar columnas
    subcategoriaProducto.rename(columns={"ProductSubcategoryID": "ProductAlternatekey"}, inplace=True)
    subcategoriaProducto.drop(columns=['rowguid', 'ModifiedDate','Name'], inplace=True)


    producto.rename(columns={"Name": "EnglishProductName"}, inplace=True)
    producto.rename(columns={"SellStartDate": "StartDate"}, inplace=True)
    producto.rename(columns={"SellEndDate": "EndDate"}, inplace=True)


    producto.sort_values(by='ProductModelID', inplace=True)
    modeloProducto.sort_values(by='ProductModelID', inplace=True)

    producto = producto.merge(modeloProducto, left_on='ProductModelID', right_on='ProductModelID', how='right')

    # Ordenar el DataFrame por la columna CurrencyAlternateKey
    producto.sort_values(by='ProductSubcategoryID', inplace=True)
    subcategoriaProducto.sort_values(by='ProductAlternatekey', inplace=True)

    dimProduct = producto.merge(subcategoriaProducto, left_on='ProductSubcategoryID', right_on='ProductAlternatekey', how='right')
    dimProduct.drop(columns=['ProductCategoryID'], inplace=True)
    dimProduct.drop(columns=['ProductAlternatekey'], inplace=True)
    dimProduct.drop(columns=['ProductModelID'], inplace=True)


    dimProduct.rename(columns={"ProductID": "ProductKey"}, inplace=True)
    dimProduct.rename(columns={"ProductNumber": "ProductAlternativeKey"}, inplace=True)
    dimProduct.rename(columns={"ProductSubcategoryID": "ProductSubcategoryKey"}, inplace=True)
    dimProduct.rename(columns={"Name": "ModelName"}, inplace=True)


    desired_column_order = [
            'ProductKey', 'ProductAlternativeKey', 'ProductSubcategoryKey', 'WeightUnitMeasureCode', 'SizeUnitMeasureCode', 'EnglishProductName', 'StandardCost', 'FinishedGoodsFlag', 'Color', 'SafetyStockLevel', 'ReorderPoint', 'ListPrice','Size', 'Weight','SizeUnitMeasureCode', 'DaysToManufacture','ProductLine','Class', 'Style', 'ModelName', 'StartDate', 'EndDate'
    ]

    # Verificar que todas las columnas deseadas están
    for column in desired_column_order:
        if column not in dimProduct.columns:
            print(f"Warning: Column '{column}' not found in dimSalesTerritory. It will be skipped in reordering.")

    # Reorganizar las columnas
    dimProduct = dimProduct[[col for col in desired_column_order if col in dimProduct.columns]]



    print(producto.head())
    # print(subcategoriaProducto.head())
    print(modeloProducto.head())
    print(dimProduct.head())
    #

    # Eliminar columnas no necesarias
    # dimProduct.drop(columns=['WeightUnitMeasureCode', 'SizeUnitMeasureCode', 'Color', 'SafetyStockLevel',
    #                          'ReorderPoint', 'SizeRange', 'ProductLine', 'DealerPrice', 'Class', 'Style',
    #                          'ModelName', 'LargePhoto', 'EnglishDescription', 'FrenchDescription', 'ChineseDescription',
    #                          'ArabicDescription', 'HebrewDescription', 'ThaiDescription', 'GermanDescription',
    #                          'JapaneseDescription', 'TurkishDescription', 'StartDate', 'EndDate', 'Status'],
    #                 inplace=True)
    #
    # return dimProduct

#
# def transform_medico(dim_medico: DataFrame) -> DataFrame:
#     dim_medico.replace({np.nan: 'no aplica', ' ': 'no aplica','':'no_aplica'}, inplace=True)
#     dim_medico["saved"] = date.today()
#     return dim_medico
#
#
# def transform_persona(args) -> DataFrame:
#     beneficiarios, cotizantes, cot_ben = args
#     cotizantes.rename(columns={'cedula': 'numero_identificacion'}, inplace=True)
#     cotizantes.drop(
#         columns=['direccion', 'tipo_cotizante', 'nivel_escolaridad', 'estracto', 'proviene_otra_eps', 'salario_base',
#                  'fecha_afiliacion', 'id_ips'], inplace=True)
#     cotizantes['tipo_documento'] = "cedula"
#     cotizantes['tipo_usuario'] = "cotizante"
#     cotizantes['grupo_familiar'] = cotizantes['numero_identificacion']
#     beneficiarios.drop(columns=['parentesco'], inplace=True)
#     beneficiarios.rename(columns={'tipo_identificacion': 'tipo_documento', 'id_beneficiario': 'numero_identificacion'},
#                          inplace=True)
#     beneficiarios['tipo_usuario'] = "beneficiario"
#     beneficiario = beneficiarios.merge(cot_ben, left_on='numero_identificacion', right_on='beneficiario', how='left')
#     beneficiario.rename(columns={'cotizante': 'grupo_familiar'}, inplace=True)
#     beneficiario.drop(columns=['beneficiario'], inplace=True)
#     dim_persona = pd.concat([beneficiario, cotizantes])
#     dim_persona["saved"] = date.today()
#     dim_persona.reset_index(drop=True, inplace=True)
#
#     return dim_persona
#
#
# def transform_servicio() -> DataFrame:
#     dim_servicio = pd.DataFrame({
#         'name': ['citas', 'hospitalizacion', 'urgencias'],
#         'descripcion': ['servicio de citas medicas', 'servicio de hospitalizacion', 'servicio de urgencias']
#     })
#     return dim_servicio
#
#

#
# def transform_trans_servicio(args) -> DataFrame:
#     df_citas, df_urgencias, df_hosp = args
#     df_hosp.rename(columns={'codigo_hospitalizacion': 'codigo_servicio'}, inplace=True)
#     df_urgencias.rename(columns={'codigo_urgencia': 'codigo_servicio'}, inplace=True)
#     df_citas.rename(columns={'codigo_cita': 'codigo_servicio'}, inplace=True)
#
#     df_citas['tipo_servicio'] = 'citas'
#     df_urgencias['tipo_servicio'] = 'urgencias'
#     df_hosp['tipo_servicio'] = 'hospitalizacion'
#
#     columns = ['codigo_servicio', 'id_usuario', 'id_medico', 'fecha_solicitud', 'fecha_atencion', 'hora_atencion',
#                'hora_solicitud', 'tipo_servicio']
#     trans_servicio = pd.concat([df_hosp, df_urgencias, df_citas], axis=0)
#     trans_servicio.head()
#     del_columns = set(trans_servicio.columns) - set(columns)
#     trans_servicio.drop(columns=del_columns, inplace=True)
#     trans_servicio['fecha_atencion'] = pd.to_datetime(trans_servicio['fecha_atencion'])
#     trans_servicio['fecha_solicitud'] = pd.to_datetime(trans_servicio['fecha_solicitud'])
#     trans_servicio['hora_atencion'] = trans_servicio['hora_atencion'].apply(
#         lambda x: timedelta(hours=x.hour, minutes=x.minute, seconds=x.second))
#     trans_servicio['hora_solicitud'] = trans_servicio['hora_solicitud'].apply(
#         lambda x: timedelta(hours=x.hour, minutes=x.minute, seconds=x.second))
#     trans_servicio['fecha_hora_atencion'] = trans_servicio['fecha_atencion'] + trans_servicio['hora_atencion']
#     trans_servicio['fecha_hora_solicitud'] = trans_servicio['fecha_solicitud'] + trans_servicio['hora_solicitud']
#     trans_servicio["saved"] = date.today()
#     trans_servicio.reset_index(drop=True, inplace=True)
#     return trans_servicio
# def transform_hecho_atencion(args) -> DataFrame:
#     df_trans, dim_persona, dim_medico, dim_servicio, dim_ips, dim_fecha = args
#     hecho_atencion = pd.merge(df_trans, dim_fecha[['date', 'key_dim_fecha']], left_on='fecha_atencion', right_on='date')
#     hecho_atencion.drop(columns=['date'], inplace=True)
#     hecho_atencion.rename(
#         columns={'key_dim_fecha': 'key_fecha_atencion', 'id_medico': 'cedula', 'id_usuario': 'numero_identificacion'},
#         inplace=True)
#     hecho_atencion = pd.merge(hecho_atencion, dim_fecha[['date', 'key_dim_fecha']], left_on='fecha_solicitud',
#                               right_on='date')
#     hecho_atencion.drop(columns=['date'], inplace=True)
#     hecho_atencion.rename(columns={'key_dim_fecha': 'key_fecha_solicitud'}, inplace=True)
#     hecho_atencion = hecho_atencion.merge(dim_persona[['key_dim_persona', 'numero_identificacion']])
#     hecho_atencion.drop(columns=['numero_identificacion'], inplace=True)
#     hecho_atencion = hecho_atencion.merge(dim_medico[['key_dim_medico', 'cedula', 'id_ips']])
#     hecho_atencion.drop(columns=['cedula'], inplace=True)
#     hecho_atencion = hecho_atencion.merge(dim_ips[['key_dim_ips', 'id_ips']])
#     hecho_atencion.drop(columns=['id_ips'], inplace=True)
#     hecho_atencion = hecho_atencion.merge(dim_servicio[['name', 'key_dim_servicio']], left_on='tipo_servicio',
#                                           right_on='name')
#     hecho_atencion.drop(columns=['name', 'tipo_servicio'], inplace=True)
#     hecho_atencion['tiempo_espera'] = hecho_atencion['fecha_hora_atencion'] - hecho_atencion['fecha_hora_solicitud']
#     hecho_atencion['tiempo_espera_dias'] = hecho_atencion['tiempo_espera'].dt.days
#     hecho_atencion['tiempo_espera_minutos'] = hecho_atencion['tiempo_espera'].dt.seconds // 60
#     hecho_atencion['tiempo_espera_horas'] = hecho_atencion['tiempo_espera'].dt.seconds // (60 * 60)
#     hecho_atencion['tiempo_espera_segundos'] = hecho_atencion['tiempo_espera'].dt.seconds
#     hecho_atencion["saved"] = date.today()
#
#     hecho_atencion.drop(
#         columns=['tiempo_espera', 'fecha_atencion', 'fecha_solicitud', 'hora_solicitud', 'hora_atencion',
#                  'fecha_hora_solicitud', 'fecha_hora_atencion', 'codigo_servicio'], inplace=True)
#
#     return hecho_atencion
