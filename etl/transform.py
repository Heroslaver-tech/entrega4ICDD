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

    # Ordenar el DataFrame por la columna CurrencyAlternateKey
    dimCurrency.sort_values(by='CurrencyName', inplace=True, key=lambda col: col.str.lower())

    # Reiniciar el índice del DataFrame
    dimCurrency.reset_index(drop=True, inplace=True)
    dimCurrency.index += 1

    # print(dimCurrency.head())
    return dimCurrency


def transformCustomer(args) -> DataFrame:
    vCustomer, vDemographics, customer, person, dimGeography = args

    # Eliminar filas duplicadas
    vCustomer.drop_duplicates(inplace=True)
    vDemographics.drop_duplicates(inplace=True)
    customer.drop_duplicates(inplace=True)
    person.drop_duplicates(inplace=True)
    dimGeography.drop_duplicates(inplace=True)

    # Borrar Columnas innecesarias
    vCustomer.drop(
        columns=['AddressType', 'EmailPromotion', 'PhoneNumberType', 'StateProvinceName', 'PostalCode',
                 'CountryRegionName', 'Demographics'], inplace=True)
    vDemographics.drop(columns=['TotalPurchaseYTD'], inplace=True)

    # Renombrar Columnas
    vCustomer.rename(columns={'PhoneNumber': 'Phone'}, inplace=True)
    vDemographics.rename(
        columns={'Education': 'EnglishEducation', 'Occupation': 'EnglishOccupation', 'HomeOwnerFlag': 'HouseOwnerFlag'},
        inplace=True)
    customer.rename(columns={'PersonID': 'BusinessEntityID', 'AccountNumber': 'CustomerAlternateKey'}, inplace=True)

    # Fusionar los DataFrames
    vCustomer = vCustomer.merge(dimGeography, on='City', how='left')
    vCustomer = pd.merge(vCustomer, vDemographics, on='BusinessEntityID', how='left')
    customer = pd.merge(customer, person, on='BusinessEntityID', how='left')
    dimCustomer = pd.merge(customer, vCustomer, on='BusinessEntityID', how='left')

    # Ordenar el DataFrame por la columna CurrencyAlternateKey
    dimCustomer.sort_values(by=['CustomerID', 'CustomerAlternateKey'], inplace=True)

    # Borrar duplicados
    dimCustomer = dimCustomer.drop_duplicates(subset=['BusinessEntityID'])

    # Convertir valores booleanos en respuestas de 0 o 1
    dimCustomer['HouseOwnerFlag'] = dimCustomer['HouseOwnerFlag'].apply(
        lambda x: 1 if x else (0 if pd.notna(x) else np.nan))
    # Convertir la columna DateFirstPurchase al formato de fecha adecuado
    dimCustomer['DateFirstPurchase'] = pd.to_datetime(dimCustomer['DateFirstPurchase'])
    # Formatear la fecha en el formato AAAA-MM-DD
    dimCustomer['DateFirstPurchase'] = dimCustomer['DateFirstPurchase'].dt.strftime('%Y-%m-%d')

    # Reorganizar el orden de las columnas en dimCustomer
    desired_column_order = [
        'CustomerID', 'GeographyKey', 'CustomerAlternateKey', 'Title', 'FirstName', 'MiddleName',
        'LastName', 'NameStyle', 'BirthDate', 'MaritalStatus', 'Suffix', 'Gender', 'EmailAddress', 'YearlyIncome',
        'TotalChildren', 'NumberChildrenAtHome', 'EnglishEducation', 'EnglishOccupation', 'HouseOwnerFlag',
        'NumberCarsOwned', 'AddressLine1', 'AddressLine2', 'Phone', 'DateFirstPurchase'
    ]

    # Verificar que todas las columnas deseadas están en dimCustomer
    for column in desired_column_order:
        if column not in dimCustomer.columns:
            print(f"Warning: Column '{column}' not found in dimCustomer. It will be skipped in reordering.")

    # Reorganizar las columnas
    dimCustomer = dimCustomer[[col for col in desired_column_order if col in dimCustomer.columns]]

    # Resetear el índice
    dimCustomer.reset_index(drop=True, inplace=True)

    # Eliminar la primera fila de dimCustomer
    dimCustomer = dimCustomer.drop(dimCustomer.index[0])

    return dimCustomer


def transformGeography(args) -> DataFrame:
    address, stateProvince, countryRegion = args

    # Eliminar filas duplicadas en 'address'
    address.drop_duplicates(inplace=True)

    # Eliminar la columna modifiedDate
    stateProvince.drop(columns=['ModifiedDate', 'rowguid', 'IsOnlyStateProvinceFlag'], inplace=True)
    countryRegion.drop(columns=['ModifiedDate'], inplace=True)

    # Ordenar el DataFrame por la columna StateProvinceID
    address.sort_values(by='StateProvinceID', inplace=True)
    stateProvince.sort_values(by='StateProvinceID', inplace=True)

    dimGeography = address.merge(stateProvince, left_on='StateProvinceID', right_on='StateProvinceID', how='left')

    dimGeography.rename(columns={'Name': 'StateProvinceName', 'TerritoryID': 'SalesTerritoryKey'}, inplace=True)

    dimGeography.sort_values(by='CountryRegionCode', inplace=True, key=lambda col: col.str.lower())
    countryRegion.sort_values(by='CountryRegionCode', inplace=True, key=lambda col: col.str.lower())
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

    # Ordenar por 'CountryRegionCode' y luego por 'StateProvinceName'
    dimGeography.sort_values(by=['CountryRegionCode', 'StateProvinceName', 'City'], inplace=True,
                             key=lambda col: col.str.lower() if col.dtype == "object" else col)

    # Reiniciar el índice del DataFrame
    dimGeography.reset_index(drop=True, inplace=True)
    dimGeography.index += 1

    return dimGeography


def transformSalesTerritory(args) -> DataFrame:
    salesTerritory, countryRegion = args

    salesTerritory.rename(columns={'Name': 'SalesTerritoryRegion', 'TerritoryID': 'SalesTerritoryAlternateKey',
                                   'Group': 'SalesTerritoryGroup'}, inplace=True)

    # Ordenar el DataFrame por la columna CurrencyAlternateKey
    salesTerritory.sort_values(by='CountryRegionCode', inplace=True, key=lambda col: col.str.lower())
    countryRegion.sort_values(by='CountryRegionCode', inplace=True, key=lambda col: col.str.lower())
    dimSalesTerritory = salesTerritory.merge(countryRegion, left_on='CountryRegionCode', right_on='CountryRegionCode',
                                             how='left')
    dimSalesTerritory.drop(columns=['ModifiedDate', 'CountryRegionCode'], inplace=True)
    dimSalesTerritory.rename(columns={'Name': 'SalesTerritoryCountry'}, inplace=True)
    desired_column_order = [
        'SalesTerritoryAlternateKey', 'SalesTerritoryRegion', 'SalesTerritoryCountry', 'SalesTerritoryGroup'
    ]

    for column in desired_column_order:
        if column not in dimSalesTerritory.columns:
            print(f"Warning: Column '{column}' not found in dimSalesTerritory. It will be skipped in reordering.")

    # Reorganizar las columnas
    dimSalesTerritory = dimSalesTerritory[[col for col in desired_column_order if col in dimSalesTerritory.columns]]

    dummy_data = pd.DataFrame({
        'SalesTerritoryAlternateKey': [0],
        'SalesTerritoryRegion': ['NA'],
        'SalesTerritoryCountry': ['NA'],
        'SalesTerritoryGroup': ['NA']
    })
    dimSalesTerritory.sort_values(by='SalesTerritoryAlternateKey', inplace=True)

    dimSalesTerritory = pd.concat([dimSalesTerritory, dummy_data], ignore_index=True)

    # Reiniciar el índice del DataFrame
    dimSalesTerritory.reset_index(drop=True, inplace=True)
    dimSalesTerritory.index += 1

    return dimSalesTerritory


def transformDate() -> pd.DataFrame:
    dimDate = pd.DataFrame({"date": pd.date_range(start='1/1/2005', end='1/1/2009', freq='D').normalize()})
    dimDate["Date"] = dimDate["date"].dt.strftime('%Y%m%d')
    # Nueva columna FullDateAlternativeKey
    dimDate["FullDateAlternativeKey"] = dimDate["date"].dt.strftime('%Y-%m-%d')

    dimDate["DayNumberOfWeek"] = dimDate['date'].dt.dayofweek.apply(lambda x: (x + 1) % 7 + 1)
    dimDate["EnglishDayNameOfWeek"] = dimDate['date'].dt.day_name()
    dimDate["SpanishDayNameOfWeek"] = dimDate['date'].apply(lambda x: format_date(x, format='EEEE', locale='es_ES'))
    dimDate["FrenchDayNameOfWeek"] = dimDate['date'].apply(lambda x: format_date(x, format='EEEE', locale='fr_FR'))

    dimDate["DayNumberOfMonth"] = dimDate["date"].dt.day
    dimDate["DayNumberOfYear"] = dimDate["date"].dt.day_of_year
    dimDate["WeekNumberOfYear"] = dimDate["date"].dt.isocalendar().week.apply(lambda x: (x + 1) % 53 + 1)
    dimDate["EnglishMonthName"] = dimDate["date"].dt.month_name()
    dimDate["SpanishMonthName"] = dimDate['date'].apply(lambda x: format_date(x, format='MMMM', locale='es_ES'))
    dimDate["FrenchMonthName"] = dimDate['date'].apply(lambda x: format_date(x, format='MMMM', locale='fr_FR'))
    dimDate["MonthNumberOfYear"] = dimDate["date"].dt.month
    dimDate["CalendarQuarter"] = dimDate["date"].dt.quarter
    dimDate["CalendarYear"] = dimDate["date"].dt.year
    dimDate["CalendarSemester"] = dimDate["date"].dt.month.map(lambda x: 1 if x <= 6 else 2)

    fiscal_start_month = 1  # Supongamos que el año fiscal empieza en enero
    dimDate["FiscalQuarter"] = dimDate["date"].apply(lambda x: ((x.month - fiscal_start_month) % 12) // 3 + 1)
    dimDate["FiscalYear"] = dimDate["date"].apply(lambda x: x.year if x.month >= fiscal_start_month else x.year - 1)
    dimDate["FiscalSemester"] = dimDate["FiscalQuarter"].apply(lambda x: 1 if x in [1, 2] else 2)

    # Eliminar la columna 'date'
    dimDate.drop(columns=["date"], inplace=True)
    return dimDate


def transformReseller(args) -> DataFrame:
    customer, salesPerson, salesPersonQuotaHistory, vAddress, vContacts, vDemographics, dimGeography = args

    # renombrar
    customer.rename(columns={'PersonID': 'BusinessEntityID', 'AccountNumber': 'ResellerAlternateKey'}, inplace=True)
    vContacts.rename(columns={'PhoneNumber':'Phone'}, inplace=True)
    vDemographics.rename(columns={'Specialty':'ProductLine'}, inplace=True)
    vAddress.rename(columns={'Name':'ResellerName'}, inplace=True)

    # Ordenar
    salesPerson.sort_values(by='BusinessEntityID', inplace=True)
    vAddress.sort_values(by='BusinessEntityID', inplace=True)
    vContacts.sort_values(by='BusinessEntityID', inplace=True)
    vDemographics.sort_values(by='BusinessEntityID', inplace=True)
    customer.sort_values(by='BusinessEntityID', inplace=True)

    # Fusionar los DataFrames
    vAddress = vAddress.merge(dimGeography, on='City', how='left')
    dimReseller = vAddress.merge(salesPerson, on='BusinessEntityID', how='left')
    dimReseller = dimReseller.merge(vContacts, left_on='BusinessEntityID', right_on='BusinessEntityID', how='left')
    dimReseller = dimReseller.merge(vDemographics, left_on='BusinessEntityID', right_on='BusinessEntityID', how='left')
    dimReseller = dimReseller.merge(customer, left_on='BusinessEntityID', right_on='BusinessEntityID', how='left')

    dimReseller.drop(columns=['City'], inplace=True)
    dimReseller.drop_duplicates(inplace=True)

    # Agregar columnas de QuotaDate
    quota_info = salesPersonQuotaHistory.groupby('BusinessEntityID')['QuotaDate'].agg(
        FirstOrderYear='max',
        LastOrderYear='min',
        OrderMonth='count'
    ).reset_index()

    # Fusionar con dimReseller
    dimReseller = dimReseller.merge(quota_info, on='BusinessEntityID', how='left')

    # Reorganizar el orden de las columnas en dimGeography si es necesario
    desired_column_order = [
        'BusinessEntityID', 'GeographyKey', 'ResellerAlternateKey', 'Phone', 'ResellerName',
        'NumberEmployees', 'OrderMonth', 'FirstOrderYear', 'LastOrderYear', 'ProductLine', 'AddressLine1',
        'AddressLine2', 'AnnualSales', 'BankName', 'AnnualRevenue', 'YearOpened'
    ]

    # Verificar que todas las columnas deseadas están en dimGeography
    for column in desired_column_order:
        if column not in dimReseller.columns:
            print(f"Warning: Column '{column}' not found in dimGeography. It will be skipped in reordering.")

    # Reorganizar las columnas
    dimReseller = dimReseller[[col for col in desired_column_order if col in dimReseller.columns]]

    return dimReseller

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

    return dimProduct

def transformPromotion(args ) -> pd.DataFrame:
    promotion=args
    promotion.rename(columns={"SpecialOfferID": "PromotionKey"}, inplace=True)
    promotion.rename(columns={"Description": "EnglishPromotionName"}, inplace=True)
    promotion.rename(columns={"Type": "EnglishPromotionType"}, inplace=True)
    promotion.rename(columns={"Category": "EnglishPromotionCategory"}, inplace=True)

    promotion.drop(columns=['rowguid'], inplace=True)
    promotion.drop(columns=['ModifiedDate'], inplace=True)

def transformSubCategory(args ) -> pd.DataFrame:
    subcategoriaProducto=args

    subcategoriaProducto.drop(columns=['rowguid','ModifiedDate'], inplace=True)
    subcategoriaProducto.rename(columns={" ProductSubcategoryID": "ProductSubcategoryKey"}, inplace=True)
    subcategoriaProducto.rename(columns={" ProductCategoryID": "ProductCategoryKey"}, inplace=True)

    subcategoriaProducto.rename(columns={" Name": "EnglishProductSubcategoryName"}, inplace=True)
    desired_column_order = [
        'ProductSubcategoryID', 'Name', 'ProductCategoryID'
    ]

    # Verificar que todas las columnas deseadas están
    for column in desired_column_order:
        if column not in subcategoriaProducto.columns:
            print(f"Warning: Column '{column}' not found in dimSalesTerritory. It will be skipped in reordering.")

    # Reorganizar las columnas
    dimProductSubcategory = subcategoriaProducto[[col for col in desired_column_order if col in subcategoriaProducto.columns]]

    return dimProductSubcategory