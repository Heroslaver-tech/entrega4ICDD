import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import MetaData


def extract(con):
    """
    :param con: the connection to the database
    :return:
    """


def extractProduct(con: Engine):
    """
    Extrae datos relacionados con productos de la base de datos AdventureWorks.

    :param con: Conexión del motor SQLAlchemy a la base de datos AdventureWorks
    :return: Diccionario que contiene DataFrames de las tablas relacionadas con productos
    """
    # Extraer datos del esquema Production
    producto_query = """
            SELECT ProductID, Name, ProductNumber, FinishedGoodsFlag, Color, SafetyStockLevel,ReorderPoint,ListPrice, Size, DaysToManufacture, Weight, Style, ProductSubcategoryID, ProductModelID, StandardCost, WeightUnitMeasureCode, ProductLine, Class, SellStartDate, SellEndDate
            FROM Production.Product
        """
    modelo_query = """
                SELECT ProductModelID, Name
                FROM Production.ProductModel
            """
    producto= pd.read_sql_query(producto_query, con)
    subcategoriaProducto = pd.read_sql_table('ProductSubcategory', con, schema='Production')
    modeloProducto = pd.read_sql_query(modelo_query, con)


    return [ producto,subcategoriaProducto,modeloProducto ]


def extractPromotion(con: Engine):
    promotion = pd.read_sql_table('SpecialOffer', con, schema='Sales')
    return promotion

def extractSubcategory(con: Engine):
    subcategoriaProducto = pd.read_sql_table('ProductSubcategory', con, schema='Production')

    return  subcategoriaProducto

def extractCurrency(con: Engine):
    """
    Extract data from database where the conexion established
    :param con:
    :return:
    """
    dimCurrency = pd.read_sql_table('Currency', con, schema='Sales')
    #print(dimCurrency.head())
    #dimCurrency.head()
    #dimCurrency.info()
    return dimCurrency

def extractCustomer(con: Engine, dw:Engine):
    # Leer las tablas excluyendo las columnas problemáticas
    customer_query = """
            SELECT CustomerID, PersonID, AccountNumber, TerritoryID
            FROM Sales.Customer
        """

    person_query = """
                SELECT BusinessEntityID, NameStyle
                FROM Person.Person
            """

    geography_query = """
                        SELECT "GeographyKey","City" 
                        FROM public."DimGeography"
                """

    vCustomer_query = """
                                SELECT *
                                FROM [Sales].[vIndividualCustomer]
                        """

    vDemographics_query = """
                                SELECT *
                                FROM [Sales].[vPersonDemographics]
                        """

    vCustomer = pd.read_sql_query(vCustomer_query, con)
    vDemographics = pd.read_sql_query(vDemographics_query, con)
    customer = pd.read_sql_query(customer_query, con)
    person = pd.read_sql_query(person_query, con)
    dimGeography = pd.read_sql_query(geography_query, dw)

    return [vCustomer, vDemographics, customer, person, dimGeography]

def extractGeography(con: Engine):
    # Leer las tablas excluyendo las columnas problemáticas
    address_query = """
        SELECT City, StateProvinceID, PostalCode
        FROM Person.Address
    """

    address = pd.read_sql_query(address_query, con)
    stateProvince = pd.read_sql_table('StateProvince', con, schema='Person')
    countryRegion = pd.read_sql_table('CountryRegion', con, schema='Person')

    return [address, stateProvince, countryRegion]


def extractSalesTerritory(con: Engine):
    # Leer las tablas excluyendo las columnas problemáticas
    salesTerritory_query = """
        SELECT TerritoryID, Name, CountryRegionCode, "Group"
        FROM Sales.SalesTerritory
    """

    salesTerritory = pd.read_sql_query(salesTerritory_query, con)
    countryRegion = pd.read_sql_table('CountryRegion', con, schema='Person')

    return [salesTerritory, countryRegion]

def extractReseller(con: Engine, dw:Engine):
    # Leer las tablas excluyendo las columnas problemáticas


    salesPerson_query = """
            SELECT BusinessEntityID, TerritoryID, SalesQuota
            FROM Sales.SalesPerson
        """

    vAddress_query = """
                SELECT BusinessEntityID, Name, City, AddressLine1, AddressLine2
                FROM [Sales].[vStoreWithAddresses]
            """

    vContacts_query = """
                SELECT BusinessEntityID, PhoneNumber
                FROM [Sales].[vStoreWithContacts]
            """

    vDemographics_query = """
                SELECT BusinessEntityID, AnnualSales, AnnualRevenue, BankName, YearOpened, Specialty, NumberEmployees
                FROM [Sales].[vStoreWithDemographics]
            """

    geography_query = """
                            SELECT "GeographyKey","City" 
                            FROM public."DimGeography"
                    """

    customer_query = """
                SELECT PersonID, AccountNumber
                FROM Sales.Customer
            """

    salesPersonQuotaHistory_query = """
                    SELECT BusinessEntityID, QuotaDate
                    FROM Sales.SalesPersonQuotaHistory
                """

    customer = pd.read_sql_query(customer_query, con)
    salesPerson = pd.read_sql_query(salesPerson_query, con)
    salesPersonQuotaHistory = pd.read_sql_query(salesPersonQuotaHistory_query, con)
    vAddress = pd.read_sql_query(vAddress_query, con)
    vContacts = pd.read_sql_query(vContacts_query, con)
    vDemographics = pd.read_sql_query(vDemographics_query, con)
    dimGeography = pd.read_sql_query(geography_query, dw)

    return [customer, salesPerson, salesPersonQuotaHistory, vAddress, vContacts, vDemographics, dimGeography]


