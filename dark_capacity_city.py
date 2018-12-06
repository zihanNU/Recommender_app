import pymongo
import pyodbc
import pprint
import pandas as pd

def GetCity():
    cn = pyodbc.connect('DRIVER={SQL Server};SERVER=ANALYTICSDev;DATABASE=ResearchScience;trusted_connection=true')
    query = """
       If(OBJECT_ID('tempdb..#citylist') Is Not Null)
        Begin
        Drop Table #citylist
        End
        Create Table #citylist (CityID int, CityName varchar (40), statecode varchar(20), ZipCode char(10), Latitude float, Longitude float,  CityState varchar (50),CityStateCounty varchar (50),
        StateCountry varchar (50), ClusterName varchar (100), CentroidLat float, CentroidLong float)
        Insert into #citylist


        select C.ID,
        C.name 'CityName', C.statecode, C.MainZipCode, C.Latitude,C.Longitude,
        C.name + ', '+ C.StateCode 'CityState',
        C.name + ', '+ C.StateCode+', '+'US' 'CityStateCounty',
        S.Name +', '+'USA' 'StateCountry',
         RCO.ClusterName,
         Rco.latitude 'Centroid-Lat',
         Rco.longitude 'Centroid-Long'
        from bazooka.dbo.City C
        left join bazooka.dbo.State S on S.id=C.StateID
        LEFT JOIN Analytics.CTM.RateClusters RCO ON RCO.BazookaCityId =  C.ID
        where C.stateID between 1 and 51



        --Create Table [ResearchScience].[dbo].[Recommendation_DarkCapacity_City]    (CityID int, CityName varchar (40), statecode varchar(20), ZipCode char(10), Latitude float, Longitude float,  CityState varchar (50),CityStateCounty varchar (50),
        --StateCountry varchar (50), ClusterName varchar (100), CentroidLat float, CentroidLong float)

        --insert into [ResearchScience].[dbo].[Recommendation_DarkCapacity_City]


        select * from #citylist
        where ClusterName is NULL
 """
    city = pd.read_sql(query, cn)
    return city
