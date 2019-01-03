import pymongo
import pyodbc
import pprint
import pandas as pd
import numpy


def GetCity():
    cn = pyodbc.connect('DRIVER={SQL Server};SERVER=ANALYTICSDev;DATABASE=ResearchScience;trusted_connection=true')
    query = """
    set nocount on
       If(OBJECT_ID('tempdb..#citylist') Is Not Null)
        Begin
        Drop Table #citylist
        End
        Create Table #citylist (CityID int, CityName varchar (40), statecode varchar(20), ZipCode char(10), Latitude float, Longitude float,  CityState varchar (50),CityStateCounty varchar (50),
        StateCountry varchar (50), ClusterName varchar (100), createdate datetime, updatedate datetime)
        Insert into #citylist


        select C.ID,
        C.name 'CityName', C.statecode, C.MainZipCode, C.Latitude,C.Longitude,
        C.name + ', '+ C.StateCode 'CityState',
        C.name + ', '+ C.StateCode+', '+'US' 'CityStateCounty',
        S.Name +', '+'USA' 'StateCountry',
        COALESCE (RCO.ClusterName,'unknown')  'ClusterName',
        getdate(),
         getdate()
        from bazooka.dbo.City C
        left join bazooka.dbo.State S on S.id=C.StateID
        LEFT JOIN Analytics.CTM.RateClusters RCO ON RCO.BazookaCityId =  C.ID
        where C.stateID between 1 and 51



        --Create Table [ResearchScience].[dbo].[Recommendation_DarkCapacity_City]    (CityID int, CityName varchar (40), statecode varchar(20), ZipCode char(10), Latitude float, Longitude float,  CityState varchar (50),CityStateCounty varchar (50),
        --StateCountry varchar (50), ClusterName varchar (100), CentroidLat float, CentroidLong float)

        --insert into [ResearchScience].[dbo].[Recommendation_DarkCapacity_City]


        select * from #citylist
        where ClusterName like 'unknown'
 """
    city = pd.read_sql(query, cn)
    query_cluster="""
     select distinct clusterName, Lat,Long
     from Analytics.CTM.RateCluster_Map_Dashboard  
    """
    cluster = pd.read_sql(query_cluster, cn)
    return city,cluster

cities,cluster =GetCity()

city_df=pd.DataFrame(cities)
cluster_df=pd.DataFrame(cluster)




cn = pyodbc.connect('DRIVER={SQL Server};SERVER=ANALYTICSDev;DATABASE=ResearchScience;trusted_connection=true')
cursor = cn.cursor()
for i in range(1,len(city_df)):
    city=city_df.iloc[i]
    cluster_test=cluster_df
    cluster_test['dist']=abs(cluster_test['Lat']-city['Latitude'])+abs(cluster_test['Lat']-city['Latitude'])
    clustername=cluster_test.sort_values(by=['dist']).iloc[0]['clusterName']
    #print (city_df.iloc[i]['ClusterName'])
    city_df['ClusterName'].iloc[i]= clustername    #need to put the column name first and then put the index.
    #print (clustername)
    #print (city_df.iloc[i]['ClusterName'])
    querystring="""
    insert into 
    [ResearchScience].[dbo].[Recommendation_DarkCapacity_City]
    values (?, ?, ?, ?, ?, ?, ?, ?, ?,?, getdate(), getdate())"""
    cid= int(city_df.iloc[i]['CityID'])
    cursor.execute(querystring,( cid, city_df.iloc[i]['CityName'],city_df.iloc[i]['statecode'], city_df.iloc[i]['ZipCode'], city_df.iloc[i]['Latitude'],
    city_df.iloc[i]['Longitude'], city_df.iloc[i]['CityState'],city_df.iloc[i]['CityStateCounty'],city_df.iloc[i]['StateCountry'], city_df.iloc[i]['ClusterName']))
cn.commit()