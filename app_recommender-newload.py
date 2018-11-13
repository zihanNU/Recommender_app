from flask import Flask, jsonify, request
import pyodbc
import pandas as pd
from scipy import spatial
import geopy.distance
import numpy as np
import math
 
from pytictoc import TicToc
from scipy import stats
import datetime
now = datetime.datetime.now()

   
def Get_newload(date1,date2):
    #cn = pyodbc.connect('DRIVER={SQL Server};SERVER=reportingdatabases;DATABASE=Bazooka;trusted_connection=true')
    cn = pyodbc.connect('DRIVER={SQL Server};SERVER=reportingdatabases;DATABASE=Bazooka;uid=BazookaAccess;pwd=C@y0te')
    #cn = pyodbc.connect('DRIVER={SQL Server};SERVER=ANALYTICSPROD;DATABASE=Bazooka;trusted_connection=true')
    query="""
    SET NOCOUNT ON
    declare @date1 as date = ?
    declare @date2 as date = ?

	If(OBJECT_ID('tempdb.. #loadID') Is Not Null)
	Begin
	Drop Table  #loadID
	End
	create table #loadID (loadID int)
	insert into #loadID
	select
	L.Id  'loadID'


	from Bazooka.dbo.[load] L
	where L.StateType = 1 and L.progresstype=1 and L.totalrate>150
    and  L.LoadDate between @Date1 and @Date2  and L.Miles>0 and L.division in (1, 2)
    AND L.Mode = 1  
    AND L.ShipmentType not in (3,4,6,7)
    AND  L.[OriginStateCode] in (select [Code]  FROM [Bazooka].[dbo].[State] where [ID]<=51)
    AND  L.[DestinationStateCode] in (select [Code]  FROM [Bazooka].[dbo].[State] where [ID]<=51)

    select L.Id  'loadID', convert (date,L.loaddate) 'loaddate',l.TotalValue 'value',
    L.totalrate 'customer_rate', L.EquipmentType 'equipment',
    L.equipmentlength,
    L.miles,
	(case when LSP.[ScheduleCloseTime] = '1753-01-01' then 
	convert(datetime, CONVERT(date, LSP.LoadByDate)) + convert(datetime, CONVERT(time, LSP.CloseTime)) 
	else LSP.[ScheduleCloseTime] end) 'pu_appt',
    L.OriginCityName + '-'+L.OriginStateCode 'origin',
	L.DestinationCityName + '-'+L.DestinationStateCode 'destination',
    CityO.Longitude 'originLon',CityO.Latitude 'originLat',
	CityD.Longitude 'destinationLon',CityD.Latitude 'destinationLat',
	 RCO.ClusterNAME 'originCluster'
	,RCD.ClusterName 'destinationCluster'
    ,RCO.ClusterNAME+'-'+RCD.ClusterName 'corridor'
	,COALESCE(C.DandBIndustryId,0)  'industryID', 
	COALESCE(D.Code,'unknown') 'industry'
    from #loadID ID
	inner join bazooka.dbo.load L on ID.loadID = l.id
    inner join bazooka.dbo.LoadCustomer LCUS on L.id=LCUS.LoadID AND LCUS.Main = 1
	INNER JOIN Bazooka.dbo.Customer CUS ON LCUS.CustomerID = CUS.ID
	LEFT JOIN Bazooka.dbo.Customer PCUS ON CUS.ParentCustomerID = PCUS.ID
    inner join bazooka.dbo.loadstop LSP on LSP.id=L.OriginLoadStopID
    inner join bazooka.dbo.City CityO on CityO.id=l.origincityid --LSP.CityID
    inner join bazooka.dbo.City CityD on CityD.id=l.destinationcityid --.CityID
	LEFT JOIN Analytics.CTM.RateClusters RCO ON RCO.BazookaCityId = CityO.ID
    LEFT JOIN Analytics.CTM.RateClusters RCD ON RCD.BazookaCityId = CityD.ID
	left join bazooka.dbo.CustomerRelationshipManagement  C on C.CustomerID=LCUS.CustomerID
	left join bazooka.dbo.DandBIndustry D  on C.DandBIndustryId=D.DandBIndustryId
   where 
	CUS.Name not like 'UPS%'
	AND --COALESCE(PCUS.CODE,CUS.CODE) NOT IN ('UPSAMZGA','UPSRAILPEA')
    COALESCE(PCUS.ID,CUS.ID) NOT IN (84739,126657) 
    """
    newload=pd.read_sql(query,cn,params=[date1,date2] )

    return (newload)

def Get_newload2(date1,date2):
    #cn = pyodbc.connect('DRIVER={SQL Server};SERVER=reportingdatabases;DATABASE=Bazooka;trusted_connection=true')
    cn = pyodbc.connect('DRIVER={SQL Server};SERVER=reportingdatabases;DATABASE=Bazooka;uid=BazookaAccess;pwd=C@y0te')
    #cn = pyodbc.connect('DRIVER={SQL Server};SERVER=ANALYTICSPROD;DATABASE=Bazooka;trusted_connection=true')
    query="""
    SET NOCOUNT ON
    declare @date1 as date = ?
    declare @date2 as date = ?

    DECLARE @St AS TABLE (Code VARCHAR(50))
    INSERT INTO @St( Code )
    select [Code]  FROM [Bazooka].[dbo].[State] where [ID]<=51

	If(OBJECT_ID('tempdb.. #loadID') Is Not Null)
	Begin
	Drop Table  #loadID
	End
	create table #loadID (loadID int)
	insert into #loadID
	select
	L.Id  'loadID'


	from Bazooka.dbo.[load] L
	INNER JOIN @St SO ON SO.Code = L.OriginStateCode
        INNER JOIN @St SD ON SD.Code = L.DestinationStateCode
	where L.StateType = 1 and L.progresstype=1 and L.totalrate>150
    and  L.LoadDate between @Date1 and @Date2  and L.Miles>0 and L.division in (1, 2)
    AND L.Mode = 1  
    AND L.ShipmentType not in (3,4,6,7)
 

    select L.Id  'loadID', convert (date,L.loaddate) 'loaddate',l.TotalValue 'value',
    L.totalrate 'customer_rate', L.EquipmentType 'equipment',
    L.equipmentlength,
    L.miles,
	(case when LSP.[ScheduleCloseTime] = '1753-01-01' then 
	convert(datetime, CONVERT(date, LSP.LoadByDate)) + convert(datetime, CONVERT(time, LSP.CloseTime)) 
	else LSP.[ScheduleCloseTime] end) 'pu_appt',
    L.OriginCityName + '-'+L.OriginStateCode 'origin',
	L.DestinationCityName + '-'+L.DestinationStateCode 'destination',
    CityO.Longitude 'originLon',CityO.Latitude 'originLat',
	CityD.Longitude 'destinationLon',CityD.Latitude 'destinationLat',
	 RCO.ClusterNAME 'originCluster'
	,RCD.ClusterName 'destinationCluster'
    ,RCO.ClusterNAME+'-'+RCD.ClusterName 'corridor'
	,COALESCE(C.DandBIndustryId,0)  'industryID', 
	COALESCE(D.Code,'unknown') 'industry'
    from #loadID ID
	inner join bazooka.dbo.load L on ID.loadID = l.id
    inner join bazooka.dbo.LoadCustomer LCUS on L.id=LCUS.LoadID AND LCUS.Main = 1
	INNER JOIN Bazooka.dbo.Customer CUS ON LCUS.CustomerID = CUS.ID
	LEFT JOIN Bazooka.dbo.Customer PCUS ON CUS.ParentCustomerID = PCUS.ID
    inner join bazooka.dbo.loadstop LSP on LSP.id=L.OriginLoadStopID
    inner join bazooka.dbo.City CityO on CityO.id=l.origincityid --LSP.CityID
    inner join bazooka.dbo.City CityD on CityD.id=l.destinationcityid --.CityID
	LEFT JOIN Analytics.CTM.RateClusters RCO ON RCO.BazookaCityId = CityO.ID
    LEFT JOIN Analytics.CTM.RateClusters RCD ON RCD.BazookaCityId = CityD.ID
	left join bazooka.dbo.CustomerRelationshipManagement  C on C.CustomerID=LCUS.CustomerID
	left join bazooka.dbo.DandBIndustry D  on C.DandBIndustryId=D.DandBIndustryId
   where 
	CUS.Name not like 'UPS%'
	AND --COALESCE(PCUS.CODE,CUS.CODE) NOT IN ('UPSAMZGA','UPSRAILPEA')
    COALESCE(PCUS.ID,CUS.ID) NOT IN (84739,126657) 

    """
    newload=pd.read_sql(query,cn,params=[date1,date2] )
    return (newload)

 
if __name__=='__main__':

    date1_default = now.strftime("%Y-%m-%d")
    date2_default = (datetime.timedelta(1) + now).strftime("%Y-%m-%d")
    t=TicToc()
    t.tic()
    Get_newload(date1_default,date2_default)
    t.toc()
    t.tic()
    Get_newload2(date1_default,date2_default)
    t.toc()
