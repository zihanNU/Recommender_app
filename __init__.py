import pyodbc
import pandas as pd
from scipy import spatial
import geopy.distance
# from geopy.distance import vincenty
# from geopy.distance import geodesic
import numpy as np
import math
import datetime
from pytictoc import TicToc

class Load:
    def __init__(self,Id,carrierId,KPIScore,originDH,originDHLevels,PUGap,originCluster,destinationCluster,equipment,corridorVolume,oriCount,destCount,customerCount,customerAll,customerSize):
        self.Id=Id
        self.carrierId=carrierId
        self.KPIScore=KPIScore
        self.originDH=originDH
        self.originDHLevels=originDHLevels
        self.PUGap=PUGap
        self.originCluster=originCluster
        self.destinationCluster=destinationCluster
        self.equipment=equipment
        self.corridorVolume=corridorVolume
        self.oriCount=oriCount
        self.destCount=destCount
        self.customerCount=customerCount
        self.customerAll=customerAll
        self.customerSize=customerSize

class originDestinationEquipment:
    def  __init__(self,origin,destination,equipment):
        self.origin=origin
        self.destination=destination
        self.equipment=equipment

class carrier_ode_loads_kpi_std:   # ode here is a pd.df with 4 features, o, d, corridor and equip.
    def  __init__(self,carrier,ode,loads,kpi,std):
        self.carrier=carrier
        self.ode=ode
        self.loads=loads
        self.kpi=kpi
        self.std=std

# did not use this class, instead use pd.df to save the final results
class carrier_newload_score:
    def __init__(self,carrierid,loadid,score):
        self.carrierid=carrierid
        self.loadid=loadid
        self.score=score

#Give CarrierID
def Give_Carrier_Load_loading (CarrierID):
    cn = pyodbc.connect('DRIVER={SQL Server};SERVER=ANALYTICSPROD;DATABASE=Bazooka;trusted_connection=true')

    query= """
        set nocount on
        declare @CarrierID as int =?

	declare @CarrierDate1 as date = '2016-01-01'
	declare @CarrierDate2 as date = '2018-06-01'

	declare @HistDate1 as date = '2016-01-01'
	declare @HistDate2 as date = '2018-06-01'


	If(OBJECT_ID('tempdb..#Bounce_Reason') Is Not Null)
	Begin
	Drop Table #Bounce_Reason
	End
	Create Table #Bounce_Reason (FaultType int, ReasonType int, Reason varchar(30))
	Insert into #Bounce_Reason Values(0,0,	'Carrier')
	Insert into #Bounce_Reason Values(1,1,	'Carrier')
	Insert into #Bounce_Reason Values(1,2,	'Carrier')
	Insert into #Bounce_Reason Values(1,3,	'Carrier')
	Insert into #Bounce_Reason Values(1,4,	'Carrier')
	Insert into #Bounce_Reason Values(1,6,	'Carrier')
	Insert into #Bounce_Reason Values(1,12,	'Carrier') 
	Insert into #Bounce_Reason Values(1,13,	'Carrier')
	Insert into #Bounce_Reason Values(1,7,	'Carrier_Reps')
	Insert into #Bounce_Reason Values(1,8,	'Cust_Reps')
	Insert into #Bounce_Reason Values(2,1,	'Carrier')
	Insert into #Bounce_Reason Values(2,2,	'Carrier')
	Insert into #Bounce_Reason Values(2,3,	'Carrier')
	Insert into #Bounce_Reason Values(2,4,	'Carrier')
	Insert into #Bounce_Reason Values(2,5,	'Customer')
	Insert into #Bounce_Reason Values(2,7,	'Carrier_Reps')
	Insert into #Bounce_Reason Values(2,8,	'Cust_Reps')
	Insert into #Bounce_Reason Values(2,9,	'Carrier')
	Insert into #Bounce_Reason Values(2,10,	'Facility')
	Insert into #Bounce_Reason Values(2,13,	'Carrier_Reps')
	Insert into #Bounce_Reason Values(3,10,	'Facility')
	Insert into #Bounce_Reason Values(3,11,	'Facility')
	Insert into #Bounce_Reason Values(3,12,	'Customer')
	Insert into #Bounce_Reason Values(3,13,	'Customer')

	If(OBJECT_ID('tempdb..#Service') Is Not Null)
	Begin
	Drop Table #Service
	End
	Create Table #Service ( LoadID int, CarrierID int, PUScore int, DelScore int)
	Insert into #Service
 
	select LoadID,
	Carrierid, 
	case when datediff(minute,PU_Appt,PU_Arrive)<=60 then 25
	when datediff(minute,PU_Appt,PU_Arrive)<= 120 then 20
	when datediff(day,PU_Appt,PU_Arrive)=0 then 10
	else 5 end 'PU',
	case when datediff(minute,DO_Appt,DO_Arrive)<=60 then 25
	when datediff(minute,DO_Appt,DO_Arrive)<= 120 then 20
	when datediff(day,DO_Appt,DO_Arrive)=0 then 10
	else 5 end 'Del'
	from (
	select  L.id 'LoadID', 
	LCAR.CarrierID, 
	(case when LSP.[ScheduleCloseTime] = '1753-01-01' then 
	convert(datetime, CONVERT(date, LSP.LoadByDate)) + convert(datetime, CONVERT(time, LSP.CloseTime)) 
	else LSP.[ScheduleCloseTime] end) 'PU_Appt',
	LSP.[ArriveDateTime] 'PU_Arrive'
	, case when LSD.[ScheduleCloseTime] = '1753-01-01' then 
	convert(datetime, CONVERT(date, LSD.DeliverByDate)) + convert(datetime, CONVERT(time, LSD.CloseTime)) 
	else LSD.[ScheduleCloseTime] end 'DO_Appt',
	LSD.[ArriveDateTime] 'DO_Arrive' 
	FROM Bazooka.dbo.[Load] L
	INNER JOIN Bazooka.dbo.LoadCarrier LCAR ON LCAR.LoadID = L.ID and LCAR.Main = 1 and LCAR.IsBounced = 0
	inner join Bazooka.dbo.loadstop LSP on  LSP.ID=L.OriginLoadStopID
	inner join Bazooka.dbo.loadstop LSD on  LSD.ID=L.DestinationLoadStopID
	WHERE L.Mode = 1 AND    L.LoadDate between @CarrierDate1 and @CarrierDate2 and L.Miles>0 and LCAR.CarrierID= @CarrierID  
	) X

	If(OBJECT_ID('tempdb..#Bounce') Is Not Null)
	Begin
	Drop Table #Bounce
	End
	Create Table #Bounce ( LoadID int, CarrierID int,  Offer int, Accept int, Bounce int, OriginDH decimal(8,2), EmptyTime datetime)
	Insert into #Bounce

	select
	L.ID, LCAR.CarrierID, 1 'Offer',1 'Accepted Offers', 
	sum (case when BR.Reason like 'Carrier' then 1 else 0 end) 'Bounce',
	min(case when LCAR.ActualDistance<1 then 1 else LCAR.ActualDistance end)   'OriginDH',
	case when convert (date, max(LCAR.ActualDateTime))='1753-01-01' then getdate() else  max(LCAR.ActualDateTime) end 'EmptyTime'
	FROM Bazooka.dbo.[Load] L
	INNER JOIN Bazooka.dbo.LoadCarrier LCAR ON LCAR.LoadID = L.ID  
	left join Bazooka.dbo.LoadChangeLog Log_B on  Log_B.ChangeType=4 and Log_B.EntityID=LCAR.ID and LCAR.IsBounced=1
	left join #Bounce_Reason BR on BR.FaultType=Log_B.FaultType and BR.ReasonType=Log_B.ReasonType 
	WHERE L.Mode = 1 AND    L.LoadDate between @CarrierDate1 and @CarrierDate2 and L.Miles>0 and LCAR.CarrierID=@CarrierID   and L.ProgressType>=7    
	group by L.id, LCAR.CarrierID
	order by Bounce
 


	If(OBJECT_ID('tempdb..#Offer') Is Not Null)
	Begin
	Drop Table #Offer
	End
	Create Table #Offer( LoadID int, CarrierID int, Offer int, Cost money, Ask money,  BadOffer int,OriginDH int, AvailableTime datetime, Rnk int)
	Insert into #Offer
	select 
	O.LoadID, CarrierID, 1 'Offer',LRD.Cost, Ask, 
	case when  Ask >lrd.Cost*0.9 then 1 else 0 end  'Badoffer',
	case when O.MilesToOrigin<1 then 1 else O.MilesToOrigin end 'OriginDH',
	convert(datetime, CONVERT(date,O.CanLoadDate))+convert(datetime, CONVERT(time,O.CanLoadTime)) 'AvailableTime',
	RANK() over (partition by O.LoadID, O.CarrierID order by O.CreateDate desc) 'rnk'
	from bazooka.dbo.Offer O
	inner join Bazooka.dbo.[Load] L on O.LoadID = L.ID
	inner join Bazooka.dbo.LoadCustomer LCUS on LCUS.LoadID = L.ID and LCUS.Main = 1
	inner join (select entityid, SUM(amount) 'Cost' from Bazooka.dbo.LoadRateDetail 
	where EntityType = 12 and EDIDataElementCode IN  ('405','FR',  'PM' ,'MN') and CreateDate > '2018-01-01' Group by entityid) LRD on LRD.entityid = lcus.id
	--inner join #Cost C on C.LoadID=O.LoadID
	where O.Carrierid=@CarrierID   and O.LoadDate between @CarrierDate1 and @CarrierDate2 and  
	Ask>0 and LRD.Cost > 150 and  L.Mode = 1  and L.ProgressType>=7  
 

	If(OBJECT_ID('tempdb..#Carrier_CustID') Is Not Null)
	Begin
	Drop Table #Carrier_CustID
	End
	Create Table #Carrier_CustID (LoadID int,  CustID int)
	Insert into #Carrier_CustID
	select COALESCE(B.LoadID,O.LoadID)   'LoadID',
	CustomerID
	from #Bounce  B
	full join #Offer O on B.LoadID=O.LoadID and B.CarrierID=O.CarrierID
	inner join bazooka.dbo.LoadCustomer LCUS on LCUS.LoadID = COALESCE(B.LoadID,O.LoadID) 
	---End of Load-Carrier KPI Score


	---Start of Carrier Features

	If(OBJECT_ID('tempdb..#Carrier_HistLoad') Is Not Null)
	Begin
	Drop Table #Carrier_HistLoad
	End
	Create Table #Carrier_HistLoad (LoadID int,  CustID int, Origin varchar (50), Destination varchar(50), Equip varchar (20), OriginCluster varchar (50), DestinationCluster varchar (50), Corridor varchar (100))
	Insert into #Carrier_HistLoad

  
	select L.id 'LoadID',  
	LCUS.CustomerID  'CustID'
	--,Miles
	, L.OriginCityName + ', ' + L.OriginStateCode  'Origin'
	,L.DestinationCityName + ', ' + L.DestinationStateCode  'Destination'
	--,L.TotalValue
	,case when  l.equipmenttype like '%V%' then 'V' when  l.equipmenttype like 'R' then 'R' else 'other' end Equipment
	,RCO.ClusterNAME 'OriginCluster'
	,RCD.ClusterName 'DestinationCluster'
	,RCO.ClusterNAME+'-'+RCD.ClusterName  'Corridor'
	FROM Bazooka.dbo.[Load] L
	INNER JOIN Bazooka.dbo.LoadCarrier LCAR ON LCAR.LoadID = L.ID and LCAR.Main = 1 and LCAR.IsBounced = 0
	--INNER JOIN Bazooka.dbo.Carrier CAR ON CAR.ID = LCAR.CarrierID
	INNER JOIN Bazooka.dbo.LoadCustomer LCUS ON LCUS.LoadID = L.ID AND LCUS.Main = 1 
	--INNER JOIN Bazooka.dbo.Customer CUS ON LCUS.CustomerID = CUS.ID
	--LEFT JOIN Bazooka.dbo.Customer PCUS ON CUS.ParentCustomerID = PCUS.ID
	INNER JOIN bazooka.dbo.LoadRate LR ON LR.LoadID = L.ID AND LR.EntityType = 13 AND LR.EntityID = LCAR.ID and LR.OriginalQuoteRateLineItemID=0
	--inner join bazooka.dbo.loadstop LS on LS.id=L.OriginLoadStopID
	LEFT JOIN Analytics.CTM.RateClusters RCO ON RCO.Location = L.OriginCityName + ', ' + L.OriginStateCode  
	LEFT JOIN Analytics.CTM.RateClusters RCD ON RCD.Location = L.DestinationCityName + ', ' + L.DestinationStateCode  
	WHERE L.StateType = 1
	and  L.LoadDate between @HistDate1 and @HistDate2  and L.Miles>0 
	AND L.Mode = 1 AND LCAR.CarrierID=@CarrierID
	AND L.ShipmentType not in (3,4,6,7)
	AND (CASE WHEN L.EquipmentType LIKE '%V%' THEN 'V' ELSE L.EquipmentType END) IN ('V', 'R')
	--AND CAR.ContractVersion NOT IN ('TMS FILE', 'UPSDS CTM', 'UPSCD CTM') --Exclude Managed Loads
	--AND COALESCE(PCUS.CODE,CUS.CODE) NOT IN ('UPSAMZGA','UPSRAILPEA')
	AND L.TotalRAte >= 150 AND L.TotalCost >= 150
	AND  L.[OriginStateCode] in (select [Code]  FROM [Bazooka].[dbo].[State] where [ID]<=51) 
	AND  L.[DestinationStateCode] in (select [Code]  FROM [Bazooka].[dbo].[State] where [ID]<=51) 
	--and car.Name not like 'UPS%'
	order by Origin,Destination
 
 

	If(OBJECT_ID('tempdb..#CustSize') Is Not Null)
	Begin
	Drop Table #CustSize
	End
	Create Table #CustSize (CustID int, Count_ALL int)
	Insert into #CustSize

	select customerID,
	count(loadid)
	from bazooka.dbo.LoadCustomer LCU
	inner join bazooka.dbo.load L on L.id=LCU.LoadID
	where L.StateType = 1 and L.ProgressType >=7 and l.Mode = 1 and L.LoadDate between @HistDate1 and @HistDate2 
	and (l.equipmenttype like '%V%' or l.equipmenttype like 'R')
	AND L.TotalRAte >= 150 AND L.TotalCost >= 150
	AND  L.[OriginStateCode] in (select [Code]  FROM [Bazooka].[dbo].[State] where [ID]<=51) 
	AND  L.[DestinationStateCode] in (select [Code]  FROM [Bazooka].[dbo].[State] where [ID]<=51) 
	and customerID in(select distinct CustID  from #Carrier_CustID)
	group by CustomerID

	If(OBJECT_ID('tempdb..#Carrier_Cust') Is Not Null)
	Begin
	Drop Table #Carrier_Cust
	End
	Create Table #Carrier_Cust (CustID int, Count_Cus int, Count_ALL int)
	Insert into #Carrier_Cust
	select distinct #Carrier_HistLoad.CustID,
	count(loadid) 'Count_Cus'
	,#CustSize.Count_ALL 'Count_ALL'
	from #Carrier_HistLoad
	inner join #CustSize on #CustSize.CustID= #Carrier_HistLoad.CustID
	group by #Carrier_HistLoad.CustID, #CustSize.Count_ALL
	order by 2 desc

	If(OBJECT_ID('tempdb..#Carrier_Corridor') Is Not Null)
	Begin
	Drop Table #Carrier_Corridor
	End
	Create Table #Carrier_Corridor (Corridor varchar (50), Count_Corridor int)
	Insert into #Carrier_Corridor
	select distinct corridor,
	count(loadid) 'Count_Corridor'
	from #Carrier_HistLoad
	group by Corridor
	order by 2 desc

	If(OBJECT_ID('tempdb..#Carrier_Origin') Is Not Null)
	Begin
	Drop Table #Carrier_Origin
	End
	Create Table #Carrier_Origin (OriginCluster varchar (50), Count_Origin int)
	Insert into #Carrier_Origin
	select distinct OriginCluster,
	count(loadid) 'Count_Origin'
	from #Carrier_HistLoad
	group by OriginCluster
	order by 2 desc


	If(OBJECT_ID('tempdb..#Carrier_Dest') Is Not Null)
	Begin
	Drop Table #Carrier_Dest
	End
	Create Table #Carrier_Dest (DestinationCluster varchar (50), Count_Dest int)
	Insert into #Carrier_Dest
	select distinct DestinationCluster,
	count(loadid) 'Count_Dest'
	from #Carrier_HistLoad
	group by DestinationCluster
	order by 2 desc
	---End of Carrier Features
  


	select * from (
	select  COALESCE(B.LoadID,O.LoadID)   'loadID',
	COALESCE(B.CarrierID,O.CarrierID)    'carrierID', L.hot 'hot',
	O.cost 'customer_rate',
	case when  B.Accept=1 then l.totalcost else o.Ask  end 'carrier_cost',
	L.miles, (case when  B.Accept=1 then l.totalcost else o.Ask end)/(L.miles+COALESCE(O.OriginDH,B.OriginDH) )  'rpm',
	--COALESCE(S.PUScore,0)          'puScore',
	--COALESCE(S.DelScore,0)            'delScore',
	--Coalesce(O.Offer, B.Offer)*40    'offer',
	--COALESCE(B.Accept,0)*10    'offerAccept' ,
	--COALESCE(B.Bounce,0)*(-20)     'bounce'  ,
	--COALESCE(O.BadOffer,0)*-20     'badOffer',
	COALESCE(S.PUScore,0) +       COALESCE(S.DelScore,0)  +
	Coalesce(O.Offer, B.Offer)*40  +
	COALESCE(B.Accept,0)*10   +
	COALESCE(B.Bounce,0)*(-20)     +
	COALESCE(O.BadOffer,0)*-20     'kpiScore',
	COALESCE(O.OriginDH,B.OriginDH )   'originDH',
	--case when COALESCE(O.OriginDH,B.OriginDH )<=10 then 10
	--when COALESCE(O.OriginDH,B.OriginDH )<=50 then 50
	--when COALESCE(O.OriginDH,B.OriginDH )<=100 then 100
	--else 200 end 'originDH-levels',
	--COALESCE(O.AvailableTime,B.EmptyTime) 'Available',
	--case when LSP.[ScheduleCloseTime] = '1753-01-01' then
	--convert(datetime, CONVERT(date, LSP.LoadByDate)) + convert(datetime, CONVERT(time, LSP.CloseTime))
	--else LSP.[ScheduleCloseTime] end  'PU_Appt',
	datediff(hour,COALESCE(O.AvailableTime,B.EmptyTime),case when LSP.[ScheduleCloseTime] = '1753-01-01' then
	convert(datetime, CONVERT(date, LSP.LoadByDate)) + convert(datetime, CONVERT(time, LSP.CloseTime))
	else LSP.[ScheduleCloseTime] end) 'pu_GAP',
	--datediff(minute,COALESCE(O.AvailableTime,S.EmptyTime),S.PU_Appt) 'PU_GAP',
	--CUS.name 'CustomerName'
	RCO.ClusterNAME 'originCluster'
	,RCD.ClusterName 'destinationCluster'
	,RCO.ClusterNAME+'-'+RCD.ClusterName 'corridor'
	, case when  l.equipmenttype like '%V%' then 'V' when  l.equipmenttype like 'R' then 'R' else 'other' end 'equipment'
	,COALESCE(Cor.Count_Corridor,0)  'corridor_count' 
	,COALESCE(Ori.Count_Origin,0)  'origin_count' 
	,COALESCE(Dest.Count_Dest,0)  'dest_count' 
	,COALESCE(CC.Count_Cus,0)  'cus_Count'
	,COALESCE(CC.Count_ALL,0)   'cus_All'
	,case when COALESCE(CC.Count_ALL,0)<3000 then 'Small'
	when COALESCE(CC.Count_ALL,0)<10000 then 'Small-Med'
	when COALESCE(CC.Count_ALL,0)< 25000 then   'Med'
	when COALESCE(CC.Count_ALL,0)<50000 then  'Med-Large'
	else 'Large' end 'cus_Size'
	,C.DandBIndustryId  'industryID', 
	D.Code 'industry'
	,	CityO.Latitude 'originLat',CityO.Longitude 'originLon',
	CityD.Latitude 'destinationLat',CityD.Longitude 'destinationLon'
	--,case when CC.Count_ALL>0 then CC.Count_Cus*1.0/CC.Count_ALL  else 0 end 'Cus_Ratio',
	--,L.Miles,
	-- Case
	--when L.Miles <250 then'Short'
	--when L.Miles between 250 and 500 then 'Medium-Short'
	--when L.Miles between 500 and 1000 then 'Medium'
	--when L.Miles between 1000 and 2000 then 'Medium-Long'
	--when L.Miles >2000 then 'Long' end 'Haul-Length'
	from #Service S
	full join #Bounce B on B.LoadID=S.LoadID and B.CarrierID=S.CarrierID
	full join #Offer O on S.LoadID=O.LoadID and S.CarrierID=O.CarrierID
	inner join bazooka.dbo.LoadCustomer LCUS on LCUS.LoadID = COALESCE(B.LoadID,O.LoadID)
	--inner join bazooka.dbo.Customer CUS on CUS.id=LCUS.CustomerID
	inner join bazooka.dbo.load L on L.id=LCUS.LoadID AND LCUS.Main = 1
	inner join bazooka.dbo.loadstop LSP on LSP.id=L.OriginLoadStopID
	inner join bazooka.dbo.loadstop LSD on LSD.id=L.DestinationLoadStopID
	inner join bazooka.dbo.City CityO on CityO.id=LSP.CityID
	inner join bazooka.dbo.City CityD on CityD.id=LSD.CityID
	LEFT JOIN Analytics.CTM.RateClusters RCO ON RCO.Location = L.OriginCityName + ', ' + L.OriginStateCode
	LEFT JOIN Analytics.CTM.RateClusters RCD ON RCD.Location = L.DestinationCityName + ', ' + L.DestinationStateCode
	left join #Carrier_Corridor Cor on Cor.Corridor=RCO.ClusterNAME +'-'+RCD.ClusterName  
	left join #Carrier_Origin Ori on Ori.OriginCluster=RCO.ClusterNAME  
	left join #Carrier_Dest Dest on Dest.DestinationCluster=RCD.ClusterNAME 
	left join #Carrier_Cust CC on CC.CustID = LCUS.CustomerID  
	inner join bazooka.dbo.CustomerRelationshipManagement  C on C.CustomerID=LCUS.CustomerID
	inner join
	bazooka.dbo.DandBIndustry D  on C.DandBIndustryId=D.DandBIndustryId
	where   rnk=1  
)X
	where pu_Gap>=0
	 order by corridor
	         """

    histload=pd.read_sql(query,cn,params= [CarrierID])
    histload['corridor_max']=max(histload.corridor_count)
    histload['origin_max']=max(histload.origin_count)
    histload['dest_max']=max(histload.dest_count)
    return (histload)

#k carriers
def Carrier_Load_loading(k):
    cn = pyodbc.connect('DRIVER={SQL Server};SERVER=ANALYTICSPROD;DATABASE=Bazooka;trusted_connection=true')
    query = """ 
    set nocount on
    declare @Carriertop as int = ?
 
    declare @CarrierDate1 as date = '2017-06-01'
    declare @CarrierDate2 as date = '2018-06-01'

    declare @HistDate1 as date = '2017-06-01'
    declare @HistDate2 as date = '2018-06-01'

    If(OBJECT_ID('tempdb..#Carrier_List') Is Not Null)
      Begin
      Drop Table #Carrier_List
      End
      Create Table #Carrier_List (CarrierID int, TotalVol int)
      Insert into #Carrier_List

    select top (@Carriertop) car.id,COUNT(L.ID) 'TolVol'
    from bazooka.dbo.[load] L
    inner join bazooka.dbo.loadcarrier lcar on lcar.loadid = l.id and lcar.main = 1 and lcar.IsBounced = 0
    inner join Bazooka.dbo.carrier car on car.id = lcar.CarrierID
    INNER JOIN Bazooka.dbo.LoadCustomer LCUS ON LCUS.LoadID = L.ID AND LCUS.Main = 1 
    INNER JOIN Bazooka.dbo.Customer CUS ON LCUS.CustomerID = CUS.ID
    LEFT JOIN Bazooka.dbo.Customer PCUS ON CUS.ParentCustomerID = PCUS.ID
    INNER JOIN bazooka.dbo.LoadRate LR ON LR.LoadID = L.ID AND LR.EntityType = 13 AND LR.EntityID = lcar.ID
    where L.StateType = 1 and L.ProgressType =8 and l.Mode = 1 and L.LoadDate > @CarrierDate1 
    AND LCAR.CarrierID NOT IN (32936 ,244862,244863,244864,244866,244867)  AND L.ShipmentType not in (3,4,6,7)
    and (l.equipmenttype like '%V%' or l.equipmenttype like 'R')
    and car.ContractVersion NOT IN ('TMS FILE', 'UPSDS CTM', 'UPSCD CTM') and car.Name not like 'UPS%'
    AND L.TotalRAte >= 150 AND L.TotalCost >= 150
    AND  L.[OriginStateCode] in (select [Code]  FROM [Bazooka].[dbo].[State] where [ID]<=51) 
    AND  L.[DestinationStateCode] in (select [Code]  FROM [Bazooka].[dbo].[State] where [ID]<=51) 
    AND COALESCE(PCUS.CODE,CUS.CODE) NOT IN ('UPSAMZGA','UPSRAILPEA')
    and  LR.OriginalQuoteRateLineItemID = 0
    group by car.ID
    order by 2 desc 




      If(OBJECT_ID('tempdb..#Bounce_Reason') Is Not Null)
                      Begin
                      Drop Table #Bounce_Reason
                      End
                      Create Table #Bounce_Reason (FaultType int, ReasonType int, Reason varchar(30))
                      Insert into #Bounce_Reason Values(0,0,	'Carrier')
                      Insert into #Bounce_Reason Values(1,1,	'Carrier')
                      Insert into #Bounce_Reason Values(1,2,	'Carrier')
                      Insert into #Bounce_Reason Values(1,3,	'Carrier')
                      Insert into #Bounce_Reason Values(1,4,	'Carrier')
                      Insert into #Bounce_Reason Values(1,6,	'Carrier')
                      Insert into #Bounce_Reason Values(1,12,	'Carrier')
                      Insert into #Bounce_Reason Values(1,13,	'Carrier')
                      Insert into #Bounce_Reason Values(1,7,	'Carrier_Reps')
                      Insert into #Bounce_Reason Values(1,8,	'Cust_Reps')
                      Insert into #Bounce_Reason Values(2,1,	'Carrier')
                      Insert into #Bounce_Reason Values(2,2,	'Carrier')
                      Insert into #Bounce_Reason Values(2,3,	'Carrier')
                      Insert into #Bounce_Reason Values(2,4,	'Carrier')
                      Insert into #Bounce_Reason Values(2,5,	'Customer')
                      Insert into #Bounce_Reason Values(2,7,	'Carrier_Reps')
                      Insert into #Bounce_Reason Values(2,8,	'Cust_Reps')
                      Insert into #Bounce_Reason Values(2,9,	'Carrier')
                      Insert into #Bounce_Reason Values(2,10,	'Facility')
                      Insert into #Bounce_Reason Values(2,13,	'Carrier_Reps')
                      Insert into #Bounce_Reason Values(3,10,	'Facility')
                      Insert into #Bounce_Reason Values(3,11,	'Facility')
                      Insert into #Bounce_Reason Values(3,12,	'Customer')
                      Insert into #Bounce_Reason Values(3,13,	'Customer')

    If(OBJECT_ID('tempdb..#Service') Is Not Null)
    Begin
    Drop Table #Service
    End
    Create Table #Service ( LoadID int, CarrierID int, PUScore int, DelScore int)
    Insert into #Service

    select LoadID,
    Carrierid,
    case when datediff(minute,PU_Appt,PU_Arrive)<=60 then 25
    when datediff(minute,PU_Appt,PU_Arrive)<= 120 then 20
    when datediff(day,PU_Appt,PU_Arrive)=0 then 10
    else 5 end 'PU',
    case when datediff(minute,DO_Appt,DO_Arrive)<=60 then 25
    when datediff(minute,DO_Appt,DO_Arrive)<= 120 then 20
    when datediff(day,DO_Appt,DO_Arrive)=0 then 10
    else 5 end 'Del'
    from (
    select  L.id 'LoadID',
    LCAR.CarrierID,
    (case when LSP.[ScheduleCloseTime] = '1753-01-01' then
    convert(datetime, CONVERT(date, LSP.LoadByDate)) + convert(datetime, CONVERT(time, LSP.CloseTime))
    else LSP.[ScheduleCloseTime] end) 'PU_Appt',
    LSP.[ArriveDateTime] 'PU_Arrive'
    , case when LSD.[ScheduleCloseTime] = '1753-01-01' then
    convert(datetime, CONVERT(date, LSD.DeliverByDate)) + convert(datetime, CONVERT(time, LSD.CloseTime))
    else LSD.[ScheduleCloseTime] end 'DO_Appt',
    LSD.[ArriveDateTime] 'DO_Arrive'
    FROM Bazooka.dbo.[Load] L
    INNER JOIN Bazooka.dbo.LoadCarrier LCAR ON LCAR.LoadID = L.ID and LCAR.Main = 1 and LCAR.IsBounced = 0
    inner join Bazooka.dbo.loadstop LSP on  LSP.ID=L.OriginLoadStopID
    inner join Bazooka.dbo.loadstop LSD on  LSD.ID=L.DestinationLoadStopID
    WHERE L.Mode = 1 AND    L.LoadDate between @CarrierDate1 and @CarrierDate2 and L.Miles>0
    and LCAR.CarrierID in (select carrierid from #Carrier_List)
    --and LCAR.CarrierID= @CarrierID
    ) X

    If(OBJECT_ID('tempdb..#Bounce') Is Not Null)
    Begin
    Drop Table #Bounce
    End
    Create Table #Bounce ( LoadID int, CarrierID int,  Offer int, Accept int, Bounce int, OriginDH decimal(8,2), EmptyTime datetime)
    Insert into #Bounce

    select
    L.ID, LCAR.CarrierID, 1 'Offer',1 'Accepted Offers',
    sum (case when BR.Reason like 'Carrier' then 1 else 0 end) 'Bounce',
    min(case when LCAR.ActualDistance<1 then 1 else LCAR.ActualDistance end)   'OriginDH',
    case when convert (date, max(LCAR.ActualDateTime))='1753-01-01' then getdate() else  max(LCAR.ActualDateTime) end 'EmptyTime'
    FROM Bazooka.dbo.[Load] L
    INNER JOIN Bazooka.dbo.LoadCarrier LCAR ON LCAR.LoadID = L.ID
    left join Bazooka.dbo.LoadChangeLog Log_B on  Log_B.ChangeType=4 and Log_B.EntityID=LCAR.ID and LCAR.IsBounced=1
    left join #Bounce_Reason BR on BR.FaultType=Log_B.FaultType and BR.ReasonType=Log_B.ReasonType
    WHERE L.Mode = 1 AND    L.LoadDate between @CarrierDate1 and @CarrierDate2 and L.Miles>0
    and LCAR.CarrierID in (select carrierid from #Carrier_List)
    --and LCAR.CarrierID=@CarrierID
    and L.ProgressType>=7
    group by L.id, LCAR.CarrierID
    order by Bounce



    If(OBJECT_ID('tempdb..#Offer') Is Not Null)
    Begin
    Drop Table #Offer
    End
    Create Table #Offer( LoadID int, CarrierID int, Offer int, Cost money, Ask money,  BadOffer int,OriginDH int, AvailableTime datetime, Rnk int)
    Insert into #Offer
    select
    O.LoadID, CarrierID, 1 'Offer',LRD.Cost, Ask,
    case when  Ask >lrd.Cost*0.9 then 1 else 0 end  'Badoffer',
    case when O.MilesToOrigin<1 then 1 else O.MilesToOrigin end 'OriginDH',
    convert(datetime, CONVERT(date,O.CanLoadDate))+convert(datetime, CONVERT(time,O.CanLoadTime)) 'AvailableTime',
    RANK() over (partition by O.LoadID, O.CarrierID order by O.CreateDate desc) 'rnk'
    from bazooka.dbo.Offer O
    inner join Bazooka.dbo.[Load] L on O.LoadID = L.ID
    inner join Bazooka.dbo.LoadCustomer LCUS on LCUS.LoadID = L.ID and LCUS.Main = 1
    inner join (select entityid, SUM(amount) 'Cost' from Bazooka.dbo.LoadRateDetail
                            where EntityType = 12 and EDIDataElementCode IN  ('405','FR',  'PM' ,'MN') and CreateDate > '2018-01-01' Group by entityid) LRD on LRD.entityid = lcus.id
    --inner join #Cost C on C.LoadID=O.LoadID
    where --O.Carrierid=@CarrierID   and
    O.LoadDate between @CarrierDate1 and @CarrierDate2
    and O.CarrierID in (select carrierid from #Carrier_List)
    and Ask>0 and LRD.Cost > 150 and  L.Mode = 1  and L.ProgressType>=7


    If(OBJECT_ID('tempdb..#Carrier_CustID') Is Not Null)
                      Begin
                      Drop Table #Carrier_CustID
                      End
                                      Create Table #Carrier_CustID (LoadID int,  CustID int)
                                      Insert into #Carrier_CustID
    select COALESCE(B.LoadID,O.LoadID)   'LoadID',
    CustomerID
    from #Bounce  B
    full join #Offer O on B.LoadID=O.LoadID and B.CarrierID=O.CarrierID
    inner join bazooka.dbo.LoadCustomer LCUS on LCUS.LoadID = COALESCE(B.LoadID,O.LoadID)
    ---End of Load-Carrier KPI Score


    ---Start of Carrier Features

    If(OBJECT_ID('tempdb..#Carrier_HistLoad') Is Not Null)
                      Begin
                      Drop Table #Carrier_HistLoad
                      End
                                      Create Table #Carrier_HistLoad (LoadID int, CarrierID int, CustID int, Origin varchar (50), Destination varchar(50), Equip varchar (20), OriginCluster varchar (50), DestinationCluster varchar (50), Corridor varchar (100))
                                      Insert into #Carrier_HistLoad


    select L.id 'LoadID', LCAR.CarrierID,
     LCUS.CustomerID  'CustID'
    --,Miles
    , L.OriginCityName + ', ' + L.OriginStateCode  'Origin'
    ,L.DestinationCityName + ', ' + L.DestinationStateCode  'Destination'
    --,L.TotalValue
    ,case when  l.equipmenttype like '%V%' then 'V' when  l.equipmenttype like 'R' then 'R' else 'other' end Equipment
    ,RCO.ClusterNAME 'OriginCluster'
    ,RCD.ClusterName 'DestinationCluster'
    ,RCO.ClusterNAME+'-'+RCD.ClusterName  'Corridor'
    FROM Bazooka.dbo.[Load] L
    INNER JOIN Bazooka.dbo.LoadCarrier LCAR ON LCAR.LoadID = L.ID and LCAR.Main = 1 and LCAR.IsBounced = 0
    --INNER JOIN Bazooka.dbo.Carrier CAR ON CAR.ID = LCAR.CarrierID
    INNER JOIN Bazooka.dbo.LoadCustomer LCUS ON LCUS.LoadID = L.ID AND LCUS.Main = 1
    --INNER JOIN Bazooka.dbo.Customer CUS ON LCUS.CustomerID = CUS.ID
    --LEFT JOIN Bazooka.dbo.Customer PCUS ON CUS.ParentCustomerID = PCUS.ID
    INNER JOIN bazooka.dbo.LoadRate LR ON LR.LoadID = L.ID AND LR.EntityType = 13 AND LR.EntityID = LCAR.ID and LR.OriginalQuoteRateLineItemID=0
    --inner join bazooka.dbo.loadstop LS on LS.id=L.OriginLoadStopID
    LEFT JOIN Analytics.CTM.RateClusters RCO ON RCO.Location = L.OriginCityName + ', ' + L.OriginStateCode
    LEFT JOIN Analytics.CTM.RateClusters RCD ON RCD.Location = L.DestinationCityName + ', ' + L.DestinationStateCode
    WHERE L.StateType = 1
    and  L.LoadDate between @HistDate1 and @HistDate2  and L.Miles>0
    AND L.Mode = 1 and LCAR.CarrierID in (select carrierid from #Carrier_List)
    --AND LCAR.CarrierID=@CarrierID
    AND L.ShipmentType not in (3,4,6,7)
    AND (CASE WHEN L.EquipmentType LIKE '%V%' THEN 'V' ELSE L.EquipmentType END) IN ('V', 'R')
    --AND CAR.ContractVersion NOT IN ('TMS FILE', 'UPSDS CTM', 'UPSCD CTM') --Exclude Managed Loads
    --AND COALESCE(PCUS.CODE,CUS.CODE) NOT IN ('UPSAMZGA','UPSRAILPEA')
    AND L.TotalRAte >= 150 AND L.TotalCost >= 150
    AND  L.[OriginStateCode] in (select [Code]  FROM [Bazooka].[dbo].[State] where [ID]<=51)
    AND  L.[DestinationStateCode] in (select [Code]  FROM [Bazooka].[dbo].[State] where [ID]<=51)
    --and car.Name not like 'UPS%'
    order by Origin,Destination



     If(OBJECT_ID('tempdb..#CustSize') Is Not Null)
                      Begin
                      Drop Table #CustSize
                      End
                                      Create Table #CustSize (CustID int, Count_ALL int)
                                      Insert into #CustSize

    select customerID,
    count(loadid)
    from bazooka.dbo.LoadCustomer LCU
    inner join bazooka.dbo.load L on L.id=LCU.LoadID
    where L.StateType = 1 and L.ProgressType >=7 and l.Mode = 1 and L.LoadDate between @HistDate1 and @HistDate2
    and (l.equipmenttype like '%V%' or l.equipmenttype like 'R')
    AND L.TotalRAte >= 150 AND L.TotalCost >= 150
    AND  L.[OriginStateCode] in (select [Code]  FROM [Bazooka].[dbo].[State] where [ID]<=51)
    AND  L.[DestinationStateCode] in (select [Code]  FROM [Bazooka].[dbo].[State] where [ID]<=51)
    and customerID in(select distinct CustID  from #Carrier_CustID)
    group by CustomerID

     If(OBJECT_ID('tempdb..#Carrier_Cust') Is Not Null)
            Begin
            Drop Table #Carrier_Cust
            End
            Create Table #Carrier_Cust (CustID int, CarrierID int, Count_Cus int, Count_ALL int)
            Insert into #Carrier_Cust
     select distinct #Carrier_HistLoad.CustID, CarrierID,
     count(loadid) 'Count_Cus'
     ,#CustSize.Count_ALL 'Count_ALL'
     from #Carrier_HistLoad
     inner join #CustSize on #CustSize.CustID= #Carrier_HistLoad.CustID
     group by #Carrier_HistLoad.CustID, #CustSize.Count_ALL,CarrierID
     order by 2 desc

	 --select distinct #Carrier_HistLoad.CustID, 
  --   count(loadid) 'Count_Cus'
  --   ,#CustSize.Count_ALL 'Count_ALL'
  --   from #Carrier_HistLoad
  --   inner join #CustSize on #CustSize.CustID= #Carrier_HistLoad.CustID
  --   group by #Carrier_HistLoad.CustID, #CustSize.Count_ALL 
  --   order by 2 desc
 
    If(OBJECT_ID('tempdb..#Carrier_Corridor') Is Not Null)
            Begin
            Drop Table #Carrier_Corridor
            End
            Create Table #Carrier_Corridor (Corridor varchar (50),carrierID int, Count_Corridor int)
            Insert into #Carrier_Corridor
    select distinct corridor,carrierID,
    count(loadid) 'Count_Corridor'
    from #Carrier_HistLoad
    group by Corridor,carrierID
    order by 3 desc

	If(OBJECT_ID('tempdb..#Carrier_Cor_max') Is Not Null)
            Begin
            Drop Table #Carrier_Cor_max
            End
            Create Table #Carrier_Cor_max (carrierID int, max_Corridor int)
            Insert into #Carrier_Cor_max
    select carrierID,
    max(Count_Corridor)  
    from #Carrier_Corridor
    group by carrierID
    order by 2 desc

    If(OBJECT_ID('tempdb..#Carrier_Origin') Is Not Null)
            Begin
            Drop Table #Carrier_Origin
            End
            Create Table #Carrier_Origin (OriginCluster varchar (50), carrierID int, Count_Origin int)
            Insert into #Carrier_Origin
     select distinct OriginCluster,carrierID,
     count(loadid) 'Count_Origin'
     from #Carrier_HistLoad
     group by OriginCluster,carrierID
     order by 3 desc

    If(OBJECT_ID('tempdb..#Carrier_Origin_max') Is Not Null)
            Begin
            Drop Table #Carrier_Origin_max
            End
            Create Table #Carrier_Origin_max ( carrierID int, max_Origin int)
            Insert into #Carrier_Origin_max
     select carrierID,max(Count_Origin)
     from #Carrier_Origin
     group by  carrierID
     order by 2 desc

     If(OBJECT_ID('tempdb..#Carrier_Dest') Is Not Null)
            Begin
            Drop Table #Carrier_Dest
            End
            Create Table #Carrier_Dest (DestinationCluster varchar (50),carrierID int, Count_Dest int)
            Insert into #Carrier_Dest
     select distinct DestinationCluster,carrierID,
    count(loadid) 'Count_Dest'
     from #Carrier_HistLoad
     group by DestinationCluster,carrierID 
     order by 3 desc

	 If(OBJECT_ID('tempdb..#Carrier_Dest_max') Is Not Null)
            Begin
            Drop Table #Carrier_Dest_max
            End
            Create Table #Carrier_Dest_max ( carrierID int, max_Dest int)
            Insert into #Carrier_Dest_max
     select  carrierID,
     max(Count_Dest)
     from #Carrier_Dest
     group by  carrierID 
     order by 2 desc
    ---End of Carrier Features

    select  COALESCE(B.LoadID,O.LoadID)   'loadID',
    COALESCE(B.CarrierID,O.CarrierID)    'carrierID', L.hot 'hot',
    O.cost,
    --COALESCE(S.PUScore,0)          'puScore',
    --COALESCE(S.DelScore,0)            'delScore',
    --Coalesce(O.Offer, B.Offer)*30    'offer',
    --COALESCE(B.Accept,0)*10    'offerAccept' ,
    --COALESCE(B.Bounce,0)*(-10)     'bounce'  ,
    --COALESCE(O.BadOffer,0)*-20     'badOffer',
    COALESCE(S.PUScore,0) +       COALESCE(S.DelScore,0)  +
    Coalesce(O.Offer, B.Offer)*40  +
    COALESCE(B.Accept,0)*10   +
    COALESCE(B.Bounce,0)*(-20)     +
    COALESCE(O.BadOffer,0)*-20     'kpiScore',
    COALESCE(O.OriginDH,B.OriginDH )   'originDH',
    --case when COALESCE(O.OriginDH,B.OriginDH )<=10 then 10
    --when COALESCE(O.OriginDH,B.OriginDH )<=50 then 50
    --when COALESCE(O.OriginDH,B.OriginDH )<=100 then 100
    --else 200 end 'originDH-levels',
     --COALESCE(O.AvailableTime,B.EmptyTime) 'Available',
     --case when LSP.[ScheduleCloseTime] = '1753-01-01' then
     --convert(datetime, CONVERT(date, LSP.LoadByDate)) + convert(datetime, CONVERT(time, LSP.CloseTime))
     --else LSP.[ScheduleCloseTime] end  'PU_Appt',
    datediff(hour,COALESCE(O.AvailableTime,B.EmptyTime),case when LSP.[ScheduleCloseTime] = '1753-01-01' then
    convert(datetime, CONVERT(date, LSP.LoadByDate)) + convert(datetime, CONVERT(time, LSP.CloseTime))
    else LSP.[ScheduleCloseTime] end) 'pu_GAP',
    --datediff(minute,COALESCE(O.AvailableTime,S.EmptyTime),S.PU_Appt) 'PU_GAP',
    --CUS.name 'CustomerName'
    CityO.Latitude 'originLat',CityO.Longitude 'originLon',
	CityD.Latitude 'destinationLat',CityD.Longitude 'destinationLon',
	 RCO.ClusterNAME 'originCluster'
	,RCD.ClusterName 'destinationCluster'
	,RCO.ClusterNAME+'-'+RCD.ClusterName 'corridor'
	, case when  l.equipmenttype like '%V%' then 'V' when  l.equipmenttype like 'R' then 'R' else 'other' end 'equipment'
	,COALESCE(Cor.Count_Corridor,0)  'corridor_count',COALESCE(Cormax.max_corridor,0)  'corridor_max'
	,COALESCE(Ori.Count_Origin,0)  'origin_count',COALESCE(ormax.max_Origin,0)  'origin_max'
	,COALESCE(Dest.Count_Dest,0)  'dest_count',COALESCE(demax.max_Dest,0)  'dest_max'
	,COALESCE(CC.Count_Cus,0)  'cus_Count'
	,COALESCE(CC.Count_ALL,0)   'cus_All'
	,case when COALESCE(CC.Count_ALL,0)<3000 then 'Small'
	when COALESCE(CC.Count_ALL,0)<10000 then 'Small-Med'
	when COALESCE(CC.Count_ALL,0)< 25000 then   'Med'
	when COALESCE(CC.Count_ALL,0)<50000 then  'Med-Large'
	else 'Large' end 'cus_Size'
	,C.DandBIndustryId  'industryID', 
	D.Code 'industry'
    --,case when CC.Count_ALL>0 then CC.Count_Cus*1.0/CC.Count_ALL  else 0 end 'Cus_Ratio',
    --,L.Miles,
    -- Case
    --when L.Miles <250 then'Short'
    --when L.Miles between 250 and 500 then 'Medium-Short'
    --when L.Miles between 500 and 1000 then 'Medium'
    --when L.Miles between 1000 and 2000 then 'Medium-Long'
    --when L.Miles >2000 then 'Long' end 'Haul-Length'
    from #Service S
    full join #Bounce B on B.LoadID=S.LoadID and B.CarrierID=S.CarrierID
    full join #Offer O on S.LoadID=O.LoadID and S.CarrierID=O.CarrierID
    inner join bazooka.dbo.LoadCustomer LCUS on LCUS.LoadID = COALESCE(B.LoadID,O.LoadID)
    --inner join bazooka.dbo.Customer CUS on CUS.id=LCUS.CustomerID
    inner join bazooka.dbo.load L on L.id=LCUS.LoadID AND LCUS.Main = 1
    inner join bazooka.dbo.loadstop LSP on LSP.id=L.OriginLoadStopID
    inner join bazooka.dbo.loadstop LSD on LSD.id=L.DestinationLoadStopID
    inner join bazooka.dbo.City CityO on CityO.id=LSP.CityID
    inner join bazooka.dbo.City CityD on CityD.id=LSD.CityID
    LEFT JOIN Analytics.CTM.RateClusters RCO ON RCO.Location = L.OriginCityName + ', ' + L.OriginStateCode
    LEFT JOIN Analytics.CTM.RateClusters RCD ON RCD.Location = L.DestinationCityName + ', ' + L.DestinationStateCode
    left join #Carrier_Corridor Cor on Cor.Corridor=RCO.ClusterNAME +'-'+RCD.ClusterName and Cor.carrierID=COALESCE(B.CarrierID,O.CarrierID)
    left join #Carrier_Origin Ori on Ori.OriginCluster=RCO.ClusterNAME and Ori.carrierID=COALESCE(B.CarrierID,O.CarrierID)
    left join #Carrier_Dest Dest on Dest.DestinationCluster=RCD.ClusterNAME and Dest.carrierID=COALESCE(B.CarrierID,O.CarrierID)
    left join #Carrier_Cust CC on CC.CustID = LCUS.CustomerID and CC.carrierID=COALESCE(B.CarrierID,O.CarrierID)
	left join #Carrier_Cor_max Cormax on Cormax.carrierID=COALESCE(B.CarrierID,O.CarrierID) 
	left join #Carrier_Dest_max demax on demax.carrierID=COALESCE(B.CarrierID,O.CarrierID) 
	left join #Carrier_Origin_max ormax on ormax.carrierID=COALESCE(B.CarrierID,O.CarrierID) 
 
	inner join bazooka.dbo.CustomerRelationshipManagement  C on C.CustomerID=LCUS.CustomerID
	inner join
	bazooka.dbo.DandBIndustry D  on C.DandBIndustryId=D.DandBIndustryId
    where   rnk=1
    order by carrierID
    """

    histload = pd.read_sql(query, cn,params=[k])
    return (histload)

def Get_newload():
    cn = pyodbc.connect('DRIVER={SQL Server};SERVER=reportingdatabases;DATABASE=Bazooka;trusted_connection=true')
    query="""
    declare @date1 as date = getdate()
    declare @date2 as date = dateadd (day,1,getdate())

    select L.Id  'loadID', convert (date,L.loaddate) 'loaddate',
    LRD.Cost 'customer_rate', L.miles,
	(case when LSP.[ScheduleCloseTime] = '1753-01-01' then 
	convert(datetime, CONVERT(date, LSP.LoadByDate)) + convert(datetime, CONVERT(time, LSP.CloseTime)) 
	else LSP.[ScheduleCloseTime] end) 'pu_appt',
    --LCUS.customerID 'customerID',
    --COALESCE(O.OriginDH,B.OriginDH )   'originDH',
	--datediff(hour,COALESCE(O.AvailableTime,B.EmptyTime),case when LSP.[ScheduleCloseTime] = '1753-01-01' then
	--convert(datetime, CONVERT(date, LSP.LoadByDate)) + convert(datetime, CONVERT(time, LSP.CloseTime))
	--else LSP.[ScheduleCloseTime] end) 'pu_GAP',
    --CUS.name 'CustomerName'
    CityO.Longitude 'originLon',CityO.Latitude 'originLat',
	CityD.Longitude 'destinationLon',CityD.Latitude 'destinationLat',
	 RCO.ClusterNAME 'originCluster'
	,RCD.ClusterName 'destinationCluster'
    ,RCO.ClusterNAME+'-'+RCD.ClusterName 'corridor'
	, case when  l.equipmenttype like '%V%' then 'V' when  l.equipmenttype like 'R' then 'R' else 'other' end 'equipment'
	,C.DandBIndustryId  'industryID', 
	D.Code 'industry'
    from bazooka.dbo.load L 
    inner join bazooka.dbo.LoadCustomer LCUS on L.id=LCUS.LoadID AND LCUS.Main = 1
    inner join bazooka.dbo.loadstop LSP on LSP.id=L.OriginLoadStopID
    inner join bazooka.dbo.loadstop LSD on LSD.id=L.DestinationLoadStopID
    inner join bazooka.dbo.City CityO on CityO.id=LSP.CityID
    inner join bazooka.dbo.City CityD on CityD.id=LSD.CityID
    LEFT JOIN Analytics.CTM.RateClusters RCO ON RCO.Location = L.OriginCityName + ', ' + L.OriginStateCode
    LEFT JOIN Analytics.CTM.RateClusters RCD ON RCD.Location = L.DestinationCityName + ', ' + L.DestinationStateCode
	inner join bazooka.dbo.CustomerRelationshipManagement  C on C.CustomerID=LCUS.CustomerID
	inner join
	Analytics.bazooka.dbo.DandBIndustry D  on C.DandBIndustryId=D.DandBIndustryId
	inner join (select entityid, SUM(amount) 'Cost' from Bazooka.dbo.LoadRateDetail
                            where EntityType = 12 and EDIDataElementCode IN  ('405','FR',  'PM' ,'MN','SCL' ) and CreateDate > '2018-01-01' Group by entityid) LRD on LRD.entityid = lcus.id
   where 
   L.StateType = 1 and L.progresstype=1 and LRD.Cost>100
    and  L.LoadDate between @Date1 and @Date2  and L.Miles>0
    AND L.Mode = 1  
    --AND LCAR.CarrierID=@CarrierID
    AND L.ShipmentType not in (3,4,6,7)
    AND (CASE WHEN L.EquipmentType LIKE '%V%' THEN 'V' ELSE L.EquipmentType END) IN ('V', 'R')
    --AND CAR.ContractVersion NOT IN ('TMS FILE', 'UPSDS CTM', 'UPSCD CTM') --Exclude Managed Loads
    --AND COALESCE(PCUS.CODE,CUS.CODE) NOT IN ('UPSAMZGA','UPSRAILPEA')
    --AND L.TotalRAte >= 150 AND L.TotalCost >= 150
    AND  L.[OriginStateCode] in (select [Code]  FROM [Bazooka].[dbo].[State] where [ID]<=51)
    AND  L.[DestinationStateCode] in (select [Code]  FROM [Bazooka].[dbo].[State] where [ID]<=51)
    """
    newload=pd.read_sql(query,cn )
    return (newload)


def Get_testload(CarrierID):
    cn = pyodbc.connect('DRIVER={SQL Server};SERVER=ANALYTICSPROD;DATABASE=Bazooka;trusted_connection=true')

    query = """
        set nocount on
        declare @CarrierID as int =?

	declare @CarrierDate1 as date = '2018-06-02'
	declare @CarrierDate2 as date = getdate() 

    declare @HistDate1 as date = '2016-01-01'
	declare @HistDate2 as date = '2018-06-01'


	If(OBJECT_ID('tempdb..#Bounce_Reason') Is Not Null)
	Begin
	Drop Table #Bounce_Reason
	End
	Create Table #Bounce_Reason (FaultType int, ReasonType int, Reason varchar(30))
	Insert into #Bounce_Reason Values(0,0,	'Carrier')
	Insert into #Bounce_Reason Values(1,1,	'Carrier')
	Insert into #Bounce_Reason Values(1,2,	'Carrier')
	Insert into #Bounce_Reason Values(1,3,	'Carrier')
	Insert into #Bounce_Reason Values(1,4,	'Carrier')
	Insert into #Bounce_Reason Values(1,6,	'Carrier')
	Insert into #Bounce_Reason Values(1,12,	'Carrier') 
	Insert into #Bounce_Reason Values(1,13,	'Carrier')
	Insert into #Bounce_Reason Values(1,7,	'Carrier_Reps')
	Insert into #Bounce_Reason Values(1,8,	'Cust_Reps')
	Insert into #Bounce_Reason Values(2,1,	'Carrier')
	Insert into #Bounce_Reason Values(2,2,	'Carrier')
	Insert into #Bounce_Reason Values(2,3,	'Carrier')
	Insert into #Bounce_Reason Values(2,4,	'Carrier')
	Insert into #Bounce_Reason Values(2,5,	'Customer')
	Insert into #Bounce_Reason Values(2,7,	'Carrier_Reps')
	Insert into #Bounce_Reason Values(2,8,	'Cust_Reps')
	Insert into #Bounce_Reason Values(2,9,	'Carrier')
	Insert into #Bounce_Reason Values(2,10,	'Facility')
	Insert into #Bounce_Reason Values(2,13,	'Carrier_Reps')
	Insert into #Bounce_Reason Values(3,10,	'Facility')
	Insert into #Bounce_Reason Values(3,11,	'Facility')
	Insert into #Bounce_Reason Values(3,12,	'Customer')
	Insert into #Bounce_Reason Values(3,13,	'Customer')

	If(OBJECT_ID('tempdb..#Service') Is Not Null)
	Begin
	Drop Table #Service
	End
	Create Table #Service ( LoadID int, CarrierID int, PUScore int, DelScore int)
	Insert into #Service

	select LoadID,
	Carrierid, 
	case when datediff(minute,PU_Appt,PU_Arrive)<=60 then 25
	when datediff(minute,PU_Appt,PU_Arrive)<= 120 then 20
	when datediff(day,PU_Appt,PU_Arrive)=0 then 10
	else 5 end 'PU',
	case when datediff(minute,DO_Appt,DO_Arrive)<=60 then 25
	when datediff(minute,DO_Appt,DO_Arrive)<= 120 then 20
	when datediff(day,DO_Appt,DO_Arrive)=0 then 10
	else 5 end 'Del'
	from (
	select  L.id 'LoadID', 
	LCAR.CarrierID, 
	(case when LSP.[ScheduleCloseTime] = '1753-01-01' then 
	convert(datetime, CONVERT(date, LSP.LoadByDate)) + convert(datetime, CONVERT(time, LSP.CloseTime)) 
	else LSP.[ScheduleCloseTime] end) 'PU_Appt',
	LSP.[ArriveDateTime] 'PU_Arrive'
	, case when LSD.[ScheduleCloseTime] = '1753-01-01' then 
	convert(datetime, CONVERT(date, LSD.DeliverByDate)) + convert(datetime, CONVERT(time, LSD.CloseTime)) 
	else LSD.[ScheduleCloseTime] end 'DO_Appt',
	LSD.[ArriveDateTime] 'DO_Arrive' 
	FROM Bazooka.dbo.[Load] L
	INNER JOIN Bazooka.dbo.LoadCarrier LCAR ON LCAR.LoadID = L.ID and LCAR.Main = 1 and LCAR.IsBounced = 0
	inner join Bazooka.dbo.loadstop LSP on  LSP.ID=L.OriginLoadStopID
	inner join Bazooka.dbo.loadstop LSD on  LSD.ID=L.DestinationLoadStopID
	WHERE L.Mode = 1 AND    L.LoadDate between @CarrierDate1 and @CarrierDate2 and L.Miles>0 and LCAR.CarrierID= @CarrierID  
	) X

	If(OBJECT_ID('tempdb..#Bounce') Is Not Null)
	Begin
	Drop Table #Bounce
	End
	Create Table #Bounce ( LoadID int, CarrierID int,  Offer int, Accept int, Bounce int, OriginDH decimal(8,2), EmptyTime datetime)
	Insert into #Bounce

	select
	L.ID, LCAR.CarrierID, 1 'Offer',1 'Accepted Offers', 
	sum (case when BR.Reason like 'Carrier' then 1 else 0 end) 'Bounce',
	min(case when LCAR.ActualDistance<1 then 1 else LCAR.ActualDistance end)   'OriginDH',
	case when convert (date, max(LCAR.ActualDateTime))='1753-01-01' then getdate() else  max(LCAR.ActualDateTime) end 'EmptyTime'
	FROM Bazooka.dbo.[Load] L
	INNER JOIN Bazooka.dbo.LoadCarrier LCAR ON LCAR.LoadID = L.ID  
	left join Bazooka.dbo.LoadChangeLog Log_B on  Log_B.ChangeType=4 and Log_B.EntityID=LCAR.ID and LCAR.IsBounced=1
	left join #Bounce_Reason BR on BR.FaultType=Log_B.FaultType and BR.ReasonType=Log_B.ReasonType 
	WHERE L.Mode = 1 AND    L.LoadDate between @CarrierDate1 and @CarrierDate2 and L.Miles>0 and LCAR.CarrierID=@CarrierID   and L.ProgressType>=7    
	group by L.id, LCAR.CarrierID
	order by Bounce



	If(OBJECT_ID('tempdb..#Offer') Is Not Null)
	Begin
	Drop Table #Offer
	End
	Create Table #Offer( LoadID int, CarrierID int, Offer int, Cost money, Ask money,  BadOffer int,OriginDH int, AvailableTime datetime, Rnk int)
	Insert into #Offer
	select 
	O.LoadID, CarrierID, 1 'Offer',LRD.Cost, Ask, 
	case when  Ask >lrd.Cost*0.9 then 1 else 0 end  'Badoffer',
	case when O.MilesToOrigin<1 then 1 else O.MilesToOrigin end 'OriginDH',
	convert(datetime, CONVERT(date,O.CanLoadDate))+convert(datetime, CONVERT(time,O.CanLoadTime)) 'AvailableTime',
	RANK() over (partition by O.LoadID, O.CarrierID order by O.CreateDate desc) 'rnk'
	from bazooka.dbo.Offer O
	inner join Bazooka.dbo.[Load] L on O.LoadID = L.ID
	inner join Bazooka.dbo.LoadCustomer LCUS on LCUS.LoadID = L.ID and LCUS.Main = 1
	inner join (select entityid, SUM(amount) 'Cost' from Bazooka.dbo.LoadRateDetail 
	where EntityType = 12 and EDIDataElementCode IN  ('405','FR',  'PM' ,'MN') and CreateDate > '2018-01-01' Group by entityid) LRD on LRD.entityid = lcus.id
	--inner join #Cost C on C.LoadID=O.LoadID
	where O.Carrierid=@CarrierID   and O.LoadDate between @CarrierDate1 and @CarrierDate2 and  
	Ask>0 and LRD.Cost > 150 and  L.Mode = 1  and L.ProgressType>=7  


	If(OBJECT_ID('tempdb..#Carrier_CustID') Is Not Null)
	Begin
	Drop Table #Carrier_CustID
	End
	Create Table #Carrier_CustID (LoadID int,  CustID int)
	Insert into #Carrier_CustID
	select COALESCE(B.LoadID,O.LoadID)   'LoadID',
	CustomerID
	from #Bounce  B
	full join #Offer O on B.LoadID=O.LoadID and B.CarrierID=O.CarrierID
	inner join bazooka.dbo.LoadCustomer LCUS on LCUS.LoadID = COALESCE(B.LoadID,O.LoadID) 
	---End of Load-Carrier KPI Score


	---Start of Carrier Features

	If(OBJECT_ID('tempdb..#Carrier_HistLoad') Is Not Null)
	Begin
	Drop Table #Carrier_HistLoad
	End
	Create Table #Carrier_HistLoad (LoadID int,  CustID int, Origin varchar (50), Destination varchar(50), Equip varchar (20), OriginCluster varchar (50), DestinationCluster varchar (50), Corridor varchar (100))
	Insert into #Carrier_HistLoad


	select L.id 'LoadID',  
	LCUS.CustomerID  'CustID'
	--,Miles
	, L.OriginCityName + ', ' + L.OriginStateCode  'Origin'
	,L.DestinationCityName + ', ' + L.DestinationStateCode  'Destination'
	--,L.TotalValue
	,case when  l.equipmenttype like '%V%' then 'V' when  l.equipmenttype like 'R' then 'R' else 'other' end Equipment
	,RCO.ClusterNAME 'OriginCluster'
	,RCD.ClusterName 'DestinationCluster'
	,RCO.ClusterNAME+'-'+RCD.ClusterName  'Corridor'
	FROM Bazooka.dbo.[Load] L
	INNER JOIN Bazooka.dbo.LoadCarrier LCAR ON LCAR.LoadID = L.ID and LCAR.Main = 1 and LCAR.IsBounced = 0
	--INNER JOIN Bazooka.dbo.Carrier CAR ON CAR.ID = LCAR.CarrierID
	INNER JOIN Bazooka.dbo.LoadCustomer LCUS ON LCUS.LoadID = L.ID AND LCUS.Main = 1 
	--INNER JOIN Bazooka.dbo.Customer CUS ON LCUS.CustomerID = CUS.ID
	--LEFT JOIN Bazooka.dbo.Customer PCUS ON CUS.ParentCustomerID = PCUS.ID
	INNER JOIN bazooka.dbo.LoadRate LR ON LR.LoadID = L.ID AND LR.EntityType = 13 AND LR.EntityID = LCAR.ID and LR.OriginalQuoteRateLineItemID=0
	--inner join bazooka.dbo.loadstop LS on LS.id=L.OriginLoadStopID
	LEFT JOIN Analytics.CTM.RateClusters RCO ON RCO.Location = L.OriginCityName + ', ' + L.OriginStateCode  
	LEFT JOIN Analytics.CTM.RateClusters RCD ON RCD.Location = L.DestinationCityName + ', ' + L.DestinationStateCode  
	WHERE L.StateType = 1
	and  L.LoadDate between @HistDate1 and @HistDate2  and L.Miles>0 
	AND L.Mode = 1 AND LCAR.CarrierID=@CarrierID
	AND L.ShipmentType not in (3,4,6,7)
	AND (CASE WHEN L.EquipmentType LIKE '%V%' THEN 'V' ELSE L.EquipmentType END) IN ('V', 'R')
	--AND CAR.ContractVersion NOT IN ('TMS FILE', 'UPSDS CTM', 'UPSCD CTM') --Exclude Managed Loads
	--AND COALESCE(PCUS.CODE,CUS.CODE) NOT IN ('UPSAMZGA','UPSRAILPEA')
	AND L.TotalRAte >= 150 AND L.TotalCost >= 150
	AND  L.[OriginStateCode] in (select [Code]  FROM [Bazooka].[dbo].[State] where [ID]<=51) 
	AND  L.[DestinationStateCode] in (select [Code]  FROM [Bazooka].[dbo].[State] where [ID]<=51) 
	--and car.Name not like 'UPS%'
	order by Origin,Destination



	If(OBJECT_ID('tempdb..#CustSize') Is Not Null)
	Begin
	Drop Table #CustSize
	End
	Create Table #CustSize (CustID int, Count_ALL int)
	Insert into #CustSize

	select customerID,
	count(loadid)
	from bazooka.dbo.LoadCustomer LCU
	inner join bazooka.dbo.load L on L.id=LCU.LoadID
	where L.StateType = 1 and L.ProgressType >=7 and l.Mode = 1 and L.LoadDate between @HistDate1 and @HistDate2 
	and (l.equipmenttype like '%V%' or l.equipmenttype like 'R')
	AND L.TotalRAte >= 150 AND L.TotalCost >= 150
	AND  L.[OriginStateCode] in (select [Code]  FROM [Bazooka].[dbo].[State] where [ID]<=51) 
	AND  L.[DestinationStateCode] in (select [Code]  FROM [Bazooka].[dbo].[State] where [ID]<=51) 
	and customerID in(select distinct CustID  from #Carrier_CustID)
	group by CustomerID

	If(OBJECT_ID('tempdb..#Carrier_Cust') Is Not Null)
	Begin
	Drop Table #Carrier_Cust
	End
	Create Table #Carrier_Cust (CustID int, Count_Cus int, Count_ALL int)
	Insert into #Carrier_Cust
	select distinct #Carrier_HistLoad.CustID,
	count(loadid) 'Count_Cus'
	,#CustSize.Count_ALL 'Count_ALL'
	from #Carrier_HistLoad
	inner join #CustSize on #CustSize.CustID= #Carrier_HistLoad.CustID
	group by #Carrier_HistLoad.CustID, #CustSize.Count_ALL
	order by 2 desc

	If(OBJECT_ID('tempdb..#Carrier_Corridor') Is Not Null)
	Begin
	Drop Table #Carrier_Corridor
	End
	Create Table #Carrier_Corridor (Corridor varchar (50), Count_Corridor int)
	Insert into #Carrier_Corridor
	select distinct corridor,
	count(loadid) 'Count_Corridor'
	from #Carrier_HistLoad
	group by Corridor
	order by 2 desc

	If(OBJECT_ID('tempdb..#Carrier_Origin') Is Not Null)
	Begin
	Drop Table #Carrier_Origin
	End
	Create Table #Carrier_Origin (OriginCluster varchar (50), Count_Origin int)
	Insert into #Carrier_Origin
	select distinct OriginCluster,
	count(loadid) 'Count_Origin'
	from #Carrier_HistLoad
	group by OriginCluster
	order by 2 desc


	If(OBJECT_ID('tempdb..#Carrier_Dest') Is Not Null)
	Begin
	Drop Table #Carrier_Dest
	End
	Create Table #Carrier_Dest (DestinationCluster varchar (50), Count_Dest int)
	Insert into #Carrier_Dest
	select distinct DestinationCluster,
	count(loadid) 'Count_Dest'
	from #Carrier_HistLoad
	group by DestinationCluster
	order by 2 desc
	---End of Carrier Features



	select * from (
	select  COALESCE(B.LoadID,O.LoadID)   'loadID',  L.loaddate,
	COALESCE(B.CarrierID,O.CarrierID)    'carrierID', L.hot 'hot',
	O.cost 'customer_rate',
	case when  B.Accept=1 then l.totalcost else o.Ask  end 'carrier_cost',
	L.miles, (case when  B.Accept=1 then l.totalcost else o.Ask end)/(L.miles+COALESCE(O.OriginDH,B.OriginDH) )  'rpm',
	--COALESCE(S.PUScore,0)          'puScore',
	--COALESCE(S.DelScore,0)            'delScore',
	--Coalesce(O.Offer, B.Offer)*40    'offer',
	--COALESCE(B.Accept,0)*10    'offerAccept' ,
	--COALESCE(B.Bounce,0)*(-20)     'bounce'  ,
	--COALESCE(O.BadOffer,0)*-20     'badOffer',
	COALESCE(S.PUScore,0) +       COALESCE(S.DelScore,0)  +
	Coalesce(O.Offer, B.Offer)*40  +
	COALESCE(B.Accept,0)*10   +
	COALESCE(B.Bounce,0)*(-20)     +
	COALESCE(O.BadOffer,0)*-20     'kpiScore',
	COALESCE(O.OriginDH,B.OriginDH )   'originDH',
	--case when COALESCE(O.OriginDH,B.OriginDH )<=10 then 10
	--when COALESCE(O.OriginDH,B.OriginDH )<=50 then 50
	--when COALESCE(O.OriginDH,B.OriginDH )<=100 then 100
	--else 200 end 'originDH-levels',
	--COALESCE(O.AvailableTime,B.EmptyTime) 'Available',
	--case when LSP.[ScheduleCloseTime] = '1753-01-01' then
	--convert(datetime, CONVERT(date, LSP.LoadByDate)) + convert(datetime, CONVERT(time, LSP.CloseTime))
	--else LSP.[ScheduleCloseTime] end  'PU_Appt',
	datediff(hour,COALESCE(O.AvailableTime,B.EmptyTime),case when LSP.[ScheduleCloseTime] = '1753-01-01' then
	convert(datetime, CONVERT(date, LSP.LoadByDate)) + convert(datetime, CONVERT(time, LSP.CloseTime))
	else LSP.[ScheduleCloseTime] end) 'pu_GAP',
	--datediff(minute,COALESCE(O.AvailableTime,S.EmptyTime),S.PU_Appt) 'PU_GAP',
	--CUS.name 'CustomerName'
	RCO.ClusterNAME 'originCluster'
	,RCD.ClusterName 'destinationCluster'
	,RCO.ClusterNAME+'-'+RCD.ClusterName 'corridor'
	, case when  l.equipmenttype like '%V%' then 'V' when  l.equipmenttype like 'R' then 'R' else 'other' end 'equipment'
	,COALESCE(Cor.Count_Corridor,0)  'corridor_count' 
	,COALESCE(Ori.Count_Origin,0)  'origin_count' 
	,COALESCE(Dest.Count_Dest,0)  'dest_count' 
	,COALESCE(CC.Count_Cus,0)  'cus_Count'
	,COALESCE(CC.Count_ALL,0)   'cus_All'
	,case when COALESCE(CC.Count_ALL,0)<3000 then 'Small'
	when COALESCE(CC.Count_ALL,0)<10000 then 'Small-Med'
	when COALESCE(CC.Count_ALL,0)< 25000 then   'Med'
	when COALESCE(CC.Count_ALL,0)<50000 then  'Med-Large'
	else 'Large' end 'cus_Size'
	,C.DandBIndustryId  'industryID', 
	D.Code 'industry'
	,	CityO.Latitude 'originLat',CityO.Longitude 'originLon',
	CityD.Latitude 'destinationLat',CityD.Longitude 'destinationLon'
	--,case when CC.Count_ALL>0 then CC.Count_Cus*1.0/CC.Count_ALL  else 0 end 'Cus_Ratio',
	--,L.Miles,
	-- Case
	--when L.Miles <250 then'Short'
	--when L.Miles between 250 and 500 then 'Medium-Short'
	--when L.Miles between 500 and 1000 then 'Medium'
	--when L.Miles between 1000 and 2000 then 'Medium-Long'
	--when L.Miles >2000 then 'Long' end 'Haul-Length'
	from #Service S
	full join #Bounce B on B.LoadID=S.LoadID and B.CarrierID=S.CarrierID
	full join #Offer O on S.LoadID=O.LoadID and S.CarrierID=O.CarrierID
	inner join bazooka.dbo.LoadCustomer LCUS on LCUS.LoadID = COALESCE(B.LoadID,O.LoadID)
	--inner join bazooka.dbo.Customer CUS on CUS.id=LCUS.CustomerID
	inner join bazooka.dbo.load L on L.id=LCUS.LoadID AND LCUS.Main = 1
	inner join bazooka.dbo.loadstop LSP on LSP.id=L.OriginLoadStopID
	inner join bazooka.dbo.loadstop LSD on LSD.id=L.DestinationLoadStopID
	inner join bazooka.dbo.City CityO on CityO.id=LSP.CityID
	inner join bazooka.dbo.City CityD on CityD.id=LSD.CityID
	LEFT JOIN Analytics.CTM.RateClusters RCO ON RCO.Location = L.OriginCityName + ', ' + L.OriginStateCode
	LEFT JOIN Analytics.CTM.RateClusters RCD ON RCD.Location = L.DestinationCityName + ', ' + L.DestinationStateCode
	left join #Carrier_Corridor Cor on Cor.Corridor=RCO.ClusterNAME +'-'+RCD.ClusterName  
	left join #Carrier_Origin Ori on Ori.OriginCluster=RCO.ClusterNAME  
	left join #Carrier_Dest Dest on Dest.DestinationCluster=RCD.ClusterNAME 
	left join #Carrier_Cust CC on CC.CustID = LCUS.CustomerID  
	inner join bazooka.dbo.CustomerRelationshipManagement  C on C.CustomerID=LCUS.CustomerID
	inner join
	bazooka.dbo.DandBIndustry D  on C.DandBIndustryId=D.DandBIndustryId
	where   rnk=1  
)X
	where pu_Gap>=0
	 order by corridor
	         """

    histload = pd.read_sql(query, cn, params=[CarrierID])
    histload['corridor_max'] = max(histload.corridor_count)
    histload['origin_max'] = max(histload.origin_count)
    histload['dest_max'] = max(histload.dest_count)
    return (histload)

#mapping into matrix -ODE-Carrier
#### set cannot works for class list, redefine makeMatrix and get_odelist using y as a pd.df
##when loading ode-list, remove duplicated rows in pd.df for ode-list
# def makeMatrix(x,y=[originDestinationEquipment],z=[]):
#     kpiMatrix = []
#     for i in z:
#         for j in y:
#             loads=[]
#             std1=[]
#             for k in x.itertuples():
#                 if (k.carrierID == i and k.originCluster == j.origin and k.destinationCluster == j.destination):
#                     loads.append(k)
#                     std1.append(k.kpiScore)
#             kpiMatrix.append(carrier_ode_loads_kpi_std(i,j,loads,np.mean(np.asarray(std1)),np.std(np.asarray(std1))))
#     return kpiMatrix
#
# def get_odelist(loadlist):
#     odelist = []
#     for x in loadlist.itertuples():
#         odelist.append(originDestinationEquipment(x.originCluster, x.destinationCluster, x.equipment))
#     return odelist
### End of Changes - 2018-07-18

def makeMatrix(x,y,z=[]):  #x is the hist load list, y is the unique ode list; x and y are pd.df structure
    kpiMatrix = []
    for i in z:
        for j in y.itertuples():
            loads=[]
            std1=[]
            selectedloads=x[(x['carrierID'] == i) & (x['corridor']==j.corridor)]
            for k in  selectedloads.itertuples():
                loads.append(k)
                std1.append(k.kpiScore)
            #for k in x.itertuples():
                 #if (k.corridor == j.corridor):
                  #  loads.append(k)
                  #  std1.append(k.kpiScore)
            # no need to loop x for i times, as x is ordered by carrierid; needs to find the blocks for carrier[i]
            if (len(selectedloads)>0):
                kpiMatrix.append(carrier_ode_loads_kpi_std(i,j,loads,np.mean(np.asarray(std1)),np.std(np.asarray(std1))))
    return  kpiMatrix

def get_odelist_hist(loadlist):
    odelist = []
    for x in loadlist.itertuples():
        odelist.append({'origin':x.originCluster,'destination':x.destinationCluster,'corridor':x.corridor,'equipment':x.equipment,'corridor_count':x.corridor_count,'corridor_max':x.corridor_max,'origin_count':x.origin_count,'origin_max':x.origin_count,'dest_count':x.dest_count,'dest_max':x.dest_max
                        })
    odelist_df=pd.DataFrame(odelist)
    return odelist_df

def get_odelist_new(loadlist):
    odelist = []
    for x in loadlist.itertuples():
        odelist.append({'origin':x.originCluster,'destination':x.destinationCluster,'corridor':x.corridor,'equipment':x.equipment})
    odelist_df=pd.DataFrame(odelist)
    return odelist_df


def find_ode(kpilist, load, carrierID=None):
    matchlist=[]
    matchindex=[]
    perc=[]
    carriers=[]
##    if carrierID is not None:
##        kpilist=subset(kpilist, kpilist.carrier=carrierID)
    for x in kpilist:
        if x.ode.corridor == load.corridor and x.ode.equipment ==load.equipment :
            matchlist.append(x)
            matchindex.append(kpilist.index(x))
            weight=1
            perc.append(weight)
            carriers.append(x.carrier)
    for x in kpilist:
        if x.carrier not in carriers and x.ode.corridor == load.corridor:
             matchlist.append(x)
             matchindex.append(kpilist.index(x))
             weight=0.7
             perc.append(weight)
             carriers.append(x.carrier)
    for x in kpilist:
        if x.carrier not in carriers and (x.ode.origin == load.origin or x.ode.destination==load.destination) and x.ode.equipment ==load.equipment:
             matchlist.append(x)
             matchindex.append(kpilist.index(x))
             carriers.append(x.carrier)
             origin_weight= x.ode.origin_count/x.ode.origin_max if x.ode.origin_max>0 else 0
             dest_weight=x.ode.dest_count/x.ode.dest_max if x.ode.dest_max>0 else 0
             weight= 1.0*max(origin_weight,dest_weight)
             perc.append(weight)
##        elif x.ode.origin == load.origin:
##            return kpilist.index(x)
    return matchlist,matchindex,perc


def similarity(loadlist, newload, weight):
    carrier_scores = []
    for load in loadlist:
        ori_dist = geopy.distance.vincenty((newload.originLat, newload.originLon),
                                           (load.originLat, load.originLon)).miles
        destination_dist = geopy.distance.vincenty((newload.destinationLat, newload.destinationLon),
                                                   (load.destinationLat, load.destinationLon)).miles
        histload_feature = [ori_dist, destination_dist, load.industryID]
        newload_feature = [0.01, 0.01, newload.industryID]
        sim = 1 - spatial.distance.cosine(histload_feature, newload_feature)

        ##        histload_feature=[ori_dist,destination_dist ]
        ##        if ori_dist==0 and destination_dist==0:
        ##            sim=1
        ##        else:
        ##            newload_feature=[0.01,0.01 ]
        ##            sim=1-spatial.distance.cosine(histload_feature, newload_feature)
        # other feature could be 'pu_GAP','DH' --- need to verify later

        ##    loadlist['ori_dist'] = [
        ##        geopy.distance.vincenty((newload.originLat.tolist()[0], newload.originLon.tolist()[0]), (x.originLat, x.originLon)).miles for x in
        ##        loadList]
        ##    loadlist['des_dist'] = [
        ##        geopy.distance.vincenty((newload.destinationLat.tolist()[0], newload.destinationLon.tolist()[0]),
        ##                                (x.destinationLat, x.destinationLon)).miles for x in loadList.itertuples()]
        origin_weight = 1.0 * load.origin_count / load.origin_max
        dest_weight = 1.0 * load.dest_count / load.dest_max
        corridor_weight = 1.0 * load.corridor_count / load.corridor_max
        carrier_scores.append(
        {'carrierID': load.carrierID, 'loadID': newload.loadID, 'similarity': sim, 'kpi': load.kpiScore,
         'rpm': load.rpm, 'miles': load.miles, 'customer_rate': load.customer_rate, 'weight': weight,
         'origin': newload.originCluster, 'dest': newload.destinationCluster, 'loaddate': newload.loaddate})
    carrier_scores_df = pd.DataFrame(carrier_scores)
    carrier_scores_df['sim_rank'] = carrier_scores_df['similarity'].rank(ascending=False)
    score_df = scoring(carrier_scores_df)
    score_df['expected_margin'] = newload.customer_rate - score_df['rpm'] * newload.miles
    score_df['expected_margin%'] = score_df['expected_margin'] / newload.customer_rate
    return score_df


def scoring(carrier_scores_df):
    # sim_score_weight=0.7
    # group_score_weight=1-sim_score_weight
    k = 0.3  # we can choose different condition: maybe top 5, top 10%, sim> 0.8 etc.
    select_k = max(math.ceil(len(carrier_scores_df) * k), min(10, len(carrier_scores_df)))
    carrier_scores_select = carrier_scores_df[
        carrier_scores_df['sim_rank'] < select_k + 1]  # can be used for kpi matrix construction
    if len(carrier_scores_select) == 0:
        print(carrier_scores_df.iloc[0].carrierID, carrier_scores_df.iloc[0].loadID)
    sim_score = sum(carrier_scores_select.kpi * carrier_scores_select.similarity * carrier_scores_select.weight) / len(
        carrier_scores_select)  # top n loads
    sim_rpm = sum(carrier_scores_select.rpm) / len(carrier_scores_select)
    # group_score=sum(carrier_scores_df.kpi*carrier_scores_df.similarity*carrier_scores_df.weight)/len(carrier_scores_df)   # all group loads
    # score=sim_score*sim_score_weight+group_score*group_score_weight
    score = sim_score
    score_df = {'carrierID': int(carrier_scores_df.iloc[0].carrierID), 'loadID': int(carrier_scores_df.iloc[0].loadID),
                'origin': carrier_scores_df.iloc[0].origin, 'destination': carrier_scores_df.iloc[0].dest,
                'loaddate': carrier_scores_df.iloc[0].loaddate, 'score': score, 'rpm': sim_rpm}

    return score_df

# with open('data.csv', newline='') as csv_file:
#     reader = csv.reader(csv_file)
#     next(reader)  # Skip the header.
#     # Unpack the row directly in the head of the for loop.
#     for Id,carrierId,KPIScore,originDH,originDHLevels,PUGap,originCluster,destinationCluster,equipment,corridorVolume,oriCount,destCount,customerCount,customerAll,customerSize in reader:
#         # Convert the numbers to floats.
#         Id = Id
#         carrierId = carrierId
#         KPIScore = int(KPIScore)
#         originDH = originDH
#         originDHLevels = originDHLevels
#         PUGap = PUGap
#         originCluster = originCluster
#         destinationCluster = destinationCluster
#         equipment = equipment
#         corridorVolume = corridorVolume
#         oriCount = oriCount
#         destCount = destCount
#         customerCount = customerCount
#         customerAll = customerAll
#         customerSize = customerSize
#         # Now create the Student instance and append it to the list.
#         loadList.append(Load(Id,carrierId,KPIScore,originDH,originDHLevels,PUGap,originCluster,destinationCluster,equipment,corridorVolume,oriCount,destCount,customerCount,customerAll,customerSize))
#def check():
if __name__ == "__main__":
    t = TicToc()
    t.tic()
    carrierID = 180543

    carrierinfo=pd.read_csv("truck20180725.csv").iloc[0:2]

    carrier1 = carrierinfo.iloc[0]
    carrier2 = carrierinfo.iloc[1]

    #newloads = Get_testload(carrierID)
    newloads= pd.read_csv("loaddata_0725.csv")
    newloads=newloads[newloads.value<=carrier1.cargolimit]
    newload_ode = get_odelist_new(newloads)
    t.toc('newload')

    t.tic()
    loadList = Give_Carrier_Load_loading(carrierID)
    # loadList=  Carrier_Load_loading(1000)
    t.toc('histload')

    t.tic()
    carriers = sorted(set(loadList.carrierID.tolist()))
    histode = get_odelist_hist(loadList)
    # odelist = set(histode)   # set is not useful for the object list
    odelist = histode.drop_duplicates(subset=['origin', 'destination', 'equipment'])
    t.toc('histode')
    # histod=set(loadList.corridor.tolist())
    ##a=loadList.originCluster.tolist()
    ##b=loadList.destinationCluster.tolist()

    t.tic()
    kpiMatrix = makeMatrix(loadList, odelist, carriers)
    t.toc('kpiMatrix')

    carrier_load_score = []
    t.tic()
    for i in range(0, len(newloads)):
        time_carrier = pd.Timestamp(carrier1.EmptyDate)
        time_load = pd.Timestamp(newloads.iloc[i].pu_appt)
        time_gap = time_load - time_carrier
        matchlist, matchindex, weight = find_ode(kpiMatrix, newload_ode.iloc[i])
        # check for all carriers, return a match list for matched carriers
        for j in range(0, len(matchlist)):
            score = similarity(matchlist[j].loads, newloads.iloc[i], weight[j])
            ###This part is for dynamic info and verification'
            score_update={'originDH':geopy.distance.vincenty((newloads.iloc[i].originLat, newloads.iloc[i].originLon),
                                           (carrier1.OriginLatitude, carrier1.OriginLongitude)).miles,
                          'destDH': geopy.distance.vincenty((newloads.iloc[i].destinationLat, newloads.iloc[i].destinationLon),
                                        (carrier1.DestinationLatitude, carrier1.DestinationLatitude)).miles,
                          'puGAP':  time_gap.days*24+time_gap.seconds/3600}
                            # 'kpi':newloads.iloc[i].kpiScore,
            #               'customer_rate':newloads.iloc[i].customer_rate,'carrier_rate':newloads.iloc[i].carrier_cost,
            #               'margin':newloads.iloc[i].customer_rate - newloads.iloc[i].carrier_cost}

            score.update(score_update)
            ### End
            carrier_load_score.append(score)
        if len(matchlist) == 0:
            score = {'carrierID': carrierID,
                     'loadID': int(newloads.iloc[i].loadID),
                     'origin': newloads.iloc[i].originCluster, 'destination': newloads.iloc[i].destinationCluster,
                     'loaddate': newloads.iloc[i].loaddate, 'score': 0, 'rpm': pd.DataFrame.mean(loadList.rpm),
                     'expected_margin': newloads.iloc[i].customer_rate - pd.DataFrame.mean(loadList.rpm)* newloads.iloc[i].miles,
                     'expected_margin%': 1-pd.DataFrame.mean(loadList.rpm)* newloads.iloc[i].miles/newloads.iloc[i].customer_rate,
                     'originDH': geopy.distance.vincenty((newloads.iloc[i].originLat, newloads.iloc[i].originLon),
                                                         (carrier1.originLat, carrier1.originLon)).miles,
                     'destDH': geopy.distance.vincenty(
                         (newloads.iloc[i].destinationLat, newloads.iloc[i].destinationLon),
                         (carrier1.destLat, carrier1.destLat)).miles,
                     'puGAP':  time_gap.days*24+time_gap.seconds/3600}
                      #carrier1 is a test
                     # 'DH': newloads.iloc[i].originDH,
                     # 'puGAP': newloads.iloc[i].pu_GAP, 'kpi': newloads.iloc[i].kpiScore,
                     # 'customer_rate': newloads.iloc[i].customer_rate,
                     # 'carrier_rate': newloads.iloc[i].carrier_cost,
                     # 'margin': newloads.iloc[i].customer_rate - newloads.iloc[i].carrier_cost}
            carrier_load_score.append(score)
    results = pd.DataFrame(carrier_load_score)
    datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    results.to_csv(
        'carrier' + str(carrierID) + '_load_recommender' + datetime.datetime.now().strftime("%Y%m%d-%H%M") + '.csv',
        index=False,
        columns=['carrierID', 'loadID', 'loaddate', 'origin', 'destination', 'originDH', 'destDH',
                 'puGAP', 'customer_rate',
                 'expected_margin', 'expected_margin%','score'])
    t.toc('scores')

def recommender(carrierID):
    t = TicToc()
    t.tic()
    newloads = Get_newload()
    newload_ode = get_odelist_new(newloads)
    t.toc('newload')

    t.tic()
    loadList = Give_Carrier_Load_loading(carrierID)
    # loadList = Carrier_Load_loading(10)

    carriers = sorted(set(loadList.carrierID.tolist()))
    histode = get_odelist_hist(loadList)
    # odelist = set(histode)   # set is not useful for the object list
    odelist = histode.drop_duplicates(subset=['origin', 'destination', 'equipment'])

    # histod=set(loadList.corridor.tolist())
    ##a=loadList.originCluster.tolist()
    ##b=loadList.destinationCluster.tolist()

    kpiMatrix = makeMatrix(loadList, odelist, carriers)
    t.toc('histload_matrix')

    carrier_load_score = []
    t.tic()
    for i in range(0, len(newloads)):
        matchlist, matchindex, weight = find_ode(kpiMatrix, newload_ode.iloc[
            i])  # check for all carriers, return a match list for matched carriers
        for j in range(0, len(matchlist)):
            score = similarity(matchlist[j].loads, newloads.iloc[i], weight[j])
            carrier_load_score.append(score)
    results = pd.DataFrame(carrier_load_score).sort_values(by=['score'],ascending=False)
    datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    results.to_csv(
        'carrier' + str(carrierID) + '_load_recommender' + datetime.datetime.now().strftime("%Y%m%d-%H%M") + '.csv',
        index=False,
        columns=['carrierID', 'loadID', 'origin', 'destination','loaddate', 'score','expected_margin','expected_margin%'])
    t.toc('scores')
    return (0)

def rec():
#if __name__ == "__main__":
    carrierID = int(input('CarrierID:'))
    recommender(carrierID)



