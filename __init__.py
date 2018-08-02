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
from scipy import stats

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
	where EntityType = 12 and EDIDataElementCode IN  ('405','FR',  'PM' ,'MN','SCL','OT','EXP') --and CreateDate > '2018-01-01' 
	Group by entityid) LRD on LRD.entityid = lcus.id
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
	(O.cost-(case when  B.Accept=1 then l.totalcost else o.Ask  end ) )/O.cost*100 'margin_perc',
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
    if (len(histload)==0):
        return {'flag':0,'histload':0}
    histload['corridor_max']=max(histload.corridor_count)
    histload['origin_max']=max(histload.origin_count)
    histload['dest_max']=max(histload.dest_count)
    return {'flag':1,'histload':histload}

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
    L.OriginCityName + '-'+L.OriginStateCode 'origin',
	L.DestinationCityName + '-'+L.DestinationStateCode 'destination',
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
    #matchindex=[]
    perc=[]
    carriers=[]
##    if carrierID is not None:
##        kpilist=subset(kpilist, kpilist.carrier=carrierID)
    for x in kpilist:
        if x.ode.corridor == load.corridor and x.ode.equipment ==load.equipment :
            matchlist.append(x)
            #matchindex.append(kpilist.index(x))
            weight=1
            perc.append(weight)
            carriers.append(x.carrier)
    for x in kpilist:
        if x.carrier not in carriers and x.ode.corridor == load.corridor:
             matchlist.append(x)
             #matchindex.append(kpilist.index(x))
             weight=0.7
             perc.append(weight)
             carriers.append(x.carrier)
    for x in kpilist:
        if x.carrier not in carriers and (x.ode.origin == load.origin or x.ode.destination==load.destination) and x.ode.equipment ==load.equipment:
             matchlist.append(x)
             #matchindex.append(kpilist.index(x))
             carriers.append(x.carrier)
             if x.ode.origin_max>0:
                 origin_weight= x.ode.origin_count/x.ode.origin_max
             else:
                 origin_weight=0
             if x.ode.dest_max>0:
                 dest_weight=x.ode.dest_count/x.ode.dest_max
             else:
                 dest_weight=0
             weight= 1.0*max(origin_weight,dest_weight)
             perc.append(weight)
##        elif x.ode.origin == load.origin:
##            return kpilist.index(x)
    return matchlist, perc


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
        # origin_weight = 1.0 * load.origin_count / load.origin_max
        # dest_weight = 1.0 * load.dest_count / load.dest_max
        # corridor_weight = 1.0 * load.corridor_count / load.corridor_max
        carrier_scores.append(
        {'carrierID': load.carrierID, 'loadID': newload.loadID, 'similarity': sim, 'kpi': load.kpiScore,
         'rpm': load.rpm, 'miles': load.miles, 'customer_rate': load.customer_rate, 'weight': weight,
         'margin_perc':load.margin_perc,
         # 'origin': newload.originCluster, 'dest': newload.destinationCluster, 'loaddate': newload.loaddate
         })
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
    carrier_info=carrier_scores_df.iloc[0]
    select_k = max(math.ceil(len(carrier_scores_df) * k), min(10, len(carrier_scores_df)))
    carrier_scores_select = carrier_scores_df[
        carrier_scores_df['sim_rank'] < select_k + 1]  # can be used for kpi matrix construction
    if len(carrier_scores_select) == 0:
        print(carrier_info.carrierID, carrier_info.loadID)
    sim_score = sum(carrier_scores_select.kpi * carrier_scores_select.similarity * carrier_scores_select.weight) / len(
        carrier_scores_select)  # top n loads
    sim_margin = sum(carrier_scores_select.margin_perc) / len(carrier_scores_select)
    sim_rpm = sum(carrier_scores_select.rpm) / len(carrier_scores_select)
    # group_score=sum(carrier_scores_df.kpi*carrier_scores_df.similarity*carrier_scores_df.weight)/len(carrier_scores_df)   # all group loads
    # score=sim_score*sim_score_weight+group_score*group_score_weight
    score = sim_score
    score_df = {'carrierID': int(carrier_info.carrierID), 'loadID': int(carrier_info.loadID),
                # 'origin': carrier_info.origin, 'destination': carrier_info.dest,
                # 'loaddate': carrier_info.loaddate,
                'hist_perf': score, 'rpm': sim_rpm, 'margin_perc': sim_margin}

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


def check(carrier,newloads):
    t = TicToc()
    carrierID=int(carrier.carrierID)
    t.tic()
    carrier_load = Give_Carrier_Load_loading(carrierID)
    corridor_info=pd.read_csv("corridor_margin.csv")
    t.toc('load hist data')
    if carrier_load['flag']==1:
        loadList=carrier_load['histload']
        # loadList=  Carrier_Load_loading(1000)
        carrier_load_score=hist_recommender(carrier, newloads, loadList)
    else:
        carrier_load_score=dyna_recommender(carrier,newloads,corridor_info)

    return (carrier_load_score)

def dyna_recommender(carrier,newloads,corridor_info):
### do not need any more, as the originDH etc dynamic info is calculated first seperated from hist performac score
# margin and rpm and margin perc, needs to use all data from this corridor, no need to grab only from this carrier if this is a new carrier
    carrier_load_score=[]
    carrierID = int(carrier.carrierID)
    for i in range(0, len(newloads)):
        newload = newloads.iloc[i]
        rpm= corridor_info[corridor_info.corridor==newload.corridor].rpm
        score = {'carrierID': carrierID,
             'loadID': newload.loadID,
             # 'origin': newload.originCluster, 'destination': newload.destinationCluster,
             # 'loaddate': newload.loaddate,
                 'hist_perf': 0, 'rpm': rpm,
                 'expected_margin': newload.customer_rate - rpm * (newload.miles + newload.originDH),
                 'expected_margin%': 100 - rpm * (newload.miles + newload.originDH) / newload.customer_rate * 100,
                 'margin_perc': corridor_info[corridor_info.corridor==newload.corridor].margin_perc
             }
        carrier_load_score.append(score)
    return (carrier_load_score)


def hist_recommender(carrier,newloads,loadList):
    """once there is any historical information for given carrier, use historical info to calculate the scores(hist preference)"""
    t = TicToc()
    carrierID = int(carrier.carrierID)
    t.tic()
    newload_ode = get_odelist_new(newloads)
    t.toc('newload')

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
        newload=newloads.iloc[i]
        new_ode=newload_ode.iloc[i]
        time_carrier = pd.Timestamp(carrier.EmptyDate)
        time_load = pd.Timestamp(newload.pu_appt)
        time_gap = time_load - time_carrier

        matchlist,   weight = find_ode(kpiMatrix,new_ode )
        # check for all carriers, return a match list for matched carriers

        for j in range(0, len(matchlist)):
            score = similarity(matchlist[j].loads, newload, weight[j])
            ###This part is for dynamic info and verification'
            # 'kpi':newloads.iloc[i].kpiScore,
            #               'customer_rate':newloads.iloc[i].customer_rate,'carrier_rate':newloads.iloc[i].carrier_cost,
            #               'margin':newloads.iloc[i].customer_rate - newloads.iloc[i].carrier_cost}
            ### End
            carrier_load_score.append(score)
        if len(matchlist) == 0:
            score = {'carrierID': carrierID,
                     'loadID': int(newload.loadID),
                     # 'origin':  'destination': ,
                     # 'corridor':newload.originCluster + newload.destinationCluster,
                     # 'loaddate': newload.loaddate,
                     'hist_perf': 0, 'rpm': pd.DataFrame.mean(loadList.rpm),
                     'expected_margin': newload.customer_rate - pd.DataFrame.mean(loadList.rpm) *
                                        (newload.miles+newload.originDH),
                     'expected_margin%': 100 - pd.DataFrame.mean(loadList.rpm) * (newload.miles+newload.originDH)/newload.customer_rate*100,
                     'margin_perc':pd.DataFrame.mean(loadList.margin_perc)}
            # carrier1 is a test
            # 'DH': newloads.iloc[i].originDH,
            # 'puGAP': newloads.iloc[i].pu_GAP, 'kpi': newloads.iloc[i].kpiScore,
            # 'customer_rate': newloads.iloc[i].customer_rate,
            # 'carrier_rate': newloads.iloc[i].carrier_cost,
            # 'margin': newloads.iloc[i].customer_rate - newloads.iloc[i].carrier_cost}
            carrier_load_score.append(score)
    t.toc('scoring')
    return (carrier_load_score)


def score_DH(DH,radius,penalty_radius):
    radius_DH=[i for i in DH if i<=radius]
    penalty_DH=[i for i in DH if i<=penalty_radius and i>=radius]
    score =np.array([100-stats.percentileofscore(radius_DH, i) for i in DH])
    #penalty =np.array([stats.percentileofscore(penalty_DH, i) for i in DH])
    return  score


def pu_Gap(pu_appt,EmptyDate):
    time_gap=pu_appt-EmptyDate
    return time_gap.days * 24 + time_gap.seconds / 3600


if __name__ == "__main__":
    newloadsall_df= pd.read_csv("loadtest.csv")
    #initialize 3 column features. if carrier put any info related to DH or puGap,we can update
    newloadsall_df['originDH'] = 0
    newloadsall_df['destDH'] = 0
    newloadsall_df['puGap'] = 0
    newloadsall_df['totalDH']= 0
    #newloadsall = Get_testload(carrierID)
    carrier_df=pd.read_csv("truck20180730.csv")
    for carrier in carrier_df.itertuples():
        # we may need to add a if condition, say if carrier put its lat and lon, and empty time.
        newloads_df = newloadsall_df[
            (newloadsall_df.value <= carrier.cargolimit) & (newloadsall_df.equipment == carrier.EquipmentType)]
        newloads_update = {
             'originDH': newloads_df.apply(lambda row: geopy.distance.vincenty((row.originLat, row.originLon), (
                 carrier.originLat, carrier.originLon)).miles, axis=1),
             'destDH': newloads_df.apply(lambda row: geopy.distance.vincenty(
                                                           (row.destinationLat, row.destinationLon),
                                                           (carrier.destLat, carrier.destLon)).miles , axis=1),
              # 'totalDH': newloads.apply(lambda row: row.originDH + row.destDH, axis=1),
             'puGap':newloads_df.apply(lambda row: pu_Gap(pd.Timestamp(row.pu_appt), pd.Timestamp(carrier.EmptyDate)),
                                       axis=1)
         }
        newloads_df.update(pd.DataFrame(newloads_update))
        newloads_df['totalDH'] = newloads_df.apply(lambda row: row.originDH + row.destDH, axis=1)
        # newloads['originDH'] = newloads.apply(lambda row: geopy.distance.vincenty((row.originLat, row.originLon), (
        #                                                  carrier.originLat, carrier.originLon)).miles, axis=1)
        # newloads['destDH'] = newloads.apply(lambda row:  geopy.distance.vincenty(
        #                                                  (row.destinationLat, row.destinationLon),
        #                                                  (carrier.destLat, carrier.destLon)).miles , axis=1)
        # newloads['totalDH'] = newloads.apply(lambda row: row.originDH+row.destDH,axis=1)
        # newloads['puGAP'] = newloads.apply(lambda row: puGap(pd.Timestamp(row.pu_appt),pd.Timestamp(carrier.EmptyDate)), axis=1)
        # newloads_df['originDH'] = newloads_df.apply(lambda row: geopy.distance.vincenty(
        #                 (row.originLat, row.originLon), (carrier.originLat, carrier.originLon)).miles, axis=1).tolist()
        # newloads_df['destDH']=newloads_df.apply(lambda row: geopy.distance.vincenty(
        #             (row.destinationLat, row.destinationLon),(carrier.destLat, carrier.destLon)).miles, axis=1).tolist()
        # newloadsall_df['puGap']=newloads_df.apply(lambda row: puGap(pd.Timestamp(row.pu_appt),
        #                                                             pd.Timestamp(carrier.EmptyDate)), axis=1).tolist()

        # newloads_df['totalDH'] = np.array(newloads_df['originDH'].tolist()) + np.array(newloads_df['destDH'].tolist())
        # newloads_df.to_csv(
        #     'carrier_all' + str(carrier.carrierID) + '_load_recommender' + datetime.datetime.now().strftime(
        #         "%Y%m%d-%H%M%S") + '.csv',
        #     index=False,)
        print (carrier.carrierID)
        newloads_select = newloads_df [(newloads_df.originDH<250) & (newloads_df.totalDH<500)]
        # 500 and 800 are the threshold radius of DH
        if len(newloads_select)>0:
            carrier_load_score=check(carrier,newloads_select)
            # if (len(carrier_load_score) > 0):
            results_df = pd.DataFrame(carrier_load_score).merge(newloads_df,left_on="loadID",right_on="loadID",how='inner')
            #results_df.merge(newloads_df,left_on="loadID",right_on="loadID",how='inner')
            results_df['ODH_Score']=score_DH(results_df['originDH'].tolist(),250,500)
            results_df['totalDH']=  results_df['originDH'] +  results_df['destDH']
            results_df['totalDH_Score'] = score_DH(results_df['totalDH'].tolist(), 500, 800)
            results_df['puGap_Score'] = score_DH(results_df['puGap'].tolist(), 24, 36)
            results_df['Score'] = results_df['ODH_Score'] * 0.40 + results_df['totalDH_Score'] * 0.60 + results_df['hist_perf'] * 0.3 \
                               + results_df['expected_margin%']* 0.3 + results_df['puGap_Score']* 0.15
            results_df['Score']= results_df['Score'] /max(results_df['Score'])*100
            datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
            results_df.to_csv(
                'carrier' + str(carrier.carrierID) + '_load_recommender' + datetime.datetime.now().strftime(
                    "%Y%m%d-%H%M%S") + '.csv',
                index=False,
                columns=['carrierID', 'loadID', 'loaddate', 'origin', 'destination', 'originDH', 'destDH',
                         'totalDH',
                         'puGap','ODH_Score','totalDH_Score',
                         'expected_margin', 'expected_margin%','margin_perc', 'hist_perf','Score'])





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
        newload=newloads.iloc[i]
        new_ode=newload_ode.iloc[i]
        matchlist,   weight = find_ode(kpiMatrix,new_ode)  # check for all carriers, return a match list for matched carriers
        for j in range(0, len(matchlist)):
            score = similarity(matchlist[j].loads, newload, weight[j])
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

def ecdf(data):
    """Compute ECDF for a one-dimensional numpy array of measurements."""
    recordsNumber = len(data)
    x = np.sort(data)
    y = np.arange(1, recordsNumber + 1) / recordsNumber
    return x, y


