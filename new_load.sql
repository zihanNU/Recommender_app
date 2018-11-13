    declare @date1 as date = ?
    declare @date2 as date = ?

    select L.Id  'loadID', convert (date,L.loaddate) 'loaddate',l.TotalValue 'value',
    --LRD.Cost 'customer_rate', 
    L.totalrate 'customer_rate', L.EquipmentType 'equipment',
    L.equipmentlength,
    L.miles,
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
	--, case when  l.equipmenttype like '%V%' then 'V' when  l.equipmenttype like 'R' then 'R' else 'other' end 'equipment'
	,COALESCE(C.DandBIndustryId,0)  'industryID', 
	COALESCE(D.Code,'unknown') 'industry'
    from bazooka.dbo.load L 
    inner join bazooka.dbo.LoadCustomer LCUS on L.id=LCUS.LoadID AND LCUS.Main = 1
	INNER JOIN Bazooka.dbo.Customer CUS ON LCUS.CustomerID = CUS.ID
	LEFT JOIN Bazooka.dbo.Customer PCUS ON CUS.ParentCustomerID = PCUS.ID
    inner join bazooka.dbo.loadstop LSP on LSP.id=L.OriginLoadStopID
    inner join bazooka.dbo.loadstop LSD on LSD.id=L.DestinationLoadStopID
    inner join bazooka.dbo.City CityO on CityO.id=LSP.CityID
    inner join bazooka.dbo.City CityD on CityD.id=LSD.CityID
    LEFT JOIN Analytics.CTM.RateClusters RCO ON RCO.Location = L.OriginCityName + ', ' + L.OriginStateCode
    LEFT JOIN Analytics.CTM.RateClusters RCD ON RCD.Location = L.DestinationCityName + ', ' + L.DestinationStateCode
	left join bazooka.dbo.CustomerRelationshipManagement  C on C.CustomerID=LCUS.CustomerID
	left join
	Analytics.bazooka.dbo.DandBIndustry D  on C.DandBIndustryId=D.DandBIndustryId
	--inner join (select  loadid, SUM(amount) 'Cost' from Bazooka.dbo.LoadRateDetail 
	--				where EntityType = 12 and EDIDataElementCode IN  ('405','FR',  'PM' ,'MN','SCL','OT','EXP') --and CreateDate > '2018-01-01' 
	--				Group by loadid) LRD on LRD.loadid = L.Id
   where 
   L.StateType = 1 and L.progresstype=1 and L.totalrate>150
    and  L.LoadDate between @Date1 and @Date2  and L.Miles>0 and L.division between 1 and 2
    AND L.Mode = 1  
    --AND LCAR.CarrierID=@CarrierID
    AND L.ShipmentType not in (3,4,6,7)
   -- AND (CASE WHEN L.EquipmentType LIKE '%V%' THEN 'V' ELSE L.EquipmentType END) IN ('V', 'R')
    --AND CAR.ContractVersion NOT IN ('TMS FILE', 'UPSDS CTM', 'UPSCD CTM') --Exclude Managed Loads
    --AND COALESCE(PCUS.CODE,CUS.CODE) NOT IN ('UPSAMZGA','UPSRAILPEA')
    --AND L.TotalRAte >= 150 AND L.TotalCost >= 150
    AND  L.[OriginStateCode] in (select [Code]  FROM [Bazooka].[dbo].[State] where [ID]<=51)
    AND  L.[DestinationStateCode] in (select [Code]  FROM [Bazooka].[dbo].[State] where [ID]<=51)
	and CUS.Name not like 'UPS%'
	AND COALESCE(PCUS.CODE,CUS.CODE) NOT IN ('UPSAMZGA','UPSRAILPEA')