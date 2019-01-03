    SET NOCOUNT ON
    declare @date1 as date = getdate()
    declare @date2 as date = '2018-11-16'

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
	distinct L.Id  'loadID'
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
	order by loadID