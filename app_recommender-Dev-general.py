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


app = Flask (__name__)


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


def Get_truckinsurance(carrierID):
    cn = pyodbc.connect('DRIVER={SQL Server};SERVER=ANALYTICSPROD;DATABASE=Bazooka;trusted_connection=true')
    query= """
       select 
        case when cargolimit= 0 then 500000 else cargolimit end 'cargolimit'
		 from
        bazooka.dbo.Carrier Car 
        where Car.ID=?
        """
    truck=pd.read_sql(query,cn,params= [carrierID])
    return truck.cargolimit.tolist()[0]

def Get_truck(carrierID):
    cn = pyodbc.connect('DRIVER={SQL Server};SERVER=ANALYTICSPROD;DATABASE=Bazooka;trusted_connection=true')
    query= """
    select 
    carrierID,car.name,EmptyDate,EquipmentType,EquipmentLength,
    OriginLatitude 'originLat',
    OriginLongitude    'originLon',
    DestinationLatitude   'destLat',
    DestinationLongitude  'destLon',
    originDeadHead 'originDeadHead_radius',
    destinationDeadHead 'destinationDeadHead_radius',
    car.cargolimit
    from bazooka.dbo.Truck Tru
     inner join bazooka.dbo.Carrier Car on Car.ID=Tru.CarrierID and Name not like 'UPS%'
    where convert (date,EmptyDate) between convert(date,getdate ()) and convert(date,dateadd (day,1,GETDATE()))
     and OriginLongitude<0 and DestinationLongitude<0
    and carrierID=?
    """
    trucks=pd.read_sql(query,cn,params= [carrierID])
    return trucks

#Give CarrierID
def Get_Carrier_histLoad (CarrierID,date1,date2):
    cn = pyodbc.connect('DRIVER={SQL Server};SERVER=ANALYTICSDev;DATABASE=ResearchScience;trusted_connection=true')
    query= """
        set nocount on
        declare @CarrierID as int =?   
        --declare @date1 as date = ?
        --declare @date2 as date =?
        select * from [ResearchScience].[dbo].[Recommendation_HistLoad]
        where CarrierID= @CarrierID  
        --and loaddate between @date1 and @date2
	"""

    histload=pd.read_sql(query,cn,params= [CarrierID])
    if (len(histload)==0):
        return {'flag':0,'histload':0}
    #histload['corridor_max']=max(histload.corridor_count)
    histload['origin_max']=max(histload.origin_count)
    histload['dest_max']=max(histload.dest_count)
    return {'flag':1,'histload':histload}

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



def makeMatrix(x,y,z):  #x is the hist load list, y is the unique ode list; x and y are pd.df structure
    kpiMatrix = []
    odlist=[]
    for j in y.itertuples():
        loads=[]
        std1=[]
        selectedloads=x[(x['carrierID'] == z) & (x['corridor']==j.corridor)]   ### Check this capital or little c
        for k in  selectedloads.itertuples():
            loads.append(k)
            std1.append(k.kpiScore)
        if (len(selectedloads)>0):
            odlist.append(j.corridor)
            kpiMatrix.append(carrier_ode_loads_kpi_std(z,j,loads,np.mean(np.asarray(std1)),np.std(np.asarray(std1))))
    return  kpiMatrix, odlist

def get_odelist_hist(loadlist):
    odelist = []
    for x in loadlist.itertuples():
        # odelist.append({'origin':x.originCluster,'destination':x.destinationCluster,'corridor':x.corridor,'equipment':x.equipment,'corridor_count':x.corridor_count,'corridor_max':x.corridor_max,'origin_count':x.origin_count,'origin_max':x.origin_count,'dest_count':x.dest_count,'dest_max':x.dest_max
        #                 })
        odelist.append({'origin':x.originCluster,'destination':x.destinationCluster,'corridor':x.corridor,'equipment':x.equipment,'origin_count':x.origin_count,'origin_max':x.origin_count,'dest_count':x.dest_count,'dest_max':x.dest_max
                        })
    odelist_df=pd.DataFrame(odelist)
    return odelist_df

def get_odelist_new(loadlist):
    odelist = []
    for x in loadlist.itertuples():
        odelist.append({'origin':x.originCluster,'destination':x.destinationCluster,'corridor':x.corridor,'equipment':x.equipment})
    odelist_df=pd.DataFrame(odelist)
    return odelist_df

def find_ode(kpilist, load, odlist ):
    matchlist=[]
    #matchindex=[]
    perc=[]
    #carriers=[]
    vol=0

    if load.corridor in odlist:
        loc=odlist.index(load.corridor)
        x=kpilist[loc]
        if  x.ode.equipment ==load.equipment :
            weight=1.0
        else:
            weight=0.9
        matchlist.append(x.loads)
        #matchindex.append(kpilist.index(x))
        perc.append(weight)
        vol=len(matchlist)

    else:
        for x in kpilist:
        #if x.carrier not in carriers and (x.ode.origin == load.origin or x.ode.destination==load.destination) and x.ode.equipment ==load.equipment:
            if x.ode.origin == load.origin or x.ode.destination == load.destination:
                 matchlist.append(x.loads)
                 #matchindex.append(kpilist.index(x))

                 if x.ode.origin_max>0:
                     origin_weight= x.ode.origin_count/x.ode.origin_max
                 else:
                     origin_weight=0
                 if x.ode.dest_max>0:
                     dest_weight=x.ode.dest_count/x.ode.dest_max
                 else:
                     dest_weight=0
                 weight= 0.9*max(origin_weight,dest_weight)
                 perc.append(weight)
 
    if len(matchlist)>0:   #merge match list, if we find mutiple matches for either origin cluster or destination cluster
        matchlist_merge=[]
        perc_merge=[]
        for i in range(0,len(matchlist)):
            for j in range(0,len(matchlist[i])):
                matchlist_merge.append(matchlist[i][j])
                perc_merge.append(perc[i])
        return matchlist_merge,perc_merge, vol
    return  matchlist, perc, vol

def similarity(loadlist, newload, weight):
    carrier_scores = []
    for i in range (0,len(loadlist)):
        load = loadlist[i]
        ori_dist = geopy.distance.vincenty((newload.originLat, newload.originLon),
                                           (load.originLat, load.originLon)).miles
        destination_dist = geopy.distance.vincenty((newload.destinationLat, newload.destinationLon),
                                                   (load.destinationLat, load.destinationLon)).miles
        histload_feature = [ori_dist, destination_dist, load.industryID*10,load.miles/10]
        newload_feature = [0.01, 0.01, newload.industryID*10, newload.miles/10]
        sim = 1 - spatial.distance.cosine(histload_feature, newload_feature)
        # other feature could be 'pu_GAP','DH' --- need to verify later         
        carrier_scores.append(
        {'carrierID': load.carrierID, 'loadID': newload.loadID, 'similarity': sim, 'kpi': load.kpiScore,
         'rpm': load.rpm, 'miles': load.miles, 'customer_rate': load.customer_rate, 'weight': weight[i],
         'margin_perc':load.margin_perc,
         # 'origin': newload.originCluster, 'dest': newload.destinationCluster, 'loaddate': newload.loaddate
         })
    carrier_scores_df = pd.DataFrame(carrier_scores)
 
    score_df = hist_scoring(carrier_scores_df, load.carrierID, newload.loadID)
 
    score_df['estimated_margin'] = newload.customer_rate - score_df['rpm'] * (newload.miles)
    score_df['estimated_margin%'] = score_df['estimated_margin'] / newload.customer_rate*100
    return score_df

def hist_scoring(carrier_scores_df, carrierID, loadID):
    k = 0.3
    # we can choose different condition: maybe top 5, top 10%, sim> 0.8 etc.
    
    select_k = max(math.ceil(len(carrier_scores_df) * k), min(10, len(carrier_scores_df)))
    
    carrier_scores_select = carrier_scores_df.sort_values(by=['similarity', 'kpi'], ascending=False)[0:select_k]
    #if len(carrier_scores_select) == 0:
        #print(carrier_info.carrierID, carrier_info.loadID)
        #print(carrierID, loadID)
    sim_score = sum(carrier_scores_select.kpi * carrier_scores_select.similarity * carrier_scores_select.weight) / len(carrier_scores_select)  # top n loads
    sim_margin = sum(carrier_scores_select.margin_perc) / len(carrier_scores_select)
    sim_rpm = sum(carrier_scores_select.rpm) / len(carrier_scores_select)
    
    score = sim_score
 
    score_df = {'carrierID': carrierID, 'loadID': loadID,
                'hist_perf': score, 'rpm': sim_rpm, 'margin_perc': sim_margin}

    return score_df


def check(carrier_load,newloads,carrier):
    if carrier_load['flag']==1:
        loadList=carrier_load['histload']
        #loadList.to_csv('carrier' + str(carrier.carrierID) + 'histload.csv',index=False)
        # loadList=  Carrier_Load_loading(1000)
        carrier_load_score=indiv_recommender(carrier, newloads, loadList)
    else:
        t=TicToc()
        t.tic()
        carrier_load_score=general_recommender(carrier,newloads)
        t.toc('general')
    return (carrier_load_score)


def newload_general(newload):
    return


def general_recommender(carrier,newloads):
##for new carriers, which has no hist data
# margin and rpm and margin perc, needs to use all data from this corridor, no need to grab only from this carrier if this is a new carrier
    carrier_load_score=[]
    carrierID = int(carrier.carrierID)
    corridor_info = read_corridor()
    newloads['hist_pref']=0
    newloads['desired_OD']=0
    newloads['estimated_margin%']=0
    newloads_temp={'estimated_margin%': newloads.apply(lambda row:new_hist_score(row,kpiMatrix, kpi_odlist,len(loadList)),axis=1)}
    newloads.update(pd.DataFrame(newloads_temp))

    for i in range(0, len(newloads)):
        newload = newloads.iloc[i]
        if (any(corridor_info.corridor==newload.corridor)):
            rpm= corridor_info[corridor_info.corridor==newload.corridor].rpm.values[0]
            estimate_margin_p = corridor_info[corridor_info.corridor == newload.corridor].corrdor_margin_perc.values[0]
        elif (any(corridor_info.OriginCluster==newload.originCluster)):
            rpm = pd.DataFrame.mean(corridor_info[corridor_info.OriginCluster == newload.originCluster].rpm)
            estimate_margin_p= pd.DataFrame.mean(corridor_info[corridor_info.OriginCluster == newload.originCluster].corrdor_margin_perc)
        else:
            rpm=pd.DataFrame.mean(corridor_info.rpm)
            estimate_margin_p = pd.DataFrame.mean(corridor_info.corrdor_margin_perc)
        score = {'carrierID': carrierID,
                'loadID': newload.loadID,
             # 'origin': newload.originCluster, 'destination': newload.destinationCluster,
             # 'loaddate': newload.loaddate,
                 'hist_perf': 0, 'rpm': rpm,
                 #'estimated_margin': newload.customer_rate - rpm * (newload.miles + newload.originDH),
                 'estimated_margin': newload.customer_rate - rpm * (newload.miles),
                 'estimated_margin%': estimate_margin_p,
                 'margin_perc': estimate_margin_p,
                 'desired_OD': 0
             }
        carrier_load_score.append(score)
    return (carrier_load_score)

def general_recommender_old(carrier,newloads):
##for new carriers, which has no hist data
# margin and rpm and margin perc, needs to use all data from this corridor, no need to grab only from this carrier if this is a new carrier
    carrier_load_score=[]
    carrierID = int(carrier.carrierID)
    corridor_info = read_corridor()
    for i in range(0, len(newloads)):
        newload = newloads.iloc[i]
        if (any(corridor_info.corridor==newload.corridor)):
            rpm= corridor_info[corridor_info.corridor==newload.corridor].rpm.values[0]
            estimate_margin_p = corridor_info[corridor_info.corridor == newload.corridor].corrdor_margin_perc.values[0]
        elif (any(corridor_info.OriginCluster==newload.originCluster)):
            rpm = pd.DataFrame.mean(corridor_info[corridor_info.OriginCluster == newload.originCluster].rpm)
            estimate_margin_p= pd.DataFrame.mean(corridor_info[corridor_info.OriginCluster == newload.originCluster].corrdor_margin_perc)
        else:
            rpm=pd.DataFrame.mean(corridor_info.rpm)
            estimate_margin_p = pd.DataFrame.mean(corridor_info.corrdor_margin_perc)
        score = {'carrierID': carrierID,
             'loadID': newload.loadID,
             # 'origin': newload.originCluster, 'destination': newload.destinationCluster,
             # 'loaddate': newload.loaddate,
                 'hist_perf': 0, 'rpm': rpm,
                 #'estimated_margin': newload.customer_rate - rpm * (newload.miles + newload.originDH),
                 'estimated_margin': newload.customer_rate - rpm * (newload.miles),
                 'estimated_margin%': estimate_margin_p,
                 'margin_perc': estimate_margin_p,
                 'desired_OD': 0
             }
        carrier_load_score.append(score)
    return (carrier_load_score)

def indiv_recommender(carrier,newloads,loadList):
    """once there is any historical information for given carrier, use historical info to calculate the scores(hist preference)"""
    carrierID = int(carrier.carrierID)
    newload_ode = get_odelist_new(newloads)
    t=TicToc()
    histode = get_odelist_hist(loadList)
    # odelist = set(histode)   # set is not useful for the object list
    odelist = histode.drop_duplicates(subset=['origin', 'destination', 'equipment'])
    t.tic()
    #kpiMatrix,kpi_odlist = multi_makeMatrix(loadList, odelist, carriers)  #this one is for multiple carriers
    kpiMatrix, kpi_odlist = makeMatrix(loadList, odelist, carrierID)
    t.toc('kpimatrix')
    carrier_load_score = []
    t.tic()
    for i in range(0, len(newloads)):
        newload=newloads.iloc[i]
        new_ode=newload_ode.iloc[i]
        matchlist,   weight, corridor_vol = find_ode(kpiMatrix,new_ode,kpi_odlist )
        if len(matchlist) > 0:
            score = similarity(matchlist, newload, weight)
            score['desired_OD'] = 100 if corridor_vol > min(len(loadList) * 0.1, 10) else 0
        else:
            score = {'carrierID': carrierID,
                     'loadID': int(newload.loadID),
                     'hist_perf': 0, 'rpm': pd.DataFrame.mean(loadList.rpm),
                  #'estimated_margin': newload.customer_rate - pd.DataFrame.mean(loadList.rpm) * (newload.miles+newload.originDH),
                     'estimated_margin': newload.customer_rate - pd.DataFrame.mean(loadList.rpm) * (newload.miles),
                      'estimated_margin%': 100 - pd.DataFrame.mean(loadList.rpm) * (newload.miles+newload.originDH)/newload.customer_rate*100,
                      'margin_perc':pd.DataFrame.mean(loadList.margin_perc),
                      'desired_OD': 0}
        carrier_load_score.append(score)
    t.toc('scoring')
    return (carrier_load_score)

def score_DH(DH,radius ):
    score=(radius-np.array(DH))/radius*100
    score_check=[min(max(0,a),100) for a in score]
    return  score_check

def pu_Gap(pu_appt,EmptyDate,traveltime):
    time_gap=pu_appt-EmptyDate
    return time_gap.days * 24-traveltime + time_gap.seconds / 3600

def dynamic_input(newloads_df,carrier):
    ##This part is for new api input
    # newloads_df['originDH'] = originDH
    # newloads_df['destDH'] = destDH
    # newloads_df['puGap'] = gap
    # newloads_df['totalDH'] = originDH+destDH
    if  carrier.originLat is not None and carrier.originLon is not None:
         newloads_ODH= {'originDH': newloads_df.apply(lambda row: geopy.distance.vincenty((row.originLat, row.originLon), (
             float(carrier.originLat), float(carrier.originLon))).miles, axis=1)}

         newloads_df.update(pd.DataFrame(newloads_ODH))
    if  carrier.destLat is not None and carrier.destLon is not None:
         newloads_DDH= {'destDH': newloads_df.apply(lambda row: geopy.distance.vincenty((row.originLat, row.originLon), (
             float(carrier.destLat), float(carrier.destLon))).miles, axis=1)}
         newloads_df.update(pd.DataFrame(newloads_DDH))
    if carrier.EmptyDate  is not None:
        if carrier.originLat is not None and carrier.originLon is not None:
            newloads_puGap={'puGap': newloads_df.apply(lambda row: pu_Gap(pd.Timestamp(row.pu_appt), pd.Timestamp(carrier.EmptyDate),row.originDH/40.0),
                                    axis=1)}
        else:
            newloads_puGap = {'puGap': newloads_df.apply(
                lambda row: pu_Gap(pd.Timestamp(row.pu_appt), pd.Timestamp(carrier.EmptyDate), 0),
                axis=1)}
        newloads_df.update(pd.DataFrame(newloads_puGap))
    newloads_df['totalDH'] = newloads_df.apply(lambda row: row.originDH + row.destDH, axis=1)
    return newloads_df


def dynamic_input(newloads_df,carrier):
    ##This part is for new api input
    # newloads_df['originDH'] = originDH
    # newloads_df['destDH'] = destDH
    # newloads_df['puGap'] = gap
    # newloads_df['totalDH'] = originDH+destDH
    if  carrier.originLat is not None and carrier.originLon is not None:
         newloads_ODH= {'originDH': newloads_df.apply(lambda row: geopy.distance.vincenty((row.originLat, row.originLon), (
             float(carrier.originLat), float(carrier.originLon))).miles, axis=1)}

         newloads_df.update(pd.DataFrame(newloads_ODH))
    if  carrier.destLat is not None and carrier.destLon is not None:
         newloads_DDH= {'destDH': newloads_df.apply(lambda row: geopy.distance.vincenty((row.originLat, row.originLon), (
             float(carrier.destLat), float(carrier.destLon))).miles, axis=1)}
         newloads_df.update(pd.DataFrame(newloads_DDH))
    if carrier.EmptyDate  is not None:
        if carrier.originLat is not None and carrier.originLon is not None:
            newloads_puGap={'puGap': newloads_df.apply(lambda row: pu_Gap(pd.Timestamp(row.pu_appt), pd.Timestamp(carrier.EmptyDate),row.originDH/40.0),
                                    axis=1)}
        else:
            newloads_puGap = {'puGap': newloads_df.apply(
                lambda row: pu_Gap(pd.Timestamp(row.pu_appt), pd.Timestamp(carrier.EmptyDate), 0),
                axis=1)}
        newloads_df.update(pd.DataFrame(newloads_puGap))
    newloads_df['totalDH'] = newloads_df.apply(lambda row: row.originDH + row.destDH, axis=1)
    return newloads_df



def reasoning(results_df):
    reasons=[]
    reason_label=['close to origin','short total deadhead','good historical performance on similar loads','estimated margin', 'close to pickup time','desired OD']
    for load in results_df.itertuples():
        scores=[load.ODH_Score * 0.35, load.totalDH_Score * 0.20, load.hist_perf * 0.30,
                load.margin_Score* 0.10, load.puGap_Score* 0.05, load.desired_OD * 0.1]
        reasons.append ( reason_label[scores.index(max(scores))])
    return reasons

def api_json_output(results_df):
    results_df['Score'] = results_df['Score'].apply(np.int)
    api_resultes_df = results_df[['loadID', 'Reason', 'Score']]
    loads=[]
    #print (results_json)
##    results_df.to_csv(
##        'carrier' + str(carrierID) + '_load_recommender' + datetime.datetime.now().strftime(
##            "%Y%m%d-%H%M%S") + '.csv',
##        index=False,
##        columns=['carrierID', 'loadID', 'loaddate', 'origin', 'destination', 'originDH', 'destDH',
##                 'totalDH', 'margin_perc', 'estimated_margin', 'corrdor_margin_perc', 'estimated_margin%',
##                 'puGap', 'ODH_Score', 'totalDH_Score', 'puGap_Score',
##                 'margin_Score', 'hist_perf', 'Score', 'Reason'])
    # for i in api_resultes_df.index:
    #     load=api_resultes_df.loc[i]
    #     #api_resultes_df.loc[i].to_json("row{}.json".format(i))
    #     load_json=load.to_json()
    #     loads.append(load_json)

    for i in api_resultes_df.index:
        load = api_resultes_df.loc[i]

        _loadid = load["loadID"].item()
        _reason = load["Reason"]
        _score = load["Score"].item()
        loads.append({
            "loadid": _loadid,
            "Reason": _reason,
            "Score": _score
        })
    return loads

def read_corridor():
    cn = pyodbc.connect('DRIVER={SQL Server};SERVER=ANALYTICSDev;DATABASE=ResearchScience;trusted_connection=true')
    query="""select * from [ResearchScience].[dbo].[Recommendation_CorridorMargin]"""
    corridor_info=pd.read_sql(query,cn)
    return corridor_info

def recommender( carrier_load,trucks_df):
    t=TicToc()
    t.tic()
    originDH_default = 250  # get radius
    destDH_default = 300
    gap_default=48
    date1_default = now.strftime("%Y-%m-%d")
    date2_default = (datetime.timedelta(1) + now).strftime("%Y-%m-%d")

    ##initialization of the final results
    #results_sort_df = pd.DataFrame(columns=['loadID', 'Reason', 'Score'])
    result_json = {'Loads': []}

    carrier = trucks_df.iloc[0]
    newloadsall_df = Get_newload(date1_default,date2_default)
     ### should deal with if equipmenttype is a string carrier['EquipmentType'].fillna('', inplace=True)
    ###This part is for new api input

    # if any date will be put in, change the variables.
    # if date1 is not None and date2 is not None:
    #     Get_newload(date1,date2)
    # elif date2 is None:
    #     date2=date1 + 1
    #     Get_newload(date1,date2)
    # else:
    #     Get_newload()

    newloads_df = newloadsall_df[(newloadsall_df.value <= float(carrier.cargolimit))
                                 & [carrier.EquipmentType in equip for equip in newloadsall_df.equipment]
                                 & (newloadsall_df.equipmentlength <= float(carrier.EquipmentLength))]
    t.toc('newload')
    # newloads_df = newloadsall_df[
    #     (newloadsall_df.value <= carrier.cargolimit) & (newloadsall_df.equipment == carrier.EquipmentType)]
    t.tic()
    originRadius = originDH_default if carrier.originDeadHead_radius == 0 else float(carrier.originDeadHead_radius)
    destRadius = destDH_default if carrier.destinationDeadHead_radius == 0 else float(carrier.destinationDeadHead_radius)

    # initialize 3 column features. if carrier put any info related to DH or puGap,we can update
    newloads_df['originDH'] = originRadius
    newloads_df['destDH'] = destRadius
    newloads_df['puGap'] = gap_default
    newloads_df['totalDH'] = originRadius+destRadius

    # need dynamic check: if equipment type is an entry, etc.
    if len(newloads_df) > 0:
        newloads_df = dynamic_input(newloads_df, carrier)

        # need to change, if not null for origin, update origin; if not null for dest, update dest,
        # if not null for date, select date from to.
        #print(carrier.carrierID)

        newloads_select = newloads_df[
            (newloads_df.originDH <= originRadius) | (newloads_df.totalDH <= (originRadius+destRadius)) & (newloads_df.puGap <= gap_default)]
        t.toc('distance')
        if len(newloads_select) > 0:
            t.tic()
            carrier_load_score = check(carrier_load, newloads_select,carrier)
            t.toc('hist_scoring')
            t.tic()
            results_df = pd.DataFrame(carrier_load_score).merge(newloads_select, left_on="loadID", right_on="loadID",
                                                                how='inner')
            corridor_info = read_corridor()
            results_df = results_df.merge(corridor_info, left_on='corridor', right_on='corridor', how='left')
            results_df['corrdor_margin_perc'].fillna(0, inplace=True)
            t.toc('merge')
            # results_df.merge(newloads_df,left_on="loadID",right_on="loadID",how='inner')
            results_df['ODH_Score'] = score_DH(results_df['originDH'].tolist(), originDH_default)
            results_df['totalDH'] = results_df['originDH'] + results_df['destDH']
            results_df['totalDH_Score'] = score_DH(results_df['totalDH'].tolist(), (originDH_default + destDH_default))
            results_df['puGap_Score'] = score_DH(abs(results_df['puGap']).tolist(),gap_default )
            results_df['margin_Score'] = results_df['estimated_margin%'] * 0.3 + results_df['margin_perc'] * 0.7 \
                                         - results_df['corrdor_margin_perc']
            # margin score needs to be verified
            results_df['Score'] = results_df['ODH_Score'] * 0.25 + results_df['totalDH_Score'] * 0.20 + \
                                  results_df['hist_perf'] * 0.30  + results_df['margin_Score'] * 0.10 + \
                                  results_df['puGap_Score'] * 0.05 + results_df['desired_OD'] * 0.1
            results_df['Reason'] = reasoning(results_df)
            results_sort_df = results_df[results_df.Score > 0].sort_values(by=['Score'], ascending= False)
            t.tic()
            result_json=api_json_output(results_sort_df)
            t.toc('output')

    return result_json



@app.route('/search/',methods=['GET'])
def search():
    t=TicToc()
    t.tic()
    truck = {'carrierID':0,
                 'originLat': None,
                 'originLon': None,
                 'destLat': None,
                 'destLon': None,
                 'EmptyDate': now.strftime("%Y-%m-%d"),
                 'EquipmentType': '',
                 'EquipmentLength':53,
                 'cargolimit': 500000,
                 'originDeadHead_radius': 0,
                 'destinationDeadHead_radius': 0
                 }
    truck_input = request.args.to_dict()
    truck.update(truck_input)
    truck['cargolimit'] = Get_truckinsurance(truck['carrierID'])
    t.toc('truck')
    t.tic()
    carrier_load = Get_Carrier_histLoad(truck['carrierID'],(datetime.timedelta(-90-7) + now).strftime("%Y-%m-%d"),(datetime.timedelta(-7) + now).strftime("%Y-%m-%d"))
    carriers = []
    carriers.append(truck)
    carrier_df = pd.DataFrame(carriers)
    t.toc('histload')
    results=recommender(carrier_load, carrier_df)
 
    return jsonify({'Loads':results, "ver": "TruckNorris.0.1.18208.04"} )
 

if __name__=='__main__':
    app.run(debug = True)
