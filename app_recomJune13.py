from flask import Flask, jsonify, request
import pandas as pd
from scipy import spatial
import geopy.distance
import numpy as np
import math

from pytictoc import TicToc
from scipy import stats
import datetime

now = datetime.datetime.now()

app = Flask(__name__)

import traceback
import logging
import os
import config
from logging.config import dictConfig

import engines
import multiprocessing

dictConfig({
    'version': 1,
    'root': {
        'level': 'INFO'
    }
})
# setup logging
CONFIG = config.CONFIG
CURRENT_DATE = datetime.datetime.now().date()
LOGFILENAME = 'app_recommender_api_log_{}.log'.format(CURRENT_DATE)
if not os.path.exists(CONFIG.logPath):
    os.makedirs(CONFIG.logPath)
# initialize logging
LOGFILENAME = CONFIG.logPath + LOGFILENAME
logging.basicConfig(filename=LOGFILENAME, format='%(asctime)s  %(name)s:%(levelname)s:%(message)s')
LOGGER = logging.getLogger('app_recommender_api_log')
LOGGER.info("*** Flask restart t={} name={}".format(datetime.datetime.now(), __name__))

# setup data location
if not os.path.exists(CONFIG.carrierDataPath):
    os.makedirs(CONFIG.carrierDataPath)

# setup QUERY engine
QUERY = engines.QueryEngine(CONFIG.researchScienceConnString, CONFIG.bazookaAnalyticsConnString,
                            CONFIG.bazookaReplConnString)


class carrier_ode_loads_kpi_std:  # ode here is a pd.df with 4 features, o, d, corridor and equip.
    def __init__(self, carrier, ode, loads):  # ,kpi,std):
        self.carrier = carrier
        self.ode = ode
        self.loads = loads
    # self.kpi=kpi
    # self.std=std


def multi_makeMatrix(x, y, z=[]):
    """x is the hist load list, y is the unique ode list; x and y are pd.df structure
    this UNUSED function is defined for MULTIPLE carriers. if we have only 1, no need to set Z as a set"""
    kpiMatrix = []
    odlist = []
    for i in z:
        for j in y.itertuples():
            loads = []
            std1 = []
            selectedloads = x[(x['carrierID'] == i) & (x['corridor'] == j.corridor)]

            for k in selectedloads.itertuples():
                loads.append(k)
                std1.append(k.kpiScore)
            # for k in x.itertuples():
            # if (k.corridor == j.corridor):
            #  loads.append(k)
            #  std1.append(k.kpiScore)
            # no need to loop x for i times, as x is ordered by carrierid; needs to find the blocks for carrier[i]
            if (len(selectedloads) > 0):
                odlist.append(j.corridor)
                kpiMatrix.append(
                    carrier_ode_loads_kpi_std(i, j, loads))  # ,np.mean(np.asarray(std1)),np.std(np.asarray(std1))))
    return kpiMatrix, odlist


def makeMatrix(x, y, z):
    """x is the hist load list, y is the unique ode list; x and y are pd.df structure
    Args:
        x: the hist load list (pandas dataframe)
        y: the unique ode list (pandas dataframe)
        z: carrierID
    Notes:
        The for each element in the ode list, we seek loads and append to the first returned
        list the carrier_ode_loads_kpi_std class
    Returns:
        two lists
    """
    kpiMatrix = []
    odlist = []
    # for j in y.itertuples():
    #     loads=[]
    #     std1=[]
    #     selectedloads=x[(x['carrierID'] == z) & (x['corridor']==j.corridor)]   ### Check this capital or little c
    #     for k in  selectedloads.itertuples():
    #         loads.append(k)
    #         std1.append(k.kpiScore)
    #     if (len(selectedloads)>0):
    #         odlist.append(j.corridor)
    #         kpiMatrix.append(carrier_ode_loads_kpi_std(z,j,loads,np.mean(np.asarray(std1)),np.std(np.asarray(std1))))
    for j in y.itertuples():
        # loads=[]
        # std1=[]
        selectedloads = x[x['corridor'] == j.corridor]  ### Check this capital or little c
        kpiMatrix.append(carrier_ode_loads_kpi_std(z, j, selectedloads))
        odlist.append(j.corridor)

    return kpiMatrix, odlist


def get_odelist_hist(loadlist):
    # odelist = []
    # for x in loadlist.itertuples():
    #     # odelist.append({'origin':x.originCluster,'destination':x.destinationCluster,'corridor':x.corridor,'equipment':x.equipment,'corridor_count':x.corridor_count,'corridor_max':x.corridor_max,'origin_count':x.origin_count,'origin_max':x.origin_count,'dest_count':x.dest_count,'dest_max':x.dest_max
    #     #                 })
    #      odelist.append({'origin':x.originCluster,'destination':x.destinationCluster,'corridor':x.corridor,'equipment':x.equipment,'origin_count':x.origin_count,'origin_max':x.origin_max,'dest_count':x.dest_count,'dest_max':x.dest_max
    #                      })
    # odelist_df=pd.DataFrame(odelist)
    odelist_df = loadlist[
        ['originCluster', 'destinationCluster', 'corridor', 'equipment', 'origin_max_weight', 'dest_max_weight']]
    odelist_df.columns = ['origin', 'destination', 'corridor', 'equipment', 'origin_max_weight', 'dest_max_weight']
    odelist = odelist_df.drop_duplicates(subset=['origin', 'destination', 'equipment'])
    return odelist


def get_odelist_new(loadlist):
    """this function is developped to use in the future."""
    # odelist = []
    # for x in loadlist.itertuples():
    #     odelist.append({'origin':x.originCluster,'destination':x.destinationCluster,'corridor':x.corridor,'equipment':x.equipment})
    # odelist_df=pd.DataFrame(odelist)

    odelist_df = loadlist[['originCluster', 'destinationCluster', 'corridor', 'equipment']]
    # odelist_df=odelist_df.drop_duplicates(odelist_df)
    odelist_df.columns = ['origin', 'destination', 'corridor', 'equipment']
    return odelist_df


def find_ode(kpilist, load, odlist):
    """find_ode method

    Args:
        kpilist: the first result of the makeMatrix call
        load: this is a single newload from the QUERY.get_newload resultset
        odlist: the second result of the makeMatrix call

    Returns:
        matchlist: a df of matching loads
        perc: a percentage or confidence in the match?
        vol: the count of matching loads

    """
    matchlist = pd.DataFrame()
    perc = []
    if load.corridor in odlist:
        loc = odlist.index(load.corridor)
        x = kpilist[loc]
        weight = 1.0
        matchlist = x.loads
        perc = [weight] * len(matchlist)
    else:
        for x in kpilist:
            # if x.carrier not in carriers and (x.ode.origin == load.origin or x.ode.destination==load.destination) and x.ode.equipment ==load.equipment:
            # did not check equipment type, can be added
            # use max as the scaling can make the number to small; should use avg or medean instead of max,
            if x.ode.origin == load.originCluster or x.ode.destination == load.destinationCluster:
                matchlist = matchlist.append(x.loads)

                if x.ode.origin_max > 0:
                    origin_weight = x.ode.origin_max_weight
                else:
                    origin_weight = 0
                if x.ode.dest_max > 0:
                    dest_weight = x.ode.dest_max_weight
                else:
                    dest_weight = 0
                weight = max(origin_weight, dest_weight)
                perc = perc + [weight] * len(x.loads)
    vol = len(matchlist)
    return matchlist, perc, vol


def find_ode_noweight(kpilist, load, odlist):
    """weight from counts, used in similarity and histscoring"""
    matchlist = pd.DataFrame()
    vol = 0
    if load.corridor in odlist:
        loc = odlist.index(load.corridor)
        x = kpilist[loc]
        matchlist = x.loads
        vol = len(matchlist)
    else:
        for x in kpilist:
            # if x.carrier not in carriers and (x.ode.origin == load.origin or x.ode.destination==load.destination) and x.ode.equipment ==load.equipment:
            if x.ode.origin == load.origin or x.ode.destination == load.destination:
                matchlist = matchlist.append(x.loads)
    return matchlist, vol


def similarity(loadlist, newload, weight):
    carrier_scores = []
    miles_weight = 20
    od_dist_weight = 10
    for i in range(0, len(loadlist)):
        load = loadlist.iloc[i]
        ori_dist = geopy.distance.vincenty((newload.originLat, newload.originLon),
                                           (load.originLat, load.originLon)).miles / od_dist_weight
        destination_dist = geopy.distance.vincenty((newload.destinationLat, newload.destinationLon),
                                                   (load.destinationLat, load.destinationLon)).miles / od_dist_weight
        pu_appt_diff = (load.pu_appt_bucket - newload.pu_appt_bucket) * (load.pu_appt_type + newload.pu_appt_type) / 4
        do_appt_diff = (load.do_appt_bucket - newload.do_appt_bucket) * (load.do_appt_type + newload.do_appt_type) / 4
        histload_feature = [ori_dist, destination_dist, load.industryID, load.miles / miles_weight, pu_appt_diff,
                            do_appt_diff]
        newload_feature = [0.0001, 0.0001, newload.industryID, newload.miles / miles_weight, 0.0001, 0.0001]
        sim = 1 - spatial.distance.cosine(histload_feature, newload_feature)

        # other feature could be 'pu_GAP','DH' --- need to verify later

        carrier_scores.append(
            {'carrierID': load.carrierID, 'loadID': newload.loadID, 'similarity': sim,
             'kpi': load.kpiScore * load.kpiweight,
             'rpm': load.rpm, 'miles': load.miles, 'customer_rate': load.customer_rate, 'weight': weight[i],
             'margin_perc': load.margin_perc,
             # 'origin': newload.originCluster, 'dest': newload.destinationCluster, 'loaddate': newload.loaddate
             })
    carrier_scores_df = pd.DataFrame(carrier_scores)

    score_df = hist_scoring(carrier_scores_df, load.carrierID, newload.loadID)

    score_df['estimated_margin'] = newload.customer_rate - score_df['rpm'] * (newload.miles)
    score_df['estimated_margin%'] = score_df['estimated_margin'] / newload.customer_rate * 100
    return score_df


def hist_scoring(carrier_scores_df, carrierID, loadID):
    k = 0.3
    # we can choose different condition: maybe top 5, top 10%, sim> 0.8 etc.

    select_k = max(math.ceil(len(carrier_scores_df) * k), min(10, len(carrier_scores_df)))

    carrier_scores_select = carrier_scores_df.sort_values(by=['similarity', 'kpi'], ascending=False)[0:select_k]

    sim_score = sum(carrier_scores_select.kpi * carrier_scores_select.similarity * carrier_scores_select.weight) / len(
        carrier_scores_select)  # top n loads
    sim_margin = sum(
        carrier_scores_select.margin_perc * carrier_scores_select.similarity * carrier_scores_select.weight) / sum(
        carrier_scores_select.similarity * carrier_scores_select.weight)
    sim_rpm = sum(carrier_scores_select.rpm * carrier_scores_select.similarity * carrier_scores_select.weight) / sum(
        carrier_scores_select.similarity * carrier_scores_select.weight)
    # needs to be verified sim_rpm, can use surcharge %, needs to check the market cost at the booking time.. too complicated.

    # score = sim_score

    score_df = {'carrierID': carrierID, 'loadID': loadID,
                'hist_perf': sim_score, 'rpm': sim_rpm, 'margin_perc': sim_margin}

    return score_df


def check(carrier_load, newloads, carrier):
    if carrier_load['flag'] == 1:
        loadList = carrier_load['histload']
        # filepath = '{}carrier{}histload.csv'.format(CONFIG.carrierDataPath, carrier.carrierID)
        # loadList.to_csv(filepath,index=False)
        # no need to save the temp file any longer
        # loadList=  Carrier_Load_loading(1000)
        carrier_load_score = indiv_recommender_nomultiprocess(carrier, newloads, loadList)
    else:
        carrier_load_score = general_recommender(carrier, newloads)
        # corridor info is saved in the database now

    return (carrier_load_score)


def general_recommender(carrier, newloads):
    ##for new carriers, which has no hist data
    # margin and rpm and margin perc, needs to use all data from this corridor, no need to grab only from this carrier if this is a new carrier
    # carrier_load_score=[]
    carrierID = int(carrier.carrierID)
    corridor_info = QUERY.get_corridorinfo()

    # using merge instead of loop
    newloads_rate = pd.DataFrame(newloads).merge(corridor_info, left_on="corridor", right_on="corridor",
                                                 how='inner')
    carrier_load_score = newloads_rate[['loadID', 'rpm', 'corrdor_margin_perc', 'customer_rate', 'miles']]
    carrier_load_score['estimated_margin'] = carrier_load_score['customer_rate'] - carrier_load_score['rpm'] * \
                                             carrier_load_score['miles']
    carrier_load_score['estimated_margin%'] = carrier_load_score['corrdor_margin_perc']
    carrier_load_score['margin_perc'] = carrier_load_score['corrdor_margin_perc']
    carrier_load_score['carrierID'] = carrierID
    carrier_load_score['hist_perf'] = 0
    carrier_load_score['desired_OD'] = 0
    carrier_load_score = carrier_load_score.drop(columns=["corrdor_margin_perc", "customer_rate", "miles"])

    # for i in range(0, len(newloads)):
    #     newload = newloads.iloc[i]
    #     if (any(corridor_info.corridor==newload.corridor)):
    #         rpm= corridor_info[corridor_info.corridor==newload.corridor].rpm.values[0]
    #         estimate_margin_p = corridor_info[corridor_info.corridor == newload.corridor].corrdor_margin_perc.values[0]
    #     elif (any(corridor_info.OriginCluster==newload.originCluster)):
    #         rpm = pd.DataFrame.mean(corridor_info[corridor_info.OriginCluster == newload.originCluster].rpm)
    #         estimate_margin_p= pd.DataFrame.mean(corridor_info[corridor_info.OriginCluster == newload.originCluster].corrdor_margin_perc)
    #     else:
    #         rpm=pd.DataFrame.mean(corridor_info.rpm)
    #         estimate_margin_p = pd.DataFrame.mean(corridor_info.corrdor_margin_perc)
    #     score = {'carrierID': carrierID,
    #         'loadID': newload.loadID,
    #         # 'origin': newload.originCluster, 'destination': newload.destinationCluster,
    #         # 'loaddate': newload.loaddate,
    #             'hist_perf': 0, 'rpm': rpm,
    #             #'estimated_margin': newload.customer_rate - rpm * (newload.miles + newload.originDH),
    #             'estimated_margin': newload.customer_rate - rpm * (newload.miles),
    #             'estimated_margin%': estimate_margin_p,
    #             'margin_perc': estimate_margin_p,
    #             'desired_OD': 0
    #         }
    #     carrier_load_score.append(score)
    return (carrier_load_score)


def indiv_recommender_nomultiprocess(carrier, newloads, loadList):
    """once there is any historical information for given carrier, use historical info to calculate the scores(hist preference)"""

    carrierID = int(carrier.carrierID)
    # newload_ode = get_odelist_new(newloads)

    # carriers = sorted(set(loadList.carrierID.tolist()))
    histode = get_odelist_hist(loadList)
    # odelist = set(histode)   # set is not useful for the object list
    # odelist = histode.drop_duplicates(subset=['origin', 'destination', 'equipment'])

    # kpiMatrix,kpi_odlist = makeMatrix(loadList, odelist, carriers)
    kpiMatrix, kpi_odlist = makeMatrix(loadList, histode, carrierID)
    carrier_load_score = []

    # for i in range(0, len(newloads)):
    for newload in newloads.itertuples():
        #       newload=newloads.iloc[i]

        # new_ode=get_odelist_new(newload)
        # matchlist,   weight, corridor_vol = find_ode_noweight(kpiMatrix,newload,kpi_odlist )
        matchlist, weight, corridor_vol = find_ode(kpiMatrix, newload, kpi_odlist)
        if len(matchlist) > 0:
            score = similarity(matchlist, newload, weight)
            score['desired_OD'] = 100 if corridor_vol > min(len(loadList) * 0.1, 10) else 0
        else:
            score = {'carrierID': carrierID,
                     'loadID': int(newload.loadID),

                     'hist_perf': 0, 'rpm': pd.DataFrame.mean(loadList.rpm),
                     # 'estimated_margin': newload.customer_rate - pd.DataFrame.mean(loadList.rpm) * (newload.miles+newload.originDH),
                     'estimated_margin': newload.customer_rate - pd.DataFrame.mean(loadList.rpm) * (newload.miles),
                     'estimated_margin%': 100 - pd.DataFrame.mean(loadList.rpm) * (
                             newload.miles + newload.originDH) / newload.customer_rate * 100,
                     'margin_perc': pd.DataFrame.mean(loadList.margin_perc),
                     'desired_OD': 0}

        carrier_load_score.append(score)

    return (carrier_load_score)


def indiv_score(parameters, newloads):
    carrier_load_score = []
    kpiMatrix = parameters['kpiMatrix']
    kpi_odlist = parameters['kpi_odlist']
    loadList = parameters['loadList']
    carrierID = parameters['carrierID']
    for newload in newloads.itertuples():
        matchlist, weight, corridor_vol = find_ode(kpiMatrix, newload, kpi_odlist)
        if len(matchlist) > 0:
            score = similarity(matchlist, newload, weight)
            score['desired_OD'] = 100 if corridor_vol > min(len(loadList) * 0.1, 10) else 0
        else:
            score = {'carrierID': carrierID,
                     'loadID': int(newload.loadID),

                     'hist_perf': 0, 'rpm': pd.DataFrame.mean(loadList.rpm),
                     # 'estimated_margin': newload.customer_rate - pd.DataFrame.mean(loadList.rpm) * (newload.miles+newload.originDH),
                     'estimated_margin': newload.customer_rate - pd.DataFrame.mean(loadList.rpm) * (newload.miles),
                     'estimated_margin%': 100 - pd.DataFrame.mean(loadList.rpm) * (
                             newload.miles + newload.originDH) / newload.customer_rate * 100,
                     'margin_perc': pd.DataFrame.mean(loadList.margin_perc),
                     'desired_OD': 0}
        carrier_load_score.append(score)
    return (carrier_load_score)


def indiv_recommender_multiprocess(carrier, newloads, loadList):
    """once there is any historical information for given carrier, use historical info to calculate the scores(hist preference)"""

    carrierID = int(carrier.carrierID)
    # newload_ode = get_odelist_new(newloads)

    # carriers = sorted(set(loadList.carrierID.tolist()))
    histode = get_odelist_hist(loadList)
    # odelist = set(histode)   # set is not useful for the object list
    # odelist = histode.drop_duplicates(subset=['origin', 'destination', 'equipment'])

    # kpiMatrix,kpi_odlist = makeMatrix(loadList, odelist, carriers)
    kpiMatrix, kpi_odlist = makeMatrix(loadList, histode, carrierID)
    # carrier_load_score = []

    ##### multiprocessing

    num_cores = multiprocessing.cpu_count() - 1  # leave one free to not freeze machine
    num_partitions = num_cores  # number of partitions to split dataframe
    df_newloadsplit = np.array_split(newloads, num_partitions)
    pool = multiprocessing.Pool(num_cores)
    argms = {'carrerID': carrierID,
             'kpiMatrix': kpiMatrix,
             'kpi_odlist': kpi_odlist,
             'loadList': loadList}
    carrier_load_score = pd.concat(pool.map(indiv_score, argms, df_newloadsplit))
    pool.close()
    pool.join()

    return (carrier_load_score)


def score_deadhead(DH, radius):
    score = (radius - np.array(DH)) / radius * 100
    score_check = [min(max(0, a), 100) for a in score]
    return score_check


def pu_Gap(pu_appt, EmptyDate, traveltime):
    time_gap = pu_appt - EmptyDate
    return time_gap.days * 24 - traveltime + time_gap.seconds / 3600


def dynamic_input(newloads_df, carrier):
    ##This part is for new api input
    # newloads_df['originDH'] = originDH
    # newloads_df['destDH'] = destDH
    # newloads_df['puGap'] = gap
    # newloads_df['totalDH'] = originDH+destDH
    if carrier.originLat is not None and carrier.originLon is not None:
        carlat = float(carrier.originLat)
        carlon = float(carrier.originLon)
        newloads_ODH = {
            'originDH': newloads_df.apply(lambda row: geopy.distance.vincenty((row.originLat, row.originLon), (
                carlat, carlon)).miles, axis=1)}
        newloads_df.update(pd.DataFrame(newloads_ODH))

        # newloads_ODH= {'originDH': newloads_df.apply(lambda row: math.sqrt((row.originLat-carlat)**2+(row.originLon-carlon)**2)+69.1, axis=1)}
        # newloads_df.update(pd.DataFrame(newloads_ODH))

        # newloads_df['originDH'] = np.sqrt((newloads_df['originLat'] - carlat)**2 + (newloads_df['originLon'] - carlon)**2) * 69.1
        # print (newloads_ODH['originDH']-newloads_df['originDH'])
        # 69.1 is 1 degree distance in miles
    if carrier.destLat is not None and carrier.destLon is not None:
        carlat = float(carrier.destLat)
        carlon = float(carrier.destLon)
        newloads_DDH = {
            'destDH': newloads_df.apply(lambda row: geopy.distance.vincenty((row.destinationLat, row.destinationLon), (
                carlat, carlon)).miles, axis=1)}
        newloads_df.update(pd.DataFrame(newloads_DDH))
    if carrier.EmptyDate is not None:
        if carrier.originLat is not None and carrier.originLon is not None:

            newloads_df['puGap'] = (pd.to_datetime(carrier.EmptyDate) - pd.to_datetime(
                newloads_df["pu_appt"])) / np.timedelta64(3600, 's') - newloads_df["originDH"] / 40.0
            # newloads_puGap={'puGap': newloads_df.apply(lambda row: pu_Gap(pd.Timestamp(row.pu_appt), pd.Timestamp(carrier.EmptyDate),row.originDH/40.0),
            #                         axis=1)}
        else:
            newloads_df['puGap'] = (pd.to_datetime(carrier.EmptyDate) - pd.to_datetime(
                newloads_df["pu_appt"])) / np.timedelta64(3600, 's')
            # newloads_puGap = {'puGap': newloads_df.apply(
            #     lambda row: pu_Gap(pd.Timestamp(row.pu_appt), pd.Timestamp(carrier.EmptyDate), 0),
            #     axis=1)}
        # newloads_df.update(pd.DataFrame(newloads_puGap))

        # newloads_df['totalDH'] = newloads_df.apply(lambda row: row.originDH + row.destDH, axis=1)
    newloads_df['totalDH'] = newloads_df['originDH'] + newloads_df['destDH']
    return newloads_df


def reasoning(results_df):
    reasons = []
    reason_label = ['close to origin', 'short total deadhead', 'good historical performance on similar loads',
                    'estimated margin', 'close to pickup time', 'desired OD']
    for load in results_df.itertuples():
        scores = [load.ODH_Score * 0.35, load.totalDH_Score * 0.20, load.hist_perf * 0.30,
                  load.margin_Score * 0.10, load.puGap_Score * 0.05, load.desired_OD * 0.1]
        reasons.append(reason_label[scores.index(max(scores))])
    return reasons


def api_json_output(results_df):
    results_df['Score'] = results_df['Score'].apply(np.int)
    # api_resultes_df = results_df[['loadID', 'Reason', 'Score']]
    loads = []

    for i in results_df.index:
        load = results_df.loc[i]
        _loadid = load["loadID"].item()
        _reason = load["Reason"]
        _score = load["Score"].item()
        loads.append({
            "loadid": _loadid,
            "Reason": _reason,
            "Score": _score
        })
    return loads


def filter_newloads(carrier, newloads_df, carrier_load):
    """add a condition to filter the corridors that this carrier is interested in in the history
    will extend this ode with search history and DOT inspection data"""
    if carrier.originLat is None or carrier.originLon is None:
        trucks_corridor = QUERY.get_trucksearch(carrier.carrierID)
        newloads_df1 = []
        newloads_df2 = []
        if len(trucks_corridor) > 0:
            newloads_df1 = newloads_df[
                (newloads_df['originCluster'].isin(trucks_corridor.originCluster)) | (
                    newloads_df['destinationCluster'].isin(trucks_corridor.destinationCluster))]
        if carrier_load['flag'] == 1:
            # loadList_ode = carrier_load['histload']['corridor'].tolist()
            origins = carrier_load['histload']['originCluster'].tolist()
            dests = carrier_load['histload']['destinationCluster'].tolist()
            newloads_df2 = newloads_df[
                (newloads_df['originCluster'].isin(origins)) | (newloads_df['destinationCluster'].isin(dests))]
        if len(newloads_df1) > 0 and len(newloads_df2) > 0:
            newloads_df = pd.concat([newloads_df1, newloads_df2])
            newloads_df = newloads_df.drop_duplicates()
        elif len(newloads_df1) > 0:
            newloads_df = newloads_df1
            newloads_df = newloads_df.drop_duplicates()
        elif len(newloads_df2) > 0:
            newloads_df = newloads_df2
            newloads_df = newloads_df.drop_duplicates()

    return newloads_df


def recommender(carrier_load, trucks_df):
    """main recommendation function

    Args:
    carrier_load: Pandas DF with historical loads for the carrier.
    trucks_df: Pandas DF with truck(s) (orig,dest,ready date, etc.) sent to search()
    """

    originDH_default = 250  # get radius
    destDH_default = 300
    gap_default = 24
    # date1_default = now.strftime("%Y-%m-%d")
    # date2_default = (datetime.timedelta(1) + now).strftime("%Y-%m-%d")
    # corridor_info = pd.read_csv("corridor_margin.csv")  # should be saved somewhere

    ##initialization of the final results
    result_json = {'Loads': [], "ver": CONFIG.versionNumber}
    carrier = trucks_df.iloc[0]  # currently only support 1 truck
    t = TicToc()
    t.tic()
    newloads_df = QUERY.get_newload(carrier.originLat, carrier.originLon, carrier.cargolimit)
    t.toc()
    LOGGER.info("loading_newdata:" + str(t.elapsed))

    # newloads_df = newloadsall_df[(newloadsall_df.value <= float(carrier.cargolimit))
    #                            & [carrier.EquipmentType in equip for equip in newloadsall_df.equipment]
    #                            & (newloadsall_df.equipmentlength <= float(carrier.EquipmentLength))]
    t.tic()
    newloads_df = filter_newloads(carrier, newloads_df, carrier_load)

    # newloads_df = newloadsall_df[
    #     (newloadsall_df.value <= carrier.cargolimit) & (newloadsall_df.equipment == carrier.EquipmentType)]
    originRadius = originDH_default if carrier.originDeadHead_radius == 0 else float(carrier.originDeadHead_radius)
    destRadius = destDH_default if carrier.destinationDeadHead_radius == 0 else float(
        carrier.destinationDeadHead_radius)

    # initialize 3 column features. if carrier put any info related to DH or puGap,we can update
    newloads_df['originDH'] = originRadius
    newloads_df['destDH'] = destRadius
    newloads_df['puGap'] = gap_default
    newloads_df['totalDH'] = originRadius + destRadius
    t.toc()
    LOGGER.info("filter_newdata:" + str(t.elapsed))
    # need dynamic check: if equipment type is an entry, etc.
    if len(newloads_df) > 0:
        t.tic()
        # multi threading and multi processing can be applied
        newloads_df = dynamic_input(newloads_df, carrier)
        t.toc()
        LOGGER.info("realtime_deadhead:" + str(t.elapsed))
        # need to change, if not null for origin, update origin; if not null for dest, update dest,
        # if not null for date, select date from to.

        # multi threading and multi processing can be applied
        newloads_select = newloads_df[
            (newloads_df.originDH <= originRadius) | (newloads_df.totalDH <= (originRadius + destRadius)) & (
                    newloads_df.puGap <= gap_default)]

        if len(newloads_select) > 0:
            t.tic()
            # multi threading and multi processing can be applied

            carrier_load_score = check(carrier_load, newloads_select, carrier)  # ,corridor_info)
            t.toc()
            LOGGER.info("scoring:" + str(t.elapsed))
            results_df = pd.DataFrame(carrier_load_score).merge(newloads_select, left_on="loadID", right_on="loadID",
                                                                how='inner')

            corridor_info = QUERY.get_corridorinfo()
            results_df = results_df.merge(corridor_info, left_on=['corridor', 'equipment'],
                                          right_on=['corridor', 'equipment'], how='left')

            results_df['corrdor_margin_perc'].fillna(0, inplace=True)
            # results_df.merge(newloads_df,left_on="loadID",right_on="loadID",how='inner')
            results_df['ODH_Score'] = score_deadhead(results_df['originDH'].tolist(), originDH_default)
            results_df['totalDH'] = results_df['originDH'] + results_df['destDH']
            results_df['totalDH_Score'] = score_deadhead(results_df['totalDH'].tolist(),
                                                         (originDH_default + destDH_default))
            results_df['puGap_Score'] = score_deadhead(abs(results_df['puGap'] * (1 - results_df['prebook'])).tolist(),
                                                       gap_default)
            results_df['margin_Score'] = results_df['estimated_margin%'] * 0.3 + results_df['margin_perc'] * 0.7 \
                                         - results_df['corrdor_margin_perc']
            results_df['margin_Score'][results_df['hot'] == 1] = 100
            results_df['RPM_Score'] = score_deadhead(results_df['rpm'] - results_df['corridor_rpm'],
                                                     np.max(results_df['rpm'] - results_df['corridor_rpm']))
            results_df['margin_Score'][results_df['hot'] == 1] = 100
            results_df['puGap_Score'][results_df['puGap'] <= 2] = 100
            # margin score needs to be verified
            if now.hour < 12:
                results_df['Score'] = results_df['ODH_Score'] * 0.20 + results_df['totalDH_Score'] * 0.20 + \
                                      results_df['hist_perf'] * results_df['TMS_weight'] * 0.35 + \
                                      results_df['margin_Score'] * 0.10 + \
                                      results_df['puGap_Score'] * 0.05 + results_df['desired_OD'] * 0.1
            else:
                results_df['Score'] = results_df['ODH_Score'] * 0.30 + results_df['totalDH_Score'] * 0.20 + \
                                      results_df['hist_perf'] * results_df['TMS_weight'] * 0.25 + \
                                      results_df['margin_Score'] * 0.05 + \
                                      results_df['puGap_Score'] * 0.10 + results_df['desired_OD'] * 0.1
            results_df['Reason'] = reasoning(results_df)
            results_sort_df = results_df[results_df.Score > 0].sort_values(by=['Score'], ascending=False)

            result_json = api_json_output(results_sort_df[['loadID', 'Reason', 'Score']])

    return result_json


# import pyodbc
# def test_get_newload( carrierlat,carrierlon,cargolimit):
#         cn =  pyodbc.connect('DRIVER={SQL Server};SERVER=ANALYTICSDEV;DATABASE=Bazooka;trusted_connection=true')
#         if carrierlat is not None and carrierlon is not None:
#             sql="""declare @carrierlat as float =?
#             declare @carrierlon as float =?
#             declare @cargolimit as float =?
#             select * from
#             [ResearchScience].[dbo].[Recommendation_Newloads]
#             where
#             statetype=1
#             and originLat between @carrierlat-10 and @carrierlat+10
#             and originLon between  @carrierlon-5 and  @carrierlon+5
#             and value <= @cargolimit"""
#             newload=pd.read_sql(sql = sql, con = cn, params=(carrierlat,carrierlon,cargolimit,))
#         else:
#             query = """
#                     declare @cargolimit as float =?
#                           select * from
#                           [ResearchScience].[dbo].[Recommendation_Newloads]
#                          where
#                           statetype=1 and value <= @cargolimit
#                           """
#             newload = pd.read_sql(query, cn, params=(cargolimit,))

@app.route('/search/', methods=['GET'])
def search():
    LOGGER.info("GET /search/ called")

    try:
        truck = {'carrierID': 0,
                 'originLat': None,
                 'originLon': None,
                 'destLat': None,
                 'destLon': None,
                 'EmptyDate': now.strftime("%Y-%m-%d"),
                 'EquipmentType': '',
                 'EquipmentLength': 53,
                 'cargolimit': 500000,
                 'originDeadHead_radius': 0,
                 'destinationDeadHead_radius': 0
                 }

        truck_input = request.args.to_dict()
        truck.update(truck_input)
        carrierID = str(truck['carrierID'])

        if not carrierID.isdigit():
            raise ValueError("carrierID parameter must be assigned")
        t = TicToc()
        t.tic()
        truck['cargolimit'] = QUERY.get_truckinsurance_Zihan(carrierID)
        carrier_load = QUERY.get_carrier_histload(carrierID)
        t.toc()
        LOGGER.info("truckinsurance_carrierhist:" + str(t.elapsed))
        carriers = []
        t.tic()
        carriers.append(truck)
        carrier_df = pd.DataFrame(carriers)
        results = recommender(carrier_load, carrier_df)
        t.toc()
        LOGGER.info("totalrecommender:" + str(t.elapsed))
        return jsonify({'Loads': results, "ver": CONFIG.versionNumber})

    except Exception as ex:
        LOGGER.exception(ex)

        errormessage = {
            'Title': 'Something bad happened',
            'Message': traceback.format_exc()
        }
        return jsonify(errormessage), 500, {'Content-Type': 'application/json'}


if __name__ == '__main__':
    app.run(debug=True)
