import pymongo
import pyodbc
import pprint
import pandas as pd



def GetCarrierID():
    cn = pyodbc.connect('DRIVER={SQL Server};SERVER=ANALYTICSDev;DATABASE=ResearchScience;trusted_connection=true')
    query= """
        select carrierID 
        FROM [ResearchScience].[dbo].[Recommendation_ActiveCarriers]
	"""
    carriers=pd.read_sql(query,cn)
    return carriers


def GetCity():
    cn = pyodbc.connect('DRIVER={SQL Server};SERVER=ANALYTICSDev;DATABASE=ResearchScience;trusted_connection=true')
    query="""
        select
        C.name 'CityName', C.statecode, 
        C.name + ', '+ C.StateCode 'City-State',
        C.name + ', '+ C.StateCode+', '+'US' 'City-State-County',
        S.Name +', '+'USA' 'State-Country',
         RCO.ClusterName,
         Rco.latitude,
         Rco.longitude
        
        from bazooka.dbo.City C
        left join bazooka.dbo.State S on S.id=C.StateID
        LEFT JOIN Analytics.CTM.RateClusters RCO ON RCO.BazookaCityId =  C.ID
         where C.stateID<=51
 
 
 """
    city=pd.read_sql(query,cn)
    return city

def GetCarrierID1():
    cn = pyodbc.connect('DRIVER={SQL Server};SERVER=ANALYTICSDev;DATABASE=ResearchScience;trusted_connection=true')
    query= """
        select carrierID 
        FROM [ResearchScience].[dbo].[Recommendation_ALLCarriers]
	"""
    carriers=pd.read_sql(query,cn)
    return carriers


carriers=GetCarrierID()
cities=GetCity()

myClient =pymongo.MongoClient('mongodb://user:pass@stggxmongo03:27017/?authMechanism=PLAIN&readPreference=primary&authSource=$external')
D=myClient.list_database_names()
#db=myClient['InternalTrackingManager']
db=myClient['Coyote-Procurement-DarkCapacity']
col=db['loadSearch']




c=[]
for post in col.find():
    if post['carrierId'] in carriers['carrierID'].tolist() and (len(post['origin'])>0 or len(post['destination'])>0):
        carrier_search={'carrierID': post['carrierId'], 'origin':post['origin'],'destination':post['destination'],
                        'searchDateTimeUtc':post['searchDateTimeUtc'], 'pickupFromDate':post['pickupFromDate'],
                         'pickupToDate':post['pickupToDate']
        }
        if len(post['origin'])>0 and len(post['destination'])>0:
            for i in range(0,len(post['origin'])):
                for j in range(0,len(post['destination'])):
                    carrier_search = {'carrierID': post['carrierId'], 'origin': post['origin'][i],
                              'destination': post['destination'][j],
                              'searchDateTimeUtc': post['searchDateTimeUtc'], 'pickupFromDate': post['pickupFromDate'],
                              'pickupToDate': post['pickupToDate']
                              }
                    c.append(carrier_search)
        elif len(post['origin'])>0 and len(post['destination'])==0:
            for i in range(0,len(post['origin'])):
                carrier_search = {'carrierID': post['carrierId'], 'origin': post['origin'][i],
                          'destination': '',
                          'searchDateTimeUtc': post['searchDateTimeUtc'], 'pickupFromDate': post['pickupFromDate'],
                          'pickupToDate': post['pickupToDate']
                          }
                c.append(carrier_search)
        elif len(post['origin'])==0 and len(post['destination'])>0:
            for i in range(0,len(post['destination'])):
                carrier_search = {'carrierID': post['carrierId'], 'origin':'',
                          'destination': post['destination'][i],
                          'searchDateTimeUtc': post['searchDateTimeUtc'], 'pickupFromDate': post['pickupFromDate'],
                          'pickupToDate': post['pickupToDate']
                          }
                c.append(carrier_search)
        #pprint.pprint(post )


#newDf=pd.DataFrame( columns =[ "applicationType", "carrierId","origin","destination","searchDateTimeUtc","deliveryApptEnd","deliveryApptFromDate","deliveryApptToDate","pickupFromDate","pickupToDate"])
newDf=pd.DataFrame(c)
newDf=pd.DataFrame(newDf).merge(cities[['City-State','ClusterName']], left_on="origin", right_on="City-State",
                                                    how='left')
newDf=pd.DataFrame(newDf).merge(cities[['City-State','ClusterName']], left_on="destination", right_on="City-State",
                                                    how='left')
newDf=pd.DataFrame(newDf).merge(cities[['City-State-County','ClusterName']], left_on="destination", right_on="City-State-County",
                                                    how='left')
newDf=pd.DataFrame(newDf).merge(cities[['City-State-County','ClusterName']], left_on="destination", right_on="City-State-County",
                                                    how='left')
newDf=pd.DataFrame(newDf).merge(cities[['State-Country','ClusterName']], left_on="origin", right_on="State-Country",
                                                    how='right')
newDf=pd.DataFrame(newDf).merge(cities[['State-Country','ClusterName']], left_on="origin", right_on="State-Country",
                                                    how='right')
newDf=pd.DataFrame(col)["applicationType", "carrierId","origin","destination","searchDateTimeUtc","deliveryApptEnd","deliveryApptFromDate","deliveryApptToDate","pickupFromDate","pickupToDate"]


print ('5')

