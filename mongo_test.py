import pymongo
import pprint
import pandas as pd

myClient =pymongo.MongoClient('mongodb://user:pass@stggxmongo03:27017/?authMechanism=PLAIN&readPreference=primary&authSource=$external')
D=myClient.list_database_names()
#db=myClient['InternalTrackingManager']
db=myClient['Coyote-Procurement-DarkCapacity']
col=db['loadSearch']

c=[]
for post in col.find():
    c.append(post)
    pprint.pprint(post)
newDf=pd.DataFrame( columns =[ "applicationType", "carrierId", "eliveryApptEnd","deliveryApptFromDate","deliveryApptStart"])


print ('5')

