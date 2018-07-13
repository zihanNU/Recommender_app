import csv
import numpy as np

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

class carrier_ode_loads_kpi_std:
    def  __init__(self,carrier,ode,loads,kpi,std):
        self.carrier=carrier
        self.ode=ode
        self.loads=loads
        self.kpi=kpi
        self.std=std



loadList=[]


with open('data.csv', newline='') as csv_file:
    reader = csv.reader(csv_file)
    next(reader)  # Skip the header.
    # Unpack the row directly in the head of the for loop.
    for Id,carrierId,KPIScore,originDH,originDHLevels,PUGap,originCluster,destinationCluster,equipment,corridorVolume,oriCount,destCount,customerCount,customerAll,customerSize in reader:
        # Convert the numbers to floats.
        Id = Id
        carrierId = carrierId
        KPIScore = int(KPIScore)
        originDH = originDH
        originDHLevels = originDHLevels
        PUGap = PUGap
        originCluster = originCluster
        destinationCluster = destinationCluster
        equipment = equipment
        corridorVolume = corridorVolume
        oriCount = oriCount
        destCount = destCount
        customerCount = customerCount
        customerAll = customerAll
        customerSize = customerSize
        # Now create the Student instance and append it to the list.
        loadList.append(Load(Id,carrierId,KPIScore,originDH,originDHLevels,PUGap,originCluster,destinationCluster,equipment,corridorVolume,oriCount,destCount,customerCount,customerAll,customerSize))
carriers=[]
for x in loadList:
  carriers.append(x.carrierId)
carriers=set(carriers)

ODEList=[]
for x in loadList:
    ODEList.append(originDestinationEquipment(x.originCluster,x.destinationCluster,x.equipment))

ODEList=set(ODEList)



def makeMatrix(x=[Load],y=[originDestinationEquipment],z=[]):
    kpiMatrix = []
    for i in z:
        for j in y:
            loads=[]
            std1=[]
            for k in x:
                if (k.carrierId == i and k.originCluster == j.origin and k.destinationCluster == j.destination):
                    loads.append(k)
                    std1.append(k.KPIScore)
            kpiMatrix.append(carrier_ode_loads_kpi_std(i,j,loads,np.mean(np.asarray(std1)),np.std(np.asarray(std1))))
    return kpiMatrix


kpiMatrix= makeMatrix(loadList,ODEList,carriers)



print(0)