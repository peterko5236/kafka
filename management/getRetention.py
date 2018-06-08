#!/usr/bin/env python
from kazoo.client import KazooClient
import sys
import json
import os.path
import os
import yaml

def getHealthCheckHost(zookeeperInfo, getHostName):
    zk = KazooClient(hosts=zookeeperInfo)
    zk.start()
    for child in zk.get_children('/brokers/ids'):
        brokerInfo = zk.get('/brokers/ids/%s' % child)
        if getHostName in brokerInfo[0]:
            getBrokerID = child
            break
    zk.stop()
    try:
        getBrokerID = int(getBrokerID)
        return getBrokerID
    except:
        print 'Not found broker in zookeeper'
        sys.exit()

def getTopicLists(zookeeperInfo, brokerID):
    zk = KazooClient(hosts=zookeeperInfo)
    zk.start()
    topicLists = []
    for child in zk.get_children('/brokers/topics'):
        if '__' not in child:
            partitionInfo = zk.get('/brokers/topics/%s' % child)
            jsonData = json.loads(partitionInfo[0])
            for partitionSeekFor in range(len(jsonData['partitions'])):
                getISR = jsonData['partitions']['%s'% partitionSeekFor]
                if brokerID in getISR:
                    topicLists.append(child)
                    break
    return topicLists
    zk.stop()

def getZookeeperURL(loadYaml, getHostName):
    for clusterNameFor in loadYaml:
        if getHostName in loadYaml[clusterNameFor]['brokers']:
            zookeeperURL = loadYaml[clusterNameFor]['zookeeper']
            return zookeeperURL

def getRetentionTime(zookeeperInfo, topicLists, topicListsFile):
    zk = KazooClient(hosts=zookeeperInfo)
    zk.start()
    for topicName in topicLists:
        topicConfigInfo = zk.get('/config/topics/%s' % topicName)
        jsonData = json.loads(topicConfigInfo[0])
        if bool(jsonData['config']):
            writeFile = '%s %s' % (topicName, jsonData['config']['retention.ms'])
            if not os.path.isfile(topicListsFile):
                open(topicListsFile, 'w').write(writeFile + '\n')
            else:
                open(topicListsFile, 'a+b').write(writeFile + '\n')
        else:
            writeFile = topicName + ' 0'
            if not os.path.isfile(topicListsFile):
                open(topicListsFile, 'w').write(writeFile + '\n')
            else:
                open(topicListsFile, 'a+b').write(writeFile + '\n')
    zk.stop()

def sortFile(beforeFile,afterFile):
    with open(beforeFile, 'r') as readFile:
        for line in sorted(readFile):
            openFile = open(afterFile, 'a+b')
            openFile.writelines(line)
        openFile.close()
    os.remove(beforeFile)

# ssh List files
topicListsFile = 'topicLists.txt'
tmptopicListsFile = 'tmptopicLists.txt'

# remove List file
if os.path.isfile(topicListsFile):
    os.remove(topicListsFile)

# read list.yml
with open("list.yml", 'r') as ymlfile:
    loadYaml = yaml.load(ymlfile)

getHostName = raw_input("Input hostName: ")

zookeeperURL = getZookeeperURL(loadYaml, getHostName)
if not zookeeperURL:
    print 'Not found zookeeper Info'
    sys.exit()

brokerID = getHealthCheckHost(zookeeperURL, getHostName)
topicLists = getTopicLists(zookeeperURL, brokerID)
getRetentionTime(zookeeperURL, topicLists, tmptopicListsFile)
sortFile(tmptopicListsFile, topicListsFile)