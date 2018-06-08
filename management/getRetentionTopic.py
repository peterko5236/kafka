#!/usr/bin/env python
from kazoo.client import KazooClient
import sys
import json
import yaml

def getTopicLists(zookeeperInfo):
    zk = KazooClient(hosts=zookeeperInfo)
    zk.start()
    topicLists = []
    for child in zk.get_children('/brokers/topics'):
        if '__' not in child:
            topicLists.append(child)
    return topicLists
    zk.stop()

def getZookeeperURL(loadYaml, getClusterName):
    zookeeperURL = loadYaml[getClusterName]['zookeeper']
    return zookeeperURL

def getRetentionTime(zookeeperInfo, getTopicName):
    zk = KazooClient(hosts=zookeeperInfo)
    zk.start()
    topicConfigInfo = zk.get('/config/topics/%s' % getTopicName)
    jsonData = json.loads(topicConfigInfo[0])
    if bool(jsonData['config']):
        print 'topic: %s retention.ms: %s' % (getTopicName, jsonData['config']['retention.ms'])
    else:
        print 'topic: %s default' % getTopicName
    zk.stop()

# read list.yml
with open("list.yml", 'r') as ymlfile:
    loadYaml = yaml.load(ymlfile)

getClusterName = raw_input("Input clusterName: ")
getTopicName = raw_input("Input topicName: ")

try:
    loadYaml[getClusterName]
except:
    print 'Not found cluster in list'
    sys.exit()

zookeeperURL = getZookeeperURL(loadYaml, getClusterName)
topicLists = getTopicLists(zookeeperURL)
if getTopicName in topicLists:
    getRetentionTime(zookeeperURL, getTopicName)
else:
    print 'Not found %s in %s' % (getTopicName, getClusterName)