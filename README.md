# kafka

## management
카프카 운영과 관련된 파일들

### getRetention.py
특정 브로커가 가진 토픽 리스트와 retention 가져오기
#### list.yml 수정
```
peter-kafka01:                                          # cluster-name
  zookeeper: peter-zk001.foo.bar:2181/peter-kafka01     # zookeeper info
  brokers:
    - peter-kafka001.foo.bar                            # hostname
    - peter-kafka002.foo.bar                            # hostname
    - peter-kafka003.foo.bar                            # hostname
```
#### 실행과 결과 확인
```
# Run
$ python getRetetion.py
Input hostName: peter-kafka001.foo.bar     # hostname

# Check result
$ cat topicLists.txt
peter-topic01 0                            # topicname retention.ms(0 means default)
peter-topic02 86400000                     # topicname retention.ms
```
### getRetetionTopic.py
특정 토픽의 retention 확인
#### list.yml 수정
```
peter-kafka01:                                          # cluseter-name
  zookeeper: peter-zk001.foo.bar:2181/peter-kafka01     # zookeeper info
  brokers:
    - peter-kafka001.foo.bar                            # hostname
    - peter-kafka002.foo.bar                            # hostname
    - peter-kafka003.foo.bar                            # hostname
```
#### 실행과 결과 확인
```
# Run
$ python getRetetionTopic.py
Input clusterName: peter-kafka01           # cluster
Input topicName: peter-topic01             # topic
peter-topic01 0                            # topicname retention.ms(0 means default)
```