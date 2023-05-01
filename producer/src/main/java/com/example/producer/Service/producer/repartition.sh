#!/bin/sh

echo "Script Execution cmd - $0 $@"

while getopts ":h:t:b:r:p:" option; do
  case $option in
    h) echo "usage: $0 [-a] [-b] [-c] ..."; exit ;;
    t) TopicName=$OPTARG ;;
    b) BrokerList=$OPTARG ;;
    r) RepartitionType=$OPTARG ;;
    p) PartitionVERGE=$OPTARG ;;
    ?) echo "error: option $OPTARG is not implemented"; exit ;;
  esac
done

if [ -z "$PartitionVERGE" ]
then
      PartitionVERGE=1000
fi
[[ -z "`cat /etc/kafka/kafka_server_jaas.conf | grep -v ^$`" ]] || export KAFKA_OPTS="-Djava.security.auth.login.config=/etc/kafka/kafka_server_jaas.conf"

THROTTLE=""
KAFKADIR="/usr/local/kafka/bin"
TOPICDIR="/tmp/topics_dir"
PUBSUBFILE="/etc/kafka/pubsub.properties"
ZKURL=`grep 'zookeeper.connect=' $PUBSUBFILE | awk -F"=" '{print $2}'`
LISTNER=`grep ^listeners $PUBSUBFILE | awk -F"=" '{print $2}'`
host=`hostname -f`
VERGE="1000000000"
LIST=0
TOTAL_SIZE=0
TOPIC_SIZE=0
CONFIGFILE="/tmp/command.confg"
[ -n "$KAFKADIR" ] || exit 1;
[ -n "$TOPICDIR" ] || exit 1;
[ -n "$ZKURL" ] || exit 1;
[ -n "$PUBSUBFILE" ] || exit 1;
[ -n "$LISTNER" ] || exit 1;

KAFKAVERSION=`$KAFKADIR/kafka-topics.sh --version 2>/dev/null | awk '{print $1}'`

if [ "$KAFKAVERSION" == "2.8.1" ]
then
  KAFKACONNECTION="--bootstrap-server $LISTNER"
else
  KAFKACONNECTION="--zookeeper $ZKURL"
fi

echo $LISTNER | grep -q SSL
ISSSL=$?

if [ $ISSSL -eq 0 ]
then
  echo "SSL Cluster"
  COMMANDCONFIG="--command-config $CONFIGFILE"
else
  echo "NON-SSL Cluster"
  COMMANDCONFIG=""
fi

grep ^ssl $PUBSUBFILE | egrep "key|keystore|truststore" > $CONFIGFILE
echo "security.protocol=SSL" >> $CONFIGFILE

echo " Creating folder $TOPICDIR and removing files $TOPICDIR"
mkdir -p $TOPICDIR
rm -rf $TOPICDIR/*

echo "KAFKADIR -> $KAFKADIR"
echo "TOPICDIR -> $TOPICDIR"
echo "PUBSUBFILE -> $PUBSUBFILE"
echo "ZKURL -> $ZKURL"
echo "LISTNER -> $LISTNER"
echo "KAFKAVERSION -> $KAFKAVERSION"
echo "KAFKACONNECTION -> $KAFKACONNECTION"
echo "COMMANDCONFIG -> $COMMANDCONFIG"
echo "THROTTLE -> $THROTTLE"
echo "PartitionVERGE -> $PartitionVERGE"
###################Check if any partition reassignment is running##########################################################
counter=0
while [ $counter -lt 1 ]
do
  sleep 1
  ${KAFKADIR}/zookeeper-shell.sh ${ZKURL} ls /admin/reassign_partitions 2>&1 | grep "Node does not exist" || { echo "Topic repartitioning is already running"; exit 1; }
  ((counter=counter+1))
done


####################################Get Broker ids from hostname##########################################################

brokerids=""
if [ "$BrokerList" != "" ]
then
   for i in $(echo $BrokerList | sed "s/,/ /g")
   do
     brokerid=`$KAFKADIR/zookeeper-shell.sh $ZKURL get /host_brokerid_mappings/$i 2>/dev/null | grep "^[0-9]"`
     echo "brokerid list -> ${brokerid}"
     if [ -z ${brokerids} ]
     then
      brokerids="$brokerid"
     else
       brokerids="$brokerids,$brokerid"
     fi
   done
   echo "brokerid list ->  ${brokerids}"
else
   brokerids=`$KAFKADIR/zookeeper-shell.sh $ZKURL ls /brokers/ids | grep "]" | sed -e 's/\[//g' -e 's/]//g' -e 's/ //g'`
   echo "brokerid list -> ${brokerids}"
   [ -n "$brokerids" ] || exit 1;
fi

broker_count=`echo $brokerids | tr ',' '\n' | egrep -v "^$| " | sort -u | wc -l`
echo "Broker Count -> $broker_count"

if [[ $brokerids =~ ',,' || $broker_count -lt 3 ]]
then
  echo "Error: Incoreect Broker Ids listed. Please recheck."
  exit 102
fi

#####################################Get Topic List###########################################################################
if [ "$TopicName" == "ALL" ]
then
  $KAFKADIR/kafka-topics.sh $KAFKACONNECTION $COMMANDCONFIG --list > /tmp/topic_list.out
elif [[ "$1" =~ "," ]]
then
  echo $1 | tr "," "\n" > /tmp/topic_list.out
else
  echo $1 > /tmp/topic_list.out
fi

topic_deletion_check=`cat /tmp/topic_list.out | grep "marked for deletion" | wc -l`
if [[ $topic_deletion_check -gt 0 ]]
then
  echo "Error: Some topics are marked for deletion. Please correct the same and rerun the partiton reassignment"
  exit 101
fi

topic_count=`wc -l /tmp/topic_list.out| awk '{print $1}'`
echo "Topic Count -> ${topic_count}"


#####################################Parallel topic repartition###########################################################################
echo "function to get topic and partition size"
# shellcheck disable=SC2112
function topicpartitionsize() {
echo "Getting topic annd partition size"
rm -rf /tmp/alltopic_details.txt
for topic in `cat /tmp/topic_list.out`
do
topic_size_partition="`$KAFKADIR/kafka-log-dirs.sh --bootstrap-server $LISTNER $COMMANDCONFIG --topic-list $topic --describe`"
topic_size="`echo $topic_size_partition | grep -oP '(?<=size":)\d+' | awk '{ sum += $1 } END { print sum }'`"
partition_count="`echo $topic_size_partition | grep -oP '(?<=size":)\d+' | wc -l`"
echo "$topic|$topic_size|$partition_count" >> /tmp/alltopic_details.txt
done
sort -t "|" -k2n -k3n /tmp/alltopic_details.txt > /tmp/alltopicsorted.txt
}
echo "function to create parallel topic repartition json"
# shellcheck disable=SC2112
function parallelltopicrepartitionjson() {
for i in `cat /tmp/alltopicsorted.txt`
do
TOPIC=`echo $i | cut -d"|" -f1`
TOPIC_SIZE=`echo $i | cut -d"|" -f2`
PARTITION_SIZE=`echo $i | cut -d"|" -f3`
((TOTAL_SIZE=TOTAL_SIZE+TOPIC_SIZE))
((PARTITION_TOTAL_SIZE=PARTITION_TOTAL_SIZE+PARTITION_SIZE))
if [ "$TOTAL_SIZE" -gt "$VERGE" ] || [ "$PARTITION_TOTAL_SIZE" -gt "$PartitionVERGE" ]
then
  echo "listing done based on topic size or partition count"
  if [ ${TOTAL_SIZE} -gt ${VERGE} ]
  then
    ((LIST=LIST+1))
    echo "Creating list of files based on topic size group -  TOPIC_LIST_$LIST"
    TOTAL_SIZE=TOPIC_SIZE
  else
    ((LIST=LIST+1))
    echo "Creating list of files based on partition count -  TOPIC_LIST_$LIST"
    PARTITION_TOTAL_SIZE=PARTITION_SIZE
  fi
fi
echo $TOPIC $TOPIC_SIZE $PARTITION_SIZE >> $TOPICDIR/topic_list_$LIST
done
for file in "$TOPICDIR"/topic_list_*
do
for topic in `cat ${file}`
do
                  echo $topic | awk '
                  BEGIN { print "{\"version\":1,\n \"topics\": ["}
                  { printf "{ \"topic\": \"%s\"}",$1
                  if (NR != tc) {printf ","}
                  }
                  END { print "\n]}"}
                  ' ${file} | sed 'x;${s/,$//;p;x;};1d' > ${file}.json
done
done
}
  echo "NonParallel topic repartition"
  topic_failure_count=0
  for topic in `cat /tmp/topic_list.out`
  do
    echo $topic | awk '
                  BEGIN { print "{\"version\":1,\n \"topics\": ["}
                  { printf "{ \"topic\": \"%s\"}",$1
                  }
                  END { print "\n]}"}
    ' > $TOPICDIR/${topic}.json
done
function nonparallelltopicrepartitionjson() {
  echo "NonParallel topic repartition"
  topic_failure_count=0
  for topic in `cat /tmp/topic_list.out`
  do
    echo $topic | awk '
                  BEGIN { print "{\"version\":1,\n \"topics\": ["}
                  { printf "{ \"topic\": \"%s\"}",$1
                  }
                  END { print "\n]}"}
    ' > $TOPICDIR/${topic}.json
done
}
function runrepartition() {
for file in "$TOPICDIR"/*.json
do
$KAFKADIR/kafka-reassign-partitions.sh $KAFKACONNECTION $COMMANDCONFIG --generate --topics-to-move-json-file ${file} --broker-list $brokerids > ${file%.json}-reassign-tmp.json
topic_error_check=`cat ${file%.json}-reassign-tmp.json | grep "Error:" |  wc -l`
                  if [[ $topic_error_check -gt 0 ]]
                  then
                    echo "Error: Generate json failed for ${file}. Please check for the failure and correct the issue manually"
                    exit 103
                  fi
                  cat ${file%.json}-reassign-tmp.json | grep "{\"version\":1" | tr '\n' ' ' | awk '{print $2}' > ${file%.json}-reassign.json
                  [ -s ${file%.json}-reassign.json ] || { echo "Error: ${file%.json}-reassign.json file is empty."; exit 105; }

                  cat ${file%.json}-reassign-tmp.json | grep "{\"version\":1" | tr '\n' ' ' | awk '{print $1}' > ${file%.json}-reassign.org.json
                  [ -s ${file%.json}-reassign.org.json ] || { echo "Error: ${file%.json}-reassign.org.json file is empty."; exit 105; }

                  echo "Run cmd to get topic in orginal partition distribution state -> $KAFKADIR/kafka-reassign-partitions.sh $KAFKACONNECTION $COMMANDCONFIG --execute --reassignment-json-file ${file%.json}-reassign.org.json"
                  sleep 5
                  $KAFKADIR/kafka-reassign-partitions.sh $KAFKACONNECTION $COMMANDCONFIG --execute --reassignment-json-file ${file%.json}-reassign.json $THROTTLE > ${file%.json}-reassign.log 2>&1
                  reassign_status=1
                  total_jobs=`$KAFKADIR/kafka-reassign-partitions.sh $KAFKACONNECTION $COMMANDCONFIG --verify --reassignment-json-file ${file%.json}-reassign.json | grep -v "partition reassignment" | grep -v ^$ | wc -l`
                  echo "################ `date`: Starting re-partitioning for $file ################"
                  while [ $reassign_status -ne 0 ]
                  do
                    $KAFKADIR/kafka-reassign-partitions.sh $KAFKACONNECTION $COMMANDCONFIG --verify --reassignment-json-file ${file%.json}-reassign.json > ${file%.json}-verify.log 2>&1
                    reassign_status=`grep "in progress" ${file%.json}-verify.log | wc -l`
                    failed_status=`egrep " failed |Error:" ${file%.json}-verify.log | wc -l`
                    ((completed_status=total_jobs-reassign_status-failed_status))
                    echo "reassignment status $file : completed - ${completed_status}, in-progress ${reassign_status}, failed ${failed_status}"
                    sleep 10
                  done
  echo "################ `date`: re-partitioning successfully completed or failed for $file ################"
  if [ $failed_status -gt 0 ]
  then
    ((topic_failure_count=topic_failure_count+1))
  fi
done

error_check=`cat $TOPICDIR/*.log | grep "Error:" | wc -l`
echo "Error Check -> $error_check"

if [[ $topic_failure_count -gt 0 || $error_check -gt 0 ]]
then
   echo "Re-partitioning failed. Please check for the failed topics and correct the issue manually"
  exit 100
fi
}
if [ "$RepartitionType" == "Parallel" ]
then
echo "calling parallel repartition"
topicpartitionsize
parallelltopicrepartitionjson
else
echo "calling nonparallel repartition"
nonparallelltopicrepartitionjson
fi
runrepartition