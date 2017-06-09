#!/bin/bash
export METRON_HOME=/usr/metron/0.4.0
echo "truncate 'profiler'" | hbase shell
curl -XDELETE "http://localhost:9200/flows*" && curl -XDELETE "http://localhost:9200/dns*" && curl -XDELETE "http://localhost:9200/error*" && curl -XDELETE "http://localhost:9200/auth*"
sudo su - hdfs -c "hadoop fs -rm -skipTrash -r /apps/metron/indexing/indexed/dns"
sudo su - hdfs -c "hadoop fs -rm -skipTrash -r /apps/metron/indexing/indexed/flows"
sudo su - hdfs -c "hadoop fs -rm -skipTrash -r /apps/metron/indexing/indexed/error"
sudo su - hdfs -c "hadoop fs -rm -skipTrash -r /apps/metron/indexing/indexed/auth"
storm kill enrichment 
storm kill indexing 
storm kill auth 
storm kill profiler
$METRON_HOME/bin/start_enrichment_topology.sh && $METRON_HOME/bin/start_elasticsearch_topology.sh &&  $METRON_HOME/bin/start_parser_topology.sh -k node1:6667 -z node1:2181 -s auth && $METRON_HOME/bin/start_profiler_topology.sh
