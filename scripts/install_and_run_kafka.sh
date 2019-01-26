#!/bin/bash

set -v #echo on
wget https://www.apache.org/dyn/closer.cgi?path=/kafka/2.1.0/kafka_2.11-2.1.0.tgz
tar -xzf kafka_2.11-2.1.0.tgz
cd kafka_2.11-2.1.0
bin/zookeeper-server-start.sh config/zookeeper.properties





set +v #echo off