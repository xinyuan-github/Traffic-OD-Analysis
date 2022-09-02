#!/bin/bash

spark-submit --master spark://node01:7077 --class com.mm.etl.MetroEtlJob /opt/tools/jt.jar \
/data/spark/jt/T08_TRADE_IC_DETAIL_gdjt20000.dat \
s_ods_metro
