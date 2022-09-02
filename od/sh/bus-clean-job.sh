#!/bin/bash

spark-submit --master spark://node01:7077 --class com.mm.clean.BusCleanJob /opt/tools/jt.jar \
s_ods_bus f_bus_cleaned
