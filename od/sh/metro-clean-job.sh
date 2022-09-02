#!/bin/bash

spark-submit --master spark://node01:7077 --class com.mm.clean.MetroCleanJob /opt/tools/jt.jar \
s_ods_metro f_metro_cleaned
