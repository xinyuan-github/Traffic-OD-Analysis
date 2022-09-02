#!/bin/bash

spark-submit --master spark://node01:7077 --class com.mm.combiner.MetroJoinSiteJob \
--conf "spark.sql.parquet.writeLegacyFormat=true" \
/opt/tools/jt.jar \
f_metro_cleaned a_metro_withsite
