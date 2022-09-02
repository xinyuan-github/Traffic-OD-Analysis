#!/bin/bash

spark-submit --master spark://node01:7077 --class com.mm.etl.BusSiteELTJob \
--conf "spark.sql.parquet.writeLegacyFormat=true" \
/opt/tools/jt.jar \
bus_station_tb bus_station_tb
