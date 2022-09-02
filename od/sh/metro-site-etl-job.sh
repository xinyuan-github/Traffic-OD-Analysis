#!/bin/bash

spark-submit --master spark://node01:7077 --class com.mm.etl.MetroSiteELTJob \
--conf "spark.sql.parquet.writeLegacyFormat=true" \
/opt/tools/jt.jar \
metro_station_tb metro_station_tb
