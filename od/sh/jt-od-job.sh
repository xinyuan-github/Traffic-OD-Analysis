#!/bin/bash

spark-submit --master spark://node01:7077 --class com.mm.od.JtODJob \
--conf "spark.sql.parquet.writeLegacyFormat=true" \
/opt/tools/jt.jar \
a_ggjt \
m_od_flow
