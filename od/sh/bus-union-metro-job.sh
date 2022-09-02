#!/bin/bash

spark-submit --master spark://node01:7077 --class com.mm.combiner.BusUnionMetroJob \
--conf "spark.sql.parquet.writeLegacyFormat=true" \
/opt/tools/jt.jar \
a_bus_withsite \
a_metro_withsite \
a_ggjt
