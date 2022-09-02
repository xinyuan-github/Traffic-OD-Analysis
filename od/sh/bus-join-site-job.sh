#!/bin/bash
#--conf "spark.sql.parquet.writeLegacyFormat=true"让spark的Decimal用的parquet格式和hive相同

spark-submit --master spark://node01:7077 --class com.mm.combiner.BusJoinSiteJob \
--conf "spark.sql.parquet.writeLegacyFormat=true" \
/opt/tools/jt.jar \
f_bus_cleaned a_bus_withsite
