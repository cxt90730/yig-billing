#!/bin/bash

path=$1
SPARKHOME=$2

exec $SPARKHOME/bin/spark-shell <<!EOF
spark.sql("use yig")
val df1 = sql("select ownerid,storageclass,sum(size) as usage from objects group by ownerid,storageclass")
val df2 = sql("select m.ownerid,m.storageclass, sum(mp.size) from multipartpart mp left join multiparts m on m.bucketname = mp.bucketname and m.uploadtime=mp.uploadtime group by m.ownerid,m.storageclass")
val df = df1.unionAll(df2).distinct().groupBy("ownerid","storageclass").sum("usage")
df.coalesce(1).write.format("com.databricks.spark.csv").mode("overwrite").option("header", "false").save("$path")
!EOF