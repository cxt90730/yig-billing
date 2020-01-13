#!/bin/bash

path=$1
SPARKHOME=$2

exec $SPARKHOME/bin/spark-shell <<!EOF
spark.sql("use yig")
val df1 = sql("select ownerid,storageclass,sum(size) as usage from objects where storageclass=0 group by ownerid,storageclass")
val df2 = sql("select m.ownerid,m.storageclass, sum(mp.size) as usage from multipartpart mp left join multiparts m on m.bucketname = mp.bucketname and m.uploadtime=mp.uploadtime group by m.ownerid,m.storageclass").where("storageclass=0")
spark.sql("use billing")
val df3 = sql("select ownerid,storageclass,sum(countsize) as usage from userbilling group by ownerid,storageclass")
val df4 = df1.unionAll(df2).distinct().groupBy("ownerid","storageclass").sum("usage")
val df = df3.unionAll(df4).distinct().groupBy("ownerid","storageclass").sum("usage")
df.coalesce(1).write.format("com.databricks.spark.csv").mode("overwrite").option("header", "false").save("$path")
!EOF

