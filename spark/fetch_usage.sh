#!/bin/bash

path=$1
SPARKHOME=$2

exec $SPARKHOME/bin/spark-shell <<!EOF
spark.sql("use yig")
val df1 = sql("select ownerid,storageclass,sum(size) as usage from objects where storageclass=0 group by ownerid,storageclass")
val df2 = sql("select m.ownerid,m.storageclass, sum(mp.size) from multipartpart mp left join multiparts m on m.bucketname = mp.bucketname and m.uploadtime=mp.uploadtime group by m.ownerid,m.storageclass").where("storageclass=0")
val df3 = df1.unionAll(df2).distinct().groupBy("ownerid","storageclass").sum("usage")
val df4 = sql("select ownerid,storageclass,sum(floor((size + 65536 - 1) / 65536) * 65536) as usage from objects where storageclass=3 group by ownerid,storageclass")
val df5 = sql("select m.ownerid,m.storageclass, sum(floor((mp.size + 65536 - 1) / 65536) * 65536) as usage from multipartpart mp left join multiparts m on m.bucketname = mp.bucketname and m.uploadtime=mp.uploadtime group by m.ownerid,m.storageclass").where("storageclass=3")
val df6 = df4.unionAll(df3).distinct().groupBy("ownerid","storageclass").sum("usage")
val df7 = df5.unionAll(df6).distinct().groupBy("ownerid","storageclass").sum("usage")
val df8 = sql("select ownerid,storageclass,sum(floor((size + 65536 - 1) / 65536) * 65536) as usage from objects where storageclass=2 group by ownerid,storageclass")
val df9 = sql("select m.ownerid,m.storageclass, sum(floor((mp.size + 65536 - 1) / 65536) * 65536) as usage from multipartpart mp left join multiparts m on m.bucketname = mp.bucketname and m.uploadtime=mp.uploadtime group by m.ownerid,m.storageclass").where("storageclass=2")
val df10 = df8.unionAll(df7).distinct().groupBy("ownerid","storageclass").sum("usage")
val df = df9.unionAll(df10).distinct().groupBy("ownerid","storageclass").sum("usage")
df.coalesce(1).write.format("com.databricks.spark.csv").mode("overwrite").option("header", "false").save("$path")
df.show
!EOF

