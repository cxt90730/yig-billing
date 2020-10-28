#!/bin/bash

path=$1
SPARKHOME=$2

exec $SPARKHOME/bin/spark-shell <<!EOF
spark.sql("use yig")
val df1 = sql("""SELECT bucketname, storageclass, sum(size) AS usage
    FROM objects
    WHERE storageclass=0
    GROUP BY bucketname,storageclass""")
val df2 = sql("""SELECT m.bucketname,m.storageclass,sum(mp.size) AS usage
    FROM multipartpart mp
    LEFT JOIN multiparts m ON m.bucketname = mp.bucketname AND m.uploadtime=mp.uploadtime
    WHERE storageclass=0
    GROUP BY m.bucketname,m.storageclass""")
val df3 = df1.unionAll(df2).distinct().groupBy("bucketname","storageclass").sum("usage")

val df4 = sql("""SELECT bucketname,storageclass,sum(floor((size + 65536 - 1) / 65536) * 65536) AS usage
    FROM objects
    WHERE storageclass IN (2, 3)
    GROUP BY bucketname,storageclass""")
val df5 = sql("""SELECT m.bucketname,m.storageclass,sum(floor((mp.size + 65536 - 1) / 65536) * 65536) AS usage
    FROM multipartpart mp
    LEFT JOIN multiparts m ON m.bucketname = mp.bucketname AND m.uploadtime=mp.uploadtime
    WHERE storageclass IN (2, 3)
    GROUP BY m.bucketname,m.storageclass""")
val df6 = df4.unionAll(df3).distinct().groupBy("bucketname","storageclass").sum("usage")
val df7 = df5.unionAll(df6).distinct().groupBy("bucketname","storageclass").sum("usage")

df7.coalesce(1).write.format("com.databricks.spark.csv").mode("overwrite").option("header", "false").save("$path")
!EOF

