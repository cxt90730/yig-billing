#!/bin/bash

path=$1
SPARKHOME=$2

exec $SPARKHOME/bin/spark-shell <<!EOF
spark.sql("use yig")

val df1 = sql("""SELECT ownerid, storageclass, sum(size) AS usage
    FROM objects
    LEFT JOIN buckets ON buckets.bucketname = objects.bucketname
    WHERE buckets.acl = '{"CannedAcl":"ecs-managed"}' AND storageclass=0
    GROUP BY ownerid, storageclass""")
val df2 = sql("""SELECT m.ownerid, m.storageclass, sum(mp.size) AS usage
    FROM multipartpart mp
    LEFT JOIN multiparts m ON m.bucketname = mp.bucketname AND m.uploadtime=mp.uploadtime
    LEFT JOIN buckets ON buckets.bucketname = m.bucketname
    WHERE buckets.acl = '{"CannedAcl":"ecs-managed"}' AND storageclass=0
    GROUP BY m.ownerid,m.storageclass""")
val df3 = df1.unionAll(df2).distinct().groupBy("ownerid","storageclass").sum("usage")

df3.coalesce(1).write.format("com.databricks.spark.csv").mode("overwrite").option("header", "false").save("$path")
!EOF
