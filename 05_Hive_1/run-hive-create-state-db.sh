#!/bin/bash
for i in $( ls ../data/state-db/state-*.csv); do
    if ! $(hdfs dfs -test -d $i 2>/dev/null) ; then
        echo "Adding data $i to hdfs"
        hdfs dfs -put $i 2>/dev/null;
    fi
done

echo "Create database 'stata' in Hive"
hive -f hive-create-state-db.hql 2>/dev/null
