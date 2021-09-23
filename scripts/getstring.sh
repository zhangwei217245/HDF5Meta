#!/bin/bash

cat rank0_out.txt | grep '\[STR\]' | while read line; do
    KEY=`echo $line | awk -F '|' '{print $2}'`
    VAL=`echo $line | awk -F '|' '{print $4}'`
    echo $VAL >> string/${KEY}.tmp
done

ls string/*.tmp | while read line;do
    KEY=`echo $line | sed  's/string\///g;s/\.tmp//g'`
    cat $line | sort | uniq -c | sort -nr -k1 | awk '{print $2"|"$1}' >> string/${KEY}.txt
done

rm -rf string/*.tmp