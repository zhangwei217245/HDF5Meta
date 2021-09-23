#!/bin/bash

cat rank0_out.txt | grep '\[FLT\]' | while read line; do
    KEY=`echo $line | awk -F '|' '{print $2}'`
    VAL=`echo $line | awk -F '|' '{print $4}'`
    echo $VAL >> float/${KEY}.tmp
done

ls float/*.tmp | while read line;do
    KEY=`echo $line | sed  's/float\///g;s/\.tmp//g'`
    cat $line | sort | uniq -c | sort -nr -k1 | awk '{print $2"|"$1}' >> float/${KEY}.txt
done

rm -rf float/*.tmp