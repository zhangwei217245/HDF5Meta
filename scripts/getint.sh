#!/bin/bash

cat rank0_out.txt | grep '\[INT\]' | while read line; do
    KEY=`echo $line | awk -F '|' '{print $2}'`
    VAL=`echo $line | awk -F '|' '{print $4}'`
    echo $VAL >> int/${KEY}.tmp
done

ls int/*.tmp | while read line;do
    KEY=`echo $line | sed  's/int\///g;s/\.tmp//g'`
    cat $line | sort | uniq -c | sort -nr -k1 | awk '{print $2"|"$1}' >> int/${KEY}.txt
done

rm -rf int/*.tmp