#!/bin/bash
for i in {1..100}
do
   echo "Iteration nr. $i"
   ./bin/cqlsh --ssl --cqlshrc=./conf/cqlshrc -f kv.cql
done
