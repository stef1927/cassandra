#!/bin/bash
for i in {1..50}
do
   echo "Iteration nr. $i"
   ./bin/cqlsh --debug --ssl --cqlshrc=/home/stefi/Documents/tests/security/cqlshrc -f kv.cql
done
