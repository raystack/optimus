#!/bin/sh

./optimus version -c ./optimus.sample.yaml &> /dev/null
STATUS=$?
if [ ! $STATUS -eq 0 ]; then
    echo "[smoke test] FAIL: optimus exited with code ${STATUS}";
    exit 1;
else
    echo "[smoke test] OK";
fi