#!/bin/sh

./optimus version &> /dev/null
STATUS=$?
if [ ! $STATUS -eq 0 ]; then
    echo "[smoke test] FAIL: optimus exited with code ${STATUS}";
    exit 1;
else
    echo "[smoke test] OK";
fi