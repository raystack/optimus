#!/bin/sh

PROJECT=${PROJECT:-project-a}
HOST=${HOST:-localhost:9100}
SECRETS_PATH=$1

if [ -z "$SECRETS_PATH" ]; then
  >&2 echo "must provide secrets_path:  ./load_secret <secrets_path>"
  exit 1
fi

while read line || [[ -n $line ]]; do
  echo $line | awk '{sub(/=/," ")}1' | sed 's/\"//g;' | while read -r key value; do
    curl -XPOST -H "Content-Type: application/json" \
    "${HOST}/api/v1beta1/project/${PROJECT}/secret/${key}" \
    -d '{"value": "'${val}'"}'
  done
done < $SECRETS_PATH
