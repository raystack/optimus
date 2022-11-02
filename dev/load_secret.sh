#!/bin/sh

PROJECT=${PROJECT:-project-a}
SECRET_NAME=${SECRET_NAME:-BQ_SERVICE_ACCOUNT}
HOST=${HOST:-localhost:9100}
SECRET_PATH=$1

if [ -z "$SECRET_PATH" ]; then
  >&2 echo "must provide secret_path:  ./load_secret <secret_path>"
  exit 1
fi

ENDPOINT="${HOST}/api/v1beta1/project/${PROJECT}/secret/${SECRET_NAME}"
SECRET_VALUE=$(cat ${SECRET_PATH} | base64)

curl -XPOST -H "Content-Type: application/json" \
$ENDPOINT -d '{"value": "'${SECRET_VALUE}'"}'