#!/bin/sh

OPTIMUS_NAMESPACE=optimus-dev

PROJECT=${PROJECT:-project-a}
HOST=${HOST:-"localhost:9100"}
SETUP_PATH=$1

is_yq_installed(){
  if [ -z $(command -v yq) ]; then
    >&2 echo "yq must be installed: \`brew install yq\`"
    exit 1
  fi
}

if [ -z "$SETUP_PATH" ]; then
  >&2 echo "must provide setup_path: ./load_secrets.sh <setup_path>"
  exit 1
fi

# load secrets
echo ">> load secrets into project ${PROJECT}"
is_yq_installed
if ! curl --output /dev/null --silent ${HOST}/ping; then
  >&2 echo "can't connect to optimus host ${HOST}"
  exit 1
fi

yq '.secrets[] | (.name | . + " ") + (.value | @base64)' ${SETUP_PATH} >> s.tmp
while read key value; do
    curl -XPOST -H "Content-Type: application/json" \
    "${HOST}/api/v1beta1/project/${PROJECT}/secret/${key}" \
    -d '{"value": "'${value}'"}' -s -o /dev/null -w "set secret $key: %{http_code}\n"
done < s.tmp && rm s.tmp
