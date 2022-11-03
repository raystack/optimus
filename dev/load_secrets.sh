#!/bin/sh

OPTIMUS_NAMESPACE=${NAMESPACE:-optimus-dev}
PROJECT=${PROJECT:-project-a}
HOST=${HOST:-"localhost:9100"}
SETUP_PATH=$1

if [ -z "$SETUP_PATH" ]; then
  >&2 echo "must provide setup_path: ./load_secrets <secrets_path>"
  exit 1
fi

# load secret

echo "load secrets into project ${PROJECT}"
if [ -z $(command -v yq) ]; then
  >&2 echo "yq must be installed: \`brew install yq\`"
  exit 1
fi
if ! curl --output /dev/null --silent ${HOST}/ping; then
  >&2 echo "can't connect to optimus host ${HOST}"
  exit 1
fi

yq '.secrets[] | (.name | . + " ") + (.value | @base64)' ${SETUP_PATH} >> secret_decoded.yaml
while read key value; do
    curl -XPUT -H "Content-Type: application/json" \
    "${HOST}/api/v1beta1/project/${PROJECT}/secret/${key}" \
    -d '{"value": "'${value}'"}' -s -o /dev/null -w "set secret $key: %{http_code}\n"
done < secret_decoded.yaml
rm secret_decoded.yaml
