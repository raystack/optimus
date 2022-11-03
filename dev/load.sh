#!/bin/sh

OPTIMUS_PLUGINS_PATH=/tmp/colima/plugins
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
  >&2 echo "must provide setup_path: ./load.sh <setup_path>"
  exit 1
fi

# load plugins
echo ">> load plugins"
is_yq_installed
mkdir -p ${OPTIMUS_PLUGINS_PATH}
OPTIMUS_PLUGIN_ARTIFACTS=""
yq '.plugins[]' ${SETUP_PATH} >> p.tmp
while read artifact; do
  artifact=$(cp $artifact $OPTIMUS_PLUGINS_PATH 2> /dev/null && echo "/app/plugins/$(basename ${artifact})" || echo $artifact)
  OPTIMUS_PLUGIN_ARTIFACTS="${OPTIMUS_PLUGIN_ARTIFACTS},${artifact}"
done < p.tmp && rm p.tmp
OPTIMUS_PLUGIN_ARTIFACTS=`echo $OPTIMUS_PLUGIN_ARTIFACTS | sed 's/^,*//g'`

POD_NAME=$(kubectl get pod -l app.kubernetes.io/name=optimus -n optimus-dev -o=jsonpath='{.items[0].metadata.name}') 2> /dev/null
if [[ ! $? -eq 0 || -z $POD_NAME ]]; then
  >&2 echo "no optimus server running, make sure \`make apply\` is executed properly"
  exit 1
fi

kubectl exec -it ${POD_NAME} -- /bin/sh -c "OPTIMUS_PLUGIN_ARTIFACTS=${OPTIMUS_PLUGIN_ARTIFACTS} optimus plugin install"

# load secrets
echo ">> load secrets into project ${PROJECT}"
is_yq_installed
if ! curl --output /dev/null --silent ${HOST}/ping; then
  >&2 echo "can't connect to optimus host ${HOST}"
  exit 1
fi

yq '.secrets[] | (.name | . + " ") + (.value | @base64)' ${SETUP_PATH} >> s.tmp
while read key value; do
    curl -XPUT -H "Content-Type: application/json" \
    "${HOST}/api/v1beta1/project/${PROJECT}/secret/${key}" \
    -d '{"value": "'${value}'"}' -s -o /dev/null -w "set secret $key: %{http_code}\n"
done < s.tmp && rm s.tmp
