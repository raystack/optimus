#!/bin/bash
# this script is used to copy plugin artifact to plugins mounted location
# then return the artifact paths which inside the plugins mounted location

OPTIMUS_PLUGINS_PATH=/tmp/colima/plugins
SETUP_PATH=$1

is_yq_installed(){
  if [ -z $(command -v yq) ]; then
    >&2 echo "yq must be installed: \`brew install yq\`"
    exit 1
  fi
}

if [ -z "$SETUP_PATH" ]; then
  >&2 echo "must provide setup_path: ./get_artifacts.sh <setup_path>"
  exit 1
fi

# load plugins
is_yq_installed
mkdir -p ${OPTIMUS_PLUGINS_PATH}
OPTIMUS_PLUGIN_ARTIFACTS=""
yq '.plugins[]' ${SETUP_PATH} >> p.tmp
while read artifact; do
  artifact=$(cp $artifact $OPTIMUS_PLUGINS_PATH 2> /dev/null && echo "/app/plugins/$(basename ${artifact})" || echo $artifact)
  OPTIMUS_PLUGIN_ARTIFACTS="${OPTIMUS_PLUGIN_ARTIFACTS}\,${artifact}"
done < p.tmp && rm p.tmp
OPTIMUS_PLUGIN_ARTIFACTS="\"$(echo $OPTIMUS_PLUGIN_ARTIFACTS | sed 's/^\\,//g')\""
echo $OPTIMUS_PLUGIN_ARTIFACTS