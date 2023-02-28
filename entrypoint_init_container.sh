#! /bin/sh

#get optimus version
echo "-- optimus client version"
# /app/optimus version

# printenv

# get resources
echo "-- print env variables (required for fetching assets)"
echo "JOB_NAME:$JOB_NAME"
echo "OPTIMUS_PROJECT:$PROJECT"
echo "JOB_DIR:$JOB_DIR"
echo "INSTANCE_TYPE:$INSTANCE_TYPE"
echo "INSTANCE_NAME:$INSTANCE_NAME"
echo "SCHEDULED_AT:$SCHEDULED_AT"
echo "OPTIMUS_HOST:$OPTIMUS_HOST"
echo ""

echo "-- initializing optimus assets"
optimus job run-input "$JOB_NAME" --project-name \
	"$PROJECT" --output-dir "$JOB_DIR" \
	--type "$INSTANCE_TYPE" --name "$INSTANCE_NAME" \
	--scheduled-at "$SCHEDULED_AT" --host "$OPTIMUS_HOST"

touch $JOB_DIR/exec_entrypoint.sh
chmod 755 $JOB_DIR/exec_entrypoint.sh

cat > $JOB_DIR/exec_entrypoint.sh <<EOF
#!/bin/bash
echo "exec entrypoint -------"
# TODO: this doesnt support using back quote sign in env vars, fix it
echo "-- exporting env"
set -o allexport
source "$JOB_DIR/in/.env"
set +o allexport

echo "-- current envs"
cat "$JOB_DIR/in/.env"

echo "-- exporting env with secret"
set -o allexport
source "$JOB_DIR/in/.secret"
set +o allexport

echo "-- running unit"
exec $(eval echo "$@")
EOF
