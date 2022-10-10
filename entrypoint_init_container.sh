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
OPTIMUS_ADMIN_ENABLED=1 optimus job run-input "$JOB_NAME" --project-name \
	"$PROJECT" --output-dir "$JOB_DIR" \
	--type "$INSTANCE_TYPE" --name "$INSTANCE_NAME" \
	--scheduled-at "$SCHEDULED_AT" --host "$OPTIMUS_HOST"

# if [ $? -ne 0 ]; then
# echo "-- job run-input failed"
# echo "-- try to register and initializing optimus assets"

# OPTIMUS_ADMIN_ENABLED=1 /opt/optimus admin build instance "$JOB_NAME" --project-name \
# 	"$PROJECT" --output-dir "$JOB_DIR" \
# 	--type "$INSTANCE_TYPE" --name "$INSTANCE_NAME" \
# 	--scheduled-at "$SCHEDULED_AT" --host "$OPTIMUS_HOST"
# # fi
