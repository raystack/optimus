EXPECTED_CONTEXT=colima
NAMESPACE=optimus-dev
OPTIMUS_goto_CHART=goto/optimus
OPTIMUS_PLUGINS_PATH=/tmp/colima/plugins

DAGS_PATH?=/tmp/colima/dags
OPTIMUS_SERVE_PORT?=9100
SETUP_FILE_PATH?=./setup.yaml

PROJECT?=project-a
HOST?=localhost:9100

apply.airflow: _context
	mkdir -p ${DAGS_PATH}
	helm repo add apache-airflow https://airflow.apache.org
	helm upgrade --install airflow apache-airflow/airflow \
		--namespace ${NAMESPACE} \
		--set scheduler.extraVolumes[0].hostPath.path=$(abspath ${DAGS_PATH}) \
		-f airflow.values.yaml

apply.optimus: _context
	mkdir -p ${DAGS_PATH}
	mkdir -p ${OPTIMUS_PLUGINS_PATH}
	helm repo add goto https://goto.github.io/charts/
	$(eval OPTIMUS_PLUGIN_ARTIFACTS = $(shell ./get_artifacts.sh ${SETUP_FILE_PATH}))
	helm upgrade --install optimus-dev ${OPTIMUS_goto_CHART} \
	-n ${NAMESPACE} \
	--set volumes[0].hostPath.path=$(abspath ${DAGS_PATH}) \
	--set volumes[1].hostPath.path=$(abspath ${OPTIMUS_PLUGINS_PATH}) \
	--set config.OPTIMUS_SERVE_PORT=${OPTIMUS_SERVE_PORT} \
	--set config.OPTIMUS_PLUGIN_ARTIFACTS=${OPTIMUS_PLUGIN_ARTIFACTS} \
	-f optimus.values.yaml --version '>0.0.2'

_load.secrets:
	PROJECT=${PROJECT} HOST=${HOST} ./load_secrets.sh ${SETUP_FILE_PATH}

_context:
	kubectl config use-context $(EXPECTED_CONTEXT)

_pre: _context
	kubectl create ns ${NAMESPACE} | true
	kubectl apply -f pvc.yaml -n ${NAMESPACE}

	kubectl apply -f postgres.yaml -n ${NAMESPACE}
	kubectl wait --namespace=${NAMESPACE} --timeout=120s --for=condition=Available deployment/optimus-db

	mkdir -p $(abspath ${DAGS_PATH})
	
apply: _pre apply.optimus apply.airflow

# -----------------------------------
delete.airflow: _context
	helm uninstall airflow -n ${NAMESPACE} 

delete.optimus: _context
	helm uninstall optimus-dev -n ${NAMESPACE} 

delete: _context
	kubectl delete ns ${NAMESPACE}

# -----------------------------------
start-colima:
	colima start --cpu 4 --memory 8 --kubernetes