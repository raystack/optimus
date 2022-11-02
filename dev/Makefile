EXPECTED_CONTEXT=colima
NAMESPACE=optimus-dev
OPTIMUS_ODPF_CHART=odpf/optimus

DAGS_PATH?=/tmp/colima/dags
OPTIMUS_PLUGINS_PATH?=/tmp/colima/plugins
OPTIMUS_PLUGINS_ARTIFACT?=./optimus-dummy-plugins.tar.gz
OPTIMUS_SERVE_PORT?=9100

apply.airflow: _context
	helm repo add apache-airflow https://airflow.apache.org
	helm upgrade --install airflow apache-airflow/airflow \
		--namespace ${NAMESPACE} \
		--set scheduler.extraVolumes[0].hostPath.path=${DAGS_PATH} \
		-f airflow.values.yaml

apply.optimus: _context
	helm repo add odpf https://odpf.github.io/charts/
	helm upgrade --install optimus-dev ${OPTIMUS_ODPF_CHART} \
	-n ${NAMESPACE} \
	--set volumes[0].hostPath.path=${DAGS_PATH} \
	--set volumes[1].hostPath.path=${OPTIMUS_PLUGINS_PATH} \
	--set volumes[2].hostPath.path=${OPTIMUS_PLUGINS_ARTIFACT} \
	--set container.volumeMounts[2].mountPath=/app/$(notdir ${OPTIMUS_PLUGINS_ARTIFACT}) \
	--set config.OPTIMUS_SERVE_PORT=${OPTIMUS_SERVE_PORT} \
	--set config.OPTIMUS_PLUGIN_ARTIFACTS=/app/$(notdir ${OPTIMUS_PLUGINS_ARTIFACT}) \
	-f optimus.values.yaml --version '>0.0.2'

_context:
	kubectl config use-context $(EXPECTED_CONTEXT)

_pre: _context
	kubectl create ns ${NAMESPACE} | true
	kubectl apply -f pvc.yaml -n ${NAMESPACE}

	kubectl apply -f postgres.yaml -n ${NAMESPACE}
	kubectl wait --namespace=${NAMESPACE} --timeout=120s --for=condition=Available deployment/optimus-db
	
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