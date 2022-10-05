EXPECTED_CONTEXT=colima
NAMESPACE=optimus-dev

apply.airflow:
	helm repo add apache-airflow https://airflow.apache.org
	helm upgrade --install airflow apache-airflow/airflow\
		--namespace ${NAMESPACE} \
		-f airflow.values.yaml

apply.optimus:
	helm install optimus-dev ~/Documents/proj/odpf/charts/stable/optimus -n ${NAMESPACE} -f optimus.values.yaml

upgrade.optimus:
	helm upgrade optimus-dev ~/Documents/proj/odpf/charts/stable/optimus -n ${NAMESPACE} -f optimus.values.yaml

_context:
	kubectl config use-context $(EXPECTED_CONTEXT)

_pre: _context
	mkdir -p /tmp/colima/dags

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
	# brew install colima
	colima start --cpu 4 --memory 8 --kubernetes