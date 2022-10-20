EXPECTED_CONTEXT=colima
NAMESPACE=optimus-dev
OPTIMUS_ODPF_CHART=odpf/optimus

apply.airflow: _context
	helm repo add apache-airflow https://airflow.apache.org
	helm upgrade --install airflow apache-airflow/airflow\
		--namespace ${NAMESPACE} \
		-f airflow.values.yaml

apply.optimus: _context
	# to create placeholder plugins for internal and odpf plugins
	echo "test" >  /tmp/colima/test
	tar -czvf /tmp/colima/internal-plugins.tar.gz /tmp/colima/test
	tar -czvf /tmp/colima/transformers.tar.gz /tmp/colima/test
	rm /tmp/colima/test

	helm repo add odpf https://odpf.github.io/charts/
	helm upgrade --install optimus-dev ${OPTIMUS_ODPF_CHART} -n ${NAMESPACE} -f optimus.values.yaml --version '>0.0.2'

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
	colima start --cpu 4 --memory 8 --kubernetes