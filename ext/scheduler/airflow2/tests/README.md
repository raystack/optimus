### Run tests

```bash
cd ext/scheduler/airflow2/tests/
python3 -m virtualenv venv
source venv/bin/activate
pip install -r requirements.txt
make test
# or
py.test -s -v . --disable-warnings
```
