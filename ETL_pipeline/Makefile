PY=.venv/bin/python
PIP=.venv/bin/pip

venv:
	python -m venv .venv && $(PIP) install -r requirements.txt

up:
	docker compose up -d

down:
	docker compose down

sim:
	$(PY) src/simulator.py

print:
	$(PY) src/consumer_print.py

validate:
	$(PY) src/consumer_validate.py

ingest:
	$(PY) src/consumer_parquet.py

kpi:
	$(PY) src/kpi_minute.py `date -u +%F`

dash:
	. .venv/bin/activate && streamlit run src/app.py
