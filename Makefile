.PHONY: install kafka produce consume demo-files test lint clean

PY ?= python
BOOTSTRAP ?= localhost:9092

install:
	$(PY) -m pip install -r requirements.txt

kafka:        ## start Redpanda (Kafka API) and its console
	docker compose up -d

produce:      ## publish the CDC stream to Kafka
	$(PY) -m cdcstream produce --kafka $(BOOTSTRAP)

consume:      ## drain the topic into Delta and print the report
	$(PY) -m cdcstream consume --kafka $(BOOTSTRAP)

demo-files:   ## same pipeline without Kafka, using the file source
	$(PY) -m cdcstream produce && $(PY) -m cdcstream consume

test:
	$(PY) -m pytest

lint:
	ruff check .

clean:
	rm -rf lake spark-warehouse metastore_db derby.log .pytest_cache
