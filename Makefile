DEBUG ?= --debug
ROOT_DIR ?= --dir dredis-data
FLUSHALL_ON_STARTUP ?= --flushall
OPTIONS = $(DEBUG) $(ROOT_DIR) $(FLUSHALL_ON_STARTUP)
TEST_PORT_OPTION = --port 6377

PID = redis-test-server.pid

fulltests:
	bash -c "trap 'make stop-testserver' EXIT; make start-testserver DEBUG=''; make test"

test: unit integration lint

unit: setup
	@py.test -v tests/unit

integration: setup
	@py.test -v tests/integration

lint: setup
	@flake8 .

server:
	PYTHONPATH=. python -m dredis.server $(OPTIONS) $(TEST_PORT_OPTION)

start-testserver:
	-PYTHONPATH=. python -m dredis.server $(OPTIONS) $(TEST_PORT_OPTION) 2>&1 & echo $$! > $(PID)

stop-testserver:
	@-touch $(PID)
	@-kill `cat $(PID)` 2> /dev/null
	@-rm $(PID)

setup:
	@pip install -r development.txt --quiet

redis_server:
	@mkdir -p dredis-data
	PYTHONPATH=. python -m dredis.server $(OPTIONS) --port 6379

release:
	rm -rf dist
	python setup.py sdist bdist_wheel
	twine upload dist/*
