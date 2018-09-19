DEBUG ?= --debug
FLUSHALL_ON_STARTUP ?= --flushall
PORT ?= --port 6377
TEST_OPTIONS = $(DEBUG) $(FLUSHALL_ON_STARTUP) $(PORT)

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
	PYTHONPATH=. python -m dredis.server $(TEST_OPTIONS)

start-testserver:
	-PYTHONPATH=. python -m dredis.server $(TEST_OPTIONS) 2>&1 & echo $$! > $(PID)

stop-testserver:
	@-touch $(PID)
	@-kill `cat $(PID)` 2> /dev/null
	@-rm $(PID)

setup:
	@pip install -r development.txt --quiet

redis_server:
	@mkdir -p dredis-data
	PYTHONPATH=. python -m dredis.server --dir /tmp/dredis-data --port 6379

release:
	rm -rf dist
	python setup.py sdist bdist_wheel
	twine upload dist/*
