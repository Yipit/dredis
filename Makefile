DEBUG = 1
ROOT_DIR = dredis-data
PORT = 6379
FLUSHALL_ON_STARTUP = 1
PID = redis-test-server.pid

fulltests:
	bash -c "trap 'make stop-testserver' EXIT; make start-testserver DEBUG=0; make test"

test: unit integration lint

unit: setup
	@py.test -v tests/unit

integration: setup
	@py.test -v tests/integration

lint: setup
	@flake8 .

server:
	PYTHONPATH=. DEBUG=$(DEBUG) FLUSHALL_ON_STARTUP=$(FLUSHALL_ON_STARTUP) python -m dredis.server

start-testserver:
	-PYTHONPATH=. DEBUG=$(DEBUG) FLUSHALL_ON_STARTUP=$(FLUSHALL_ON_STARTUP) python -m dredis.server 2>&1 & echo $$! > $(PID)

stop-testserver:
	@-touch $(PID)
	@-kill `cat $(PID)` 2> /dev/null
	@-rm $(PID)

setup:
	@pip install -r development.txt --quiet

redis_server:
	@mkdir -p dredis-data
	PYTHONPATH=. DREDIS_PORT=$(PORT) ROOT_DIR=$(ROOT_DIR) DEBUG=$(DEBUG) python -m dredis.server
