DEBUG = 1
ROOT_DIR = dredis-data
PORT = 6379

test: unit integration

unit: setup
	@py.test -v tests/unit

integration: setup
	@py.test -v tests/integration

server:
	PYTHONPATH=. DEBUG=$(DEBUG) python dredis/server.py

setup:
	@pip install -r development.txt --quiet

redis_server:
	@mkdir -p dredis-data
	PYTHONPATH=. DREDIS_PORT=$(PORT) ROOT_DIR=$(ROOT_DIR) DEBUG=$(DEBUG) python -m dredis.server
