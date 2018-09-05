test: unit integration

unit: setup
	@py.test -v tests/unit

integration: setup
	@py.test -v tests/integration

server:
	PYTHONPATH=. python dredis/server.py

setup:
	@pip install -r development.txt --quiet

redis_server:
	@mkdir -p dredis-data
	PYTHONPATH=. DREDIS_PORT=6379 ROOT_DIR=dredis-data python dredis/server.py
