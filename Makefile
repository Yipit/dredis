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
	PYTHONPATH=. DREDIS_PORT=6379 python dredis/server.py
