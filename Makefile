export PYTHONPATH=.

DEBUG ?= --debug
FLUSHALL_ON_STARTUP ?= --flushall
PORT ?= --port 6377
TEST_OPTIONS = $(DEBUG) $(FLUSHALL_ON_STARTUP) $(PORT)
PID = dredis-test-server.pid
REDIS_PID = redis-test-server.pid

PROFILE_DIR ?= --dir /tmp/dredis-data
PROFILE_PORT = --port 6376
PROFILE_OPTIONS = $(PROFILE_DIR) $(FLUSHALL_ON_STARTUP) $(PROFILE_PORT)
STATS_FILE = stats.prof
STATS_METRIC ?= cumtime
PERFORMANCE_PID = dredis-performance-test-server.pid


fulltests:
	bash -c "trap 'make stop-testserver' EXIT; make start-testserver DEBUG=''; make test"

fulltests-real-redis:
	bash -c "trap 'make stop-redistestserver' EXIT; make start-redistestserver; make test"

test: unit integration lint

unit: setup
	@py.test -v tests/unit

integration: setup
	@py.test -v tests/integration

lint: setup
	@flake8 --exclude tests/fixtures .

server:
	python -m dredis.server $(TEST_OPTIONS)

start-testserver:
	-python -m dredis.server $(TEST_OPTIONS) 2>&1 & echo $$! > $(PID)

stop-testserver:
	@-touch $(PID)
	@-kill `cat $(PID)` 2> /dev/null
	@-rm $(PID)

setup: clean
	@pip install -r development.txt --quiet

start-redistestserver:
	-@redis-server $(PORT) 2>&1 & echo $$! > $(REDIS_PID)

stop-redistestserver:
	@-touch $(REDIS_PID)
	@-kill `cat $(REDIS_PID)` 2> /dev/null
	@-rm $(REDIS_PID)

redis_server:
	@mkdir -p dredis-data
	PYTHONPATH=. python -m dredis.server --dir /tmp/dredis-data --port 6379

release:
	rm -rf dist build
	./release.sh

test-performance:
	@py.test -vvvvv -s tests-performance

performance-server:
	python -m cProfile -o $(STATS_FILE) dredis/server.py $(PROFILE_OPTIONS)

performance-stats:
	python -c 'import pstats ; pstats.Stats("$(STATS_FILE)").sort_stats("$(STATS_METRIC)").print_stats()' | less

clean:
	rm -rf build/ dist/
	find . -name '*.pyc' -delete
	rm -f dump_*.rdb
