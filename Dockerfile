FROM python:2.7.15-jessie

RUN apt-get update && apt-get install -y --no-install-recommends libleveldb-dev

COPY . /tmp/dredis-src
RUN pip install /tmp/dredis-src

CMD dredis --port 6377 --host 0.0.0.0 --dir /dredis-data --debug
