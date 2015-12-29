FROM python:2.7-slim
MAINTAINER coding4m@gmail.com

RUN apt-get update && apt-get install -y --no-install-recommends \
		gcc \
		python-dev \
	&& rm -rf /var/lib/apt/lists/*

ONBUILD ADD . /var/dnswall/
ONBUILD RUN cd /var/dnswall/ && python setup.py install