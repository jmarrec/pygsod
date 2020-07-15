FROM ubuntu:16.04
LABEL maintainer="Christopher MERA <chris@massiveclouds.com>"
LABEL version="1.0"
LABEL description="Container to execute read_excel.py and output json file to volume on host."

USER root

WORKDIR /gsodpy
ENV HOME=/gsodpy

# Send build context, cf .dockerignore
ADD . /gsodpy

RUN mkdir /results &&\
    apt-get update && apt-get install -y python3-pip && pip3 install --upgrade pip
# This will correctly install dependencies as listed in setup.py (if setup.py is correct)
RUN pip3 install -e .

# Entrypoint to containerized app
ENTRYPOINT ["/gsodpy/docker-entrypoint.sh"]
