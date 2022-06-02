FROM python:3.9.10-slim

WORKDIR /usr/src/feast

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV PYTHONPATH=$PYTHONPATH:/usr/src/feast:/usr/src/feast/provider/sdk

RUN apt-get update \
    && apt-get -y install gcc git build-essential procps net-tools \
    && apt-get clean \
    && pip install --no-cache-dir -U pip \
    && rm -rf /var/lib/apt/lists/*

COPY . .

RUN pip install --no-cache-dir -r ./online_server/requirements.txt

RUN git submodule init && git submodule update

RUN cd ./feast/ && patch -p1 < ../feast_ol_server.patch && make install-python

RUN pip3 install git+https://github.com/oneconvergence/dkube.git@feast_changes --upgrade

WORKDIR /usr/src/feast/online_server

RUN addgroup --system olserver && adduser --system --group olserver

USER olserver

CMD ["python", "server.py"]
