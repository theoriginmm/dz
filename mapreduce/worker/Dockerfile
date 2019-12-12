FROM python:3-alpine

RUN pip install pika

ADD rpc_server.py /rpc_server.py

WORKDIR /files

ENTRYPOINT ["python", "-u", "/rpc_server.py"]