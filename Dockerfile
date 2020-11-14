FROM python:latest

ADD kill-watcher.py /
ADD requirements.txt /
ADD config.toml /

RUN pip install -r requirements.txt

CMD [ "python", "./kill-watcher.py" ]
