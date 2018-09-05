FROM python:3.6
WORKDIR /load_test/
ADD requirements.txt .
RUN pip install -r /load_test/requirements.txt
