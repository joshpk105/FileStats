FROM python:latest
WORKDIR /code
COPY ./requirements.txt ./
RUN pip install -r requirements.txt