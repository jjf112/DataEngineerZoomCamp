FROM python:3.9

RUN apt update
RUN apt install wget
RUN pip install pandas sqlalchemy psycopg2

COPY ingest_data.py ingest_data.py

ENTRYPOINT [ "python", "ingest_data.py" ]