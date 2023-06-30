FROM python:3-alpine

ENV DBHOST=database
ENV DBPORT=5432
ENV DBNAME=Distribuidos
ENV DBUSER=postgres
ENV DBPASSWORD=1234
ENV AGENTE_HOST=agente
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN \
 apk add --no-cache postgresql-libs && \
 apk add --no-cache --virtual .build-deps gcc musl-dev postgresql-dev && \
 python3 -m pip install psycopg2 --no-cache-dir && \
 apk --purge del .build-deps

COPY . .

CMD ["python3", "nodo.py", "-u", "run", "--host=0.0.0.0"]