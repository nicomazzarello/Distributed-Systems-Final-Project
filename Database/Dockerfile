FROM postgres:15.1-alpine

ENV POSTGRES_DB=Distribuidos
ENV PGUSER=postgres
ENV POSTGRES_PASSWORD=1234

HEALTHCHECK --interval=5s --timeout=3s \
  CMD pg_isready

LABEL author="Distribuidos"
LABEL description="Base de Datos del Proyecto"
LABEL version="1.0"

COPY ./backup.sql /docker-entrypoint-initdb.d/