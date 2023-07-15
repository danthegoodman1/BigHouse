FROM clickhouse/clickhouse-server:23.6-alpine

RUN apk add envsubst

COPY ./config.xml /etc/clickhouse-server/temp-config.xml

ENTRYPOINT cat /etc/clickhouse-server/temp-config.xml | envsubst > /etc/clickhouse-server/config.xml && /entrypoint.sh
