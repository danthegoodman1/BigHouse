FROM clickhouse/clickhouse-keeper:head-alpine

RUN apk add envsubst

COPY ./keeper_config.xml /etc/clickhouse-keeper/temp-config.xml

ENTRYPOINT cat /etc/clickhouse-keeper/temp-config.xml | envsubst > /etc/clickhouse-keeper/keeper_config.xml && /entrypoint.sh
