FROM clickhouse/clickhouse-server:23.3.8

RUN apt update
RUN apt install gettext-base -y

COPY ./config.xml /etc/clickhouse-server/temp-config.xml

ENTRYPOINT envsubst < /etc/clickhouse-server/temp-config.xml > /etc/clickhouse-server/config.xml && /entrypoint.sh
