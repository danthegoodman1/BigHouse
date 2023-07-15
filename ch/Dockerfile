FROM clickhouse/clickhouse-server:23.6

RUN apt update
RUN apt install gettext-base -y

COPY ./config.xml /etc/clickhouse-server/temp-config.xml

ENTRYPOINT cat /etc/clickhouse-server/temp-config.xml | envsubst > /etc/clickhouse-server/config.xml && /entrypoint.sh
