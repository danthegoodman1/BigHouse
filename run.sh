docker compose up --build -d

# Wait until cluster is bootstrapped
while true; do
  # output=$(docker exec -it bighouse-ch-1-1 clickhouse-client -q "select count() from system.clusters where cluster = 'randomclustername' FORMAT TabSeparatedRaw")
  output=$(docker exec -it bighouse-ch-1-1 clickhouse-client -q "select 3 from system.zookeeper where path='/clickhouse/task_queue/'")
  status=$?

  # Break the loop if the status code is 1 or the output equals 3
  if [[ "$output" =~ "3" ]]; then
    break
  fi
  echo waiting for boostrap...
done

echo "Cluster bootstrapped, executing query"
time docker exec -it bighouse-ch-1-1 clickhouse-client -q "select * from urlCluster('randomclustername', 'https://datasets-documentation.s3.eu-west-3.amazonaws.com/aapl_stock.csv', 'CSVWithNames') LIMIT 5"
# time docker exec -it bighouse-ch-1-1 clickhouse-client -q "select * from url('https://datasets-documentation.s3.eu-west-3.amazonaws.com/aapl_stock.csv', 'CSVWithNames') LIMIT 5"

docker compose down
