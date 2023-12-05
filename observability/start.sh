docker compose down
docker compose wait prometheus
docker compose wait pushgateway
docker compose wait grafana

docker volume rm observability_prometheus_data
docker compose up
