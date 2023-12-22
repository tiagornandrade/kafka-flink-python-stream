up:
	docker-compose up -d

down:
	docker-compose down

run:
	docker-compose exec jobmanager ./bin/flink run -py /opt/flink_pipeline/job.py