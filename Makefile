up:
	docker-compose up -d

down:
	docker-compose down

build:
	docker build -t flink:1.14.0-dev .

install-generator-requirements:
	cd generator && virtualenv venv && source venv/bin/activate && pip3 install -r requirements.txt

install-main-requirements:
	virtualenv venv && source venv/bin/activate && pip3 install -r requirements.txt

run-generator:
	cd generator && source venv/bin/activate && python3 data_generator.py

run-main:
	source venv/bin/activate && python3 src/flink_stream_processor.py