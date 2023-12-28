import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer


def main():
    env = StreamExecutionEnvironment.get_execution_environment()

    properties = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'flink-group'
    }

    kafka_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'flink-sql-connector-kafka_2.12-1.14.6.jar')
    env.add_jars(f"file:///{kafka_jar}")

    kafka_source = FlinkKafkaConsumer(
        'financial_transactions',
        SimpleStringSchema(),
        properties=properties
    )

    kafka_sink = FlinkKafkaProducer(
        'processed_transactions',
        SimpleStringSchema(),
        producer_config=properties
    )

    stream = env.add_source(kafka_source)
    stream.add_sink(kafka_sink)

    processed_stream = stream.map(lambda x: f"Processed: {x}")
    processed_stream.print()

    env.execute("Kafka Flink Python Example")


if __name__ == '__main__':
    main()
