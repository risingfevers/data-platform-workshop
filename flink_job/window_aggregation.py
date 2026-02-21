import logging
import sys
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.common import Types, WatermarkStrategy, Time
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.datastream.functions import ReduceFunction

# Настройка логирования
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

# Тип данных для входящего сообщения
click_type = Types.ROW_NAMED(
    ['product_id', 'category', 'timestamp', 'user_id'],
    [Types.INT(), Types.STRING(), Types.LONG(), Types.INT()]
)

# Тип данных для агрегата (product_id, count)
agg_type = Types.ROW_NAMED(['product_id', 'count'], [Types.INT(), Types.LONG()])

class CountReducer(ReduceFunction):
    def reduce(self, value1, value2):
        return (value1[0], value1[1] + value2[1])

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars("file:///путь/к/flink-sql-connector-kafka-3.0.1-1.18.jar")  # при необходимости

    # Источник Kafka
    deserialization_schema = JsonRowDeserializationSchema.builder() \
        .type_info(click_type) \
        .build()

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_topics("clicks") \
        .set_group_id("flink-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(deserialization_schema) \
        .build()

    stream = env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "Kafka Source")

    # Преобразование в кортеж (product_id, 1) для суммирования
    mapped = stream.map(lambda click: (click[0], 1), output_type=Types.TUPLE([Types.INT(), Types.LONG()]))

    # Оконная агрегация (окна по 5 минут)
    windowed = mapped.window_all(TumblingProcessingTimeWindows.of(Time.minutes(5))) \
                     .reduce(CountReducer())

    # Сериализация результата в JSON
    serialization_schema = JsonRowSerializationSchema.builder() \
        .with_type_info(agg_type) \
        .build()

    # Приёмник Kafka
    kafka_sink = KafkaSink.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic("aggregated_clicks")
                .set_value_serialization_schema(serialization_schema)
                .build()
        ) \
        .build()

    windowed.sink_to(kafka_sink)

    env.execute("Click Aggregation Job")

if __name__ == "__main__":
    main()
