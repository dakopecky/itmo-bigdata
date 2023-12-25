from argparse import ArgumentParser
from pyflink.common import SimpleStringSchema, Time
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.datastream.functions import MapFunction
from pyflink.datastream.checkpoint_config import CheckpointingMode
from pyflink.datastream.window import TumblingEventTimeWindows, SlidingEventTimeWindows, EventTimeSessionWindows
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.time import Duration


class FormatTemperature(MapFunction):
    def map(self, value):
        return "max_temp: {}".format(value[1])


def python_data_stream_example(checkpoint_dir, window_type):
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(10000)
    env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_min_pause_between_checkpoints(500)
    env.get_checkpoint_config().set_max_concurrent_checkpoints(1)
    env.get_checkpoint_config().set_checkpoint_storage_dir(checkpoint_dir)

    type_info = Types.ROW_NAMED(["device_id", "temperature", "execution_time"],
                                [Types.LONG(), Types.DOUBLE(), Types.INT()])

    json_row_schema = JsonRowDeserializationSchema.builder().type_info(type_info).build()

    source = KafkaSource.builder() \
        .set_bootstrap_servers('kafka:9092') \
        .set_topics('hw3_topic') \
        .set_group_id('pyflink-e2e-source') \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(json_row_schema) \
        .build()

    sink = KafkaSink.builder() \
        .set_bootstrap_servers('kafka:9092') \
        .set_record_serializer(KafkaRecordSerializationSchema.builder()
                               .set_topic('hw3_preprocessed_topic')
                               .set_value_serialization_schema(SimpleStringSchema())
                               .build()
                               ) \
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .build()

    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5)) \
        .with_timestamp_assigner(lambda event, timestamp: event[2])

    ds = env.from_source(source, watermark_strategy, "Kafka Source")

    if window_type == "tumbling":
        ds = ds.key_by(lambda value: value[0]) \
            .window(TumblingEventTimeWindows.of(Time.seconds(15))) \
            .reduce(lambda a, b: a if a[1] > b[1] else b) \
            .map(FormatTemperature(), output_type=Types.STRING())
    elif window_type == "sliding":
        ds = ds.key_by(lambda value: value[0]) \
            .window(SlidingEventTimeWindows.of(Time.seconds(15), Time.seconds(5))) \
            .reduce(lambda a, b: a if a[1] > b[1] else b) \
            .map(FormatTemperature(), output_type=Types.STRING())
    elif window_type == "session":
        ds = ds.key_by(lambda value: value[0]) \
            .window(EventTimeSessionWindows.with_gap(Time.seconds(10))) \
            .reduce(lambda a, b: a if a[1] > b[1] else b) \
            .map(FormatTemperature(), output_type=Types.STRING())
    else:
        raise ValueError("Unsupported window type: {}".format(window_type))

    ds.sink_to(sink)
    env.execute_async("Devices preprocessing")

if __name__ == '__main__':
    parser = ArgumentParser(description="Flink Job with Different Window Types")
    parser.add_argument("--checkpoint_dir", type=str, help="Directory for saving checkpoints",
                        default='file:///opt/pyflink/tmp/checkpoints/logs')
    parser.add_argument("--window_type", type=str, help="Type of window to apply (tumbling, sliding, session)",
                        default='tumbling')
    args = parser.parse_args()
    python_data_stream_example(args.checkpoint_dir, args.window_type)
