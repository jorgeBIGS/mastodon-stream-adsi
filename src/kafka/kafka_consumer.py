from confluent_kafka.avro import AvroConsumer


def consume_record(topic_names):
    default_group_name = "default-consumer-group"
    
    consumer_config = {
        'bootstrap.servers': 'localhost:9092',
        'schema.registry.url': 'http://localhost:8081',
        'group.id': 'consumer1', 
        'broker.address.family': 'v4'
    }

    consumer = AvroConsumer(consumer_config)

    consumer.subscribe(topic_names)

    try:
        message = consumer.poll(5)
    except Exception as e:
        print(f"Exception while trying to poll messages - {e}")
    else:
        if message:
            print(f"Successfully poll a record from "
                  f"Kafka topic: {message.topic()}, partition: {message.partition()}, offset: {message.offset()}\n"
                  f"message key: {message.key()} || message value: {message.value()}")
            consumer.commit()
        else:
            print("No new messages at this point. Try again later.")

    consumer.close()


if __name__ == "__main__":
    while(True):
        consume_record(['mastodon-topic-batch'])
        consume_record(['mastodon-topic-speed'])