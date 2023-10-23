from atlan import Atlan, AtlanMetadataPipeline
from metadata import MetadataConnector
class MonteCarloConnector(MetadataConnector):
    """
    A connector for ingesting metadata change events from Monte Carlo.
    """

    def __init__(self, atlan: Atlan, monte_carlo_api_key: str):
        super().__init__(atlan)

        self.monte_carlo_api_key = monte_carlo_api_key

    def consume(self):
        """
        Consume metadata change events from Monte Carlo.

        Returns:
            A list of metadata change events.
        """

        events = []

        # Get the latest metadata change events from Monte Carlo.
        # ...

        for event in events:
            # Convert the Monte Carlo event to an Atlan metadata change event.
            # ...

            # Ingest the Atlan metadata change event into Atlan.
            self.ingest([event])


def main():
    atlan = Atlan(api_key="my_atlan_api_key", base_url="https://my-atlan-instance.com/api/v1")
    # generic_connector = GenericMetadataConnector(atlan=atlan)
    # atlan_processor = AtlanProcessor(atlan=atlan)
    # kafka_consumer = AtlanKafkaConsumer(bootstrap_servers="127.0.0.1:9092", group_id="test", topic="test")
    # kafka_producer = AtlanKafkaProducer(bootstrap_servers="127.0.0.1:9092", topic="test")
    # events_from_source = kafka_consumer.consume()
    # processed_events = atlan_processor.process(events_from_source)
    # for event in processed_events:
    #     atlan.write_metadata(event['entity_type'], event['entity_id'], event['change_type'], event['data'])
    # kafka_producer.produce_events(processed_events)
    # except Exception as e:
    #     logging.error(f"Error processing metadata change events: {e}")
    # finally:
    # kafka_consumer.consumer.close()
    # kafka_producer.producer.clo----------------------------------------------se()
    atlanMetaDataPipeline = AtlanMetadataPipeline(atlan=atlan, bootstrap_servers="127.0.0.1:9092", topic="test", monte_carlo_api_key= "Monte_Carlo_api_l=key")
    atlanMetaDataPipeline.inbound()
if __name__ == "__main__":
    main()
