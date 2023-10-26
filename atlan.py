import json
from typing import List
from kafka import KafkaConsumer, KafkaProducer
from pyhive import hive
from pyhive.exc import DatabaseError
import json
from typing import List
from kafka import KafkaConsumer
from metadata import MetadataChangeEvent
from metadata import MetadataChangeEvent
import requests

atlanmetaDataStore = {}
class Atlan:
    def __init__(self, api_key, base_url):
        self.api_key = api_key
        self.base_url = base_url
        
    def write_metadata(self, entity_type, entity_id, change_type, data):
        # Placeholder implementation
        if change_type == "create":
            print(f"Writing metadata: {entity_type}, {entity_id}, {change_type}, {data} to Atlan Dashboard")
        else:
            print(f"Updating metadata: {entity_type}, {entity_id}, {change_type}, {data} to Atlan Dashboard")
        atlanmetaDataStore[entity_id] = ((entity_type, entity_id, change_type, data))

class AtlanProcessor:
    """
    A processor for transforming metadata change events into a format that is compatible with the Atlan metadata model and performing any necessary pre-ingest and post-consume transformations.
    """

    def __init__(self, atlan: Atlan):
        self.atlan = atlan

    def process(self, events: List[MetadataChangeEvent]):
        """
        Process a list of metadata change events.

        Args:
            events: A list of metadata change events.

        Returns:
            A list of processed metadata change events.
        """

        processed_events = []

        for event in events:
            # Transform the metadata change event into a format that is compatible with the Atlan metadata model.
            # ...

            # Perform any necessary pre-ingest and post-consume transformations.
            # ...
            data = {}
            data['entity_type'] = event[0]
            data['entity_id'] = event[1]
            data['change_type'] = event[2]
            data['data'] = event[3]

            processed_events.append(data)

        return processed_events


class AtlanHiveMetadataStore:
    def __init__(self, host, port, database):
        self.host = host
        self.port = port
        self.database = database
        # self.username = username
        # self.password = password

    def connect(self):
        try:
            conn = hive.Connection(
                host=self.host,
                port=self.port,
                database=self.database,
            )
            return conn
        except Exception as e:
            print(f"Error connecting to Hive: {e}")
            return None

    def write_metadata(self, entity_type, entity_id, change_type, data):
        conn = self.connect()
        if conn is not None:
            try:
                with conn.cursor() as cursor:
                    # Prepare I SQL statement to insert or update metadata in Hive
                    # Use entity_type, entity_id, change_type, and data to construct the SQL statement
                    sql = f"INSERT INTO metadata_table (entity_type, entity_id, change_type, data) " \
                          f"VALUES ('{entity_type}', '{entity_id}', '{change_type}', '{json.dumps(data)}')"
                    cursor.execute(sql)
                conn.commit()
                
            except Exception as e:
                print(f"Error writing metadata to Hive: {e}")
            finally:
                cursor.close()
                conn.close()
                
class AtlanKafkaConsumer:
    def __init__(self, bootstrap_servers: str, group_id: str, topic: str):
        self.consumer = KafkaConsumer(
            topic,
            group_id=None,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='latest',
             max_poll_records=1 # This is to ensure that we only consume one message at a time
        )
        global atlanmetaDataStore
    def check_entity_existence(self, entity_id):
        # We will Implement the logic to check if the entity exists in the metadata store
        return entity_id in atlanmetaDataStore.keys()

    def consume(self):
            events = []
            print("Polling for the latest message")
            # Use poll with a timeout (e.g., 1 second) to check for new messages
            msg_pack = self.consumer.poll(timeout_ms=1000)
            if msg_pack:
                # Extract the latest message
                latest_message = None
                for messages in msg_pack.values():
                    for message in messages:
                        latest_message = message

                if latest_message:
                    message_data = json.loads(latest_message.value.decode('utf-8'))
                    entity_id = message_data.get('entity_id')
                    entity_type = message_data.get('entity_type')
                    change_type = message_data.get('change_type')
                    if self.check_entity_existence(entity_id):
                        change_type = "update"
                    #  for downstream_entity in downstream_entities:
                    # # Here, we use the producer to send changes to downstream entities
                    #     downstream_change = (
                    #          entity_type,  # Adjust entity type if needed
                    #         downstream_entity,
                    #         "propagate",
                    #         message_data.get('data'),
                    #     )
                    #     events.append(downstream_change)
                    #     return events
                    data = message_data.get('data')
                    events.append((entity_type, entity_id, change_type, data))
            return events
        
class AtlanKafkaProducer:
    def __init__(self, bootstrap_servers: str, topic: str ):
        self.topic = topic
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers,api_version=(0,10))

    def produce_events(self, events: List[MetadataChangeEvent]):
        for event in events:
            self.producer.send(self.topic, value=json.dumps(event).encode())
        self.producer.flush()
        
        
class AtlanMetadataPipeline:
    
    def __init__(self, atlan: Atlan, bootstrap_servers: str, topic: str, monte_carlo_api_key: str):
        self.atlan = atlan
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        # self.monte_carlo_connector = MonteCarloConnector(atlan, monte_carlo_api_key)
        self.atlan_processor = AtlanProcessor(atlan)
        self.atlan_kafka_consumer = AtlanKafkaConsumer(bootstrap_servers, 'test',topic)
        self.atlan_kafka_producer = AtlanKafkaProducer(bootstrap_servers, topic)

    def inbound(self):
        while True:
            # Consume a batch of metadata change events from the Kafka topic.
            events = self.atlan_kafka_consumer.consume()

            # Process the metadata change events.
            processed_events = self.atlan_processor.process(events)
    
            # Split the events based on entity types for further processing
            column_events = []
            other_events = []
            for event in processed_events:
                if event['entity_type'] == 'column':
                    column_events.append(event)
                else:
                    other_events.append(event)

            # Ingest columns as eventually consistent
            for event in column_events:
                self.atlan.write_metadata(event['entity_type'], event['entity_id'], event['change_type'], event['data'])
                self.write_to_hive(event['entity_type'], event['entity_id'], event['change_type'], event['data'])


            # Process and ingest other asset types as needed
            self.ingest_other_assets(other_events)

    def ingest_other_assets(self, other_events):
        # We will Implement the logic to process and ingest other asset types (databases, schemas, tables, dashboards)
        for event in other_events:
            # Process and ingest other asset types as needed
            self.atlan.write_metadata(event['entity_type'], event['entity_id'], event['change_type'], event['data'])
            self.write_to_hive(event['entity_type'], event['entity_id'], event['change_type'], event['data'])
    def ingest_saaS_data(self, saas_connector, entity_type, entity_id):
        # Assume saas_connector is an instance of SaaSConnector
        data = saas_connector.get_data(f'entities/{entity_id}')
        # You may need to transform and prepare the data before ingestion
        # For example, convert the SaaS-specific format to a standard format

        # Ingest the transformed data into Atlan
        self.write_metadata(entity_type, entity_id, 'create', data)
    def write_to_hive(self, entity_type, entity_id, change_type, data):
        # Create an instance of AtlanHiveMetadataStore
        hive_metadata_store = AtlanHiveMetadataStore(
            host='127.0.0.1',
            port=10000, 
            database='hivedb',
            # username='hive',
            # password='Uday@hive',
        )
        hive_metadata_store.write_metadata(entity_type, entity_id, change_type, data)
        
    def outbound_internal(self):
        while True:
            events = self.atlan_kafka_consumer.consume()
            processed_events = self.atlan_processor.process(events)

            for event in processed_events:
                # We will Implement logic to propagate changes to downstream entities or systems
                # I can use Atlan APIs to trigger updates in downstream systems
                # Example: atlan.propagate_changes(event)
                self.atlan.propagate_changes(event)
    
    def outbound_external(self):
        while True:
            events = self.atlan_kafka_consumer.consume()
            processed_events = self.atlan_processor.process(events)
            
            for event in processed_events:
                # Check if the entity has PII or GDPR annotations
                if self.is_pii_or_gdpr_annotated(event['entity_id']):
                    # We will Implement logic to notify external data tools and enforce access control
                    # Example: atlan.notify_external_system(event)
                    self.atlan.notify_external_system(event)

    def is_pii_or_gdpr_annotated(self, entity_id):
        # We will Implement logic to check if the entity has PII or GDPR annotations in Atlan
        # Example: We can use Atlan APIs to query entity annotations
        # If annotations are found, return True; otherwise, return False
        # Replace this with actual logic
        return False

    def notify_external_system(self, event):
        # We will Implement logic to notify external data tools and enforce access control
        # Example: We can use Atlan APIs to notify and enforce access control
        pass

    def propagate_changes(self, event):
        # We will Implement logic to propagate changes to downstream entities or systems
        # Example: We can use Atlan APIs to trigger updates in downstream systems
        pass


class SaaSConnector:
    def __init__(self, api_key, base_url):
        self.api_key = api_key
        self.base_url = base_url

    def authenticate(self):
        #  We will Implement authentication logic based on the SaaS application's requirements
        # For example, if the SaaS uses API key authentication:
        headers = {
            'Authorization': f'Bearer {self.api_key}'
        }
        return headers

    def get_data(self, endpoint):
        headers = self.authenticate()
        url = f'{self.base_url}/{endpoint}'
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            return data
        else:
            raise Exception(f'Failed to retrieve data from {url}. Status code: {response.status_code}')


