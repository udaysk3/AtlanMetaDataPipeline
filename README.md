<!DOCTYPE html>
<html>


<body>
  <h1>Installation and Usage Guide</h1>

  <h2>Prerequisites</h2>
  <ul>
    <li>Python 3.7+</li>
    <li>Docker (for containerization)</li>
    <li>Docker Compose (for managing multi-container applications)</li>
  </ul>

  <h2>1. Set Up Kafka</h2>

  <h3>Install Kafka:</h3>
  <p>Download and install Apache Kafka from the <a href="https://kafka.apache.org/quickstart">official website</a>.</p>

  <h3>Start ZooKeeper:</h3>
  <pre>
bin/zookeeper-server-start.sh config/zookeeper.properties
</pre>

  <h3>Start Kafka Server:</h3>
  <pre>
bin/kafka-server-start.sh config/server.properties
</pre>

  <h3>Create a Kafka Topic:</h3>
  <pre>
bin/kafka-topics.sh --create --topic test --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
</pre>

  <h2>2. Set Up Hive (Optional)</h2>

  <p>If you have specific use cases that require Hive integration:</p>

  <h3>Install Hive:</h3>
  <p>Download and install Apache Hive from the <a href="https://hive.apache.org/downloads.html">official website</a>.</p>

  <h3>Start the Hive Metastore:</h3>
  <pre>
bin/schematool -dbType derby -initSchema
</pre>
  <p>Replace "derby" with your preferred database type.</p>

  <h2>3. Running the Kafka Producer</h2>

  <h3>Clone the Repository:</h3>
  <p>Clone your metadata management project repository to your local machine.</p>

  <h3>Install Dependencies:</h3>
  <pre>
pip install -r requirements.txt
</pre>

  <h3>Run the Kafka Producer:</h3>
  <pre>
python kafka_producer.py
</pre>
  <p>The producer will send metadata events to the "test" Kafka topic.</p>

  <h2>4. Running the Kafka Consumer</h2>

  <h3>Run the Kafka Consumer:</h3>
  <pre>
python kafka_consumer.py
</pre>
  <p>The consumer will receive and process these events, implementing pre-ingest and post-consume transformations as needed.</p>

  <p><strong>Note:</strong> The provided instructions cover a basic setup for Kafka. Depending on your specific requirements, you may need to configure Kafka and Hive further.</p>

  <p>This guide assumes you have Python 3.7+ installed, but you can adapt it for your specific Python version if needed.</p>

  <p>Enjoy using your metadata management platform with real-time ingestion and enrichment!</p>
</body>

</html>
