# Real-Time E-Commerce Analytics Pipeline
A Real-time analytics project for E-commerce data designed to be robust and scalable, with health checks, restart policies, and network configurations ensuring efficient communication and fault tolerance.


## Stack
### Kafka: The Data Streaming Hub
Once the data is generated, it’s streamed into a Kafka cluster. Our setup includes a 2-broker Kafka cluster, efficiently managing high-throughput data streaming and serving as a reliable conduit to the next phase of processing.

### Spark: The Processing Powerhouse
As the data arrives in Kafka, Spark Structured Streaming comes into play. Our Spark setup, written in Python, consumes the data, applies real-time processing and analytics, and readies it for storage and visualization.

### Elasticsearch: The Storage Solution
Post-processing, Spark forwards the data to Elasticsearch, where it’s indexed for efficient storage, quick retrieval, and subsequent analytical processes.

### Kibana: Turning Data into Insights
Kibana, integrated with Elasticsearch, is where data transforms into actionable insights. It enables us to create interactive dashboards that reflect real-time e-commerce metrics, helping businesses make data-driven decisions swiftly.

### Docker: Orchestrating Our Services
Each service in our project, from Kafka to Spark to Elasticsearch, runs in isolated Docker containers. This modular architecture ensures smooth interoperability, simplifies scalability and aids in debugging.

