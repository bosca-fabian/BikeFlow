"""BikeFlow: real-time rebalancing radar for bike-share systems.

Shared modules every component can import:
    bikeflow.config   settings from environment variables
    bikeflow.lake     data-lake layout and table paths
    bikeflow.spark    Delta-enabled SparkSession (get_spark)
    bikeflow.schemas  station_status message schema
    bikeflow.streams  readers for the Kafka topic and the raw lake table
Ingestion lives in bikeflow.ingest (services: `bikeflow-ingest produce | sink`).
"""

__version__ = "0.1.0"
