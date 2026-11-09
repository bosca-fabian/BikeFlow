"""Speed layer: Kafka -> Spark Structured Streaming -> Redis.

Owner: streaming lead. The human-readable contract of the Redis keys is
``docs/SPEED_LAYER.md``; ``bikeflow.streaming.redis_keys`` is the code version.

Modules
-------
model       what a station_status message means (empty / full / out of service)
alerts      the empty/full alert lifecycle (open, escalate, close)
warnings    the 30-minute early warnings from the batch demand profiles
redis_sink  writes the speed layer to Redis (one pipeline per micro-batch)
lake_extras reads station metadata from the batch `stations` table (name, position)
job         the Structured Streaming job itself
cli         ``bikeflow-stream run | dump | reset``
"""
