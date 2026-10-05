import json
import os
import random
import time

import boto3

kinesis = boto3.client("kinesis")

STREAM_NAME = os.environ.get("STREAM_NAME", "sample_replay_input_stream")
NUM_RECORDS = int(os.environ.get("NUM_RECORDS", "10"))

FROM_LOCATIONS = ["Manhattan", "Brooklyn", "Queens", "Bronx", "StatenIsland"]
TO_LOCATIONS = ["JFK", "LaGuardia", "Newark", "TimesSquare", "CentralPark"]
CATEGORIES = ["standard", "premium", "shared", "xl"]

# random is the Mersenne Twister PRNG, which is not cryptographically secure.
# That is intentional and appropriate here: this function produces sample trip
# records to seed a test stream. No secret, key or token is generated, and a
# seedable generator is what makes synthetic data reproducible.


def build_trip_record(trip_id: int) -> dict:
    """
    Produce a record matching the JSON shape that JsonDeserializationSchema reads:
      {"tripId": .., "timestamp": .., "fromLocationName": .., "toLocationName": ..,
       "distance": .., "amount": .., "category": ..}
    Values stay inside the bounds the deserializer validates.
    """
    return {
        "tripId": trip_id,
        "timestamp": int(time.time() * 1000),
        "fromLocationName": random.choice(FROM_LOCATIONS),
        "toLocationName": random.choice(TO_LOCATIONS),
        "distance": round(random.uniform(0.5, 40.0), 2),   # 0 <= distance <= 10000
        "amount": round(random.uniform(5.0, 250.0), 2),    # 0 <= amount <= 100000
        "category": random.choice(CATEGORIES),
    }


def lambda_handler(event, context):
    records = []
    for i in range(NUM_RECORDS):
        payload = json.dumps(build_trip_record(trip_id=i))
        records.append(
            {
                "Data": payload.encode("utf-8"),
                # Partition key spreads records across shards; any stable value works.
                "PartitionKey": str(i),
            }
        )

    response = kinesis.put_records(StreamName=STREAM_NAME, Records=records)

    failed = response.get("FailedRecordCount", 0)
    result = {
        "stream": STREAM_NAME,
        "requested": NUM_RECORDS,
        "failed": failed,
        "sample": records[0]["Data"].decode("utf-8"),
    }
    print(json.dumps(result))
    return result
