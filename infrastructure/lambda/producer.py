import json
import os
import random
import time

import boto3

kinesis = boto3.client("kinesis")

STREAM_NAME = os.environ.get("STREAM_NAME", "sample_replay_input_stream")
NUM_RECORDS = int(os.environ.get("NUM_RECORDS", "10"))

# Kinesis PutRecords hard limits: at most 500 records, and at most 5 MiB total,
# per call. NUM_RECORDS is validated at deploy time (CFN MaxValue) to stay at or
# below this, but batching defensively here means the Lambda still behaves
# correctly if invoked directly with a larger NUM_RECORDS override.
KINESIS_PUT_RECORDS_MAX_BATCH = 500

# Bounded retry for partial failures: a 200 response from put_records can
# still contain individual failed records (e.g. throttling), which are
# retried here rather than reported as if the whole batch succeeded.
MAX_RETRIES = 3
INITIAL_BACKOFF_SECONDS = 0.5

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


def _chunk(items, size):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def put_records_with_retry(stream_name, records):
    """
    Sends `records` to `stream_name` via PutRecords, retrying only the entries
    that failed (identified by their position in the response's `Records`
    list), with bounded exponential backoff.

    Returns the total count of records that were never written (0 on full
    success), so the caller can treat a partial write as a terminal failure
    rather than a silent HTTP 200.
    """
    pending = list(records)
    backoff = INITIAL_BACKOFF_SECONDS

    for attempt in range(1, MAX_RETRIES + 1):
        response = kinesis.put_records(StreamName=stream_name, Records=pending)
        failed_count = response.get("FailedRecordCount", 0)

        if failed_count == 0:
            return 0

        # Entries in response["Records"] line up positionally with `pending`.
        # A failed entry has an "ErrorCode"; a succeeded one does not.
        pending = [
            record
            for record, result in zip(pending, response["Records"])
            if "ErrorCode" in result
        ]

        if attempt == MAX_RETRIES:
            break

        time.sleep(backoff)
        backoff *= 2

    return len(pending)


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

    total_failed = 0
    for batch in _chunk(records, KINESIS_PUT_RECORDS_MAX_BATCH):
        total_failed += put_records_with_retry(STREAM_NAME, batch)

    result = {
        "stream": STREAM_NAME,
        "requested": NUM_RECORDS,
        "failed": total_failed,
        # records[0] is accessed unconditionally only when there is a records[0]:
        # a direct invocation can override NUM_RECORDS to 0, in which case
        # `records` is empty and there is no sample to report.
        "sample": records[0]["Data"].decode("utf-8") if records else None,
    }

    if total_failed > 0:
        # Surface a terminal failure instead of returning HTTP 200 with
        # unwritten records: callers should not mistake this response for
        # complete test-data creation.
        print(json.dumps(result))
        raise RuntimeError(
            f"{total_failed} of {NUM_RECORDS} record(s) could not be written to "
            f"'{STREAM_NAME}' after {MAX_RETRIES} attempt(s)."
        )

    print(json.dumps(result))
    return result
