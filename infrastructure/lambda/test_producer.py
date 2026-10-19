"""
Unit tests for the sample producer Lambda (infrastructure/lambda/producer.py).

Covers producer partial-failure handling, retry of partially failed Kinesis
writes, and batching above the PutRecords API limit.

Run with:
    python3 -m unittest infrastructure/lambda/test_producer.py -v

Uses only the standard library (unittest.mock) plus boto3 for type realism in
fake responses, no pytest or moto dependency required.
"""
import importlib
import sys
import unittest
from unittest.mock import MagicMock, patch


def _reload_producer_with_env(num_records: str = "10", stream_name: str = "test-stream"):
    """
    producer.py reads NUM_RECORDS/STREAM_NAME from the environment at import
    time, so each test that needs a specific value reloads the module after
    setting the environment.
    """
    with patch.dict("os.environ", {"NUM_RECORDS": num_records, "STREAM_NAME": stream_name}):
        if "producer" in sys.modules:
            return importlib.reload(sys.modules["producer"])
        return importlib.import_module("producer")


class BuildTripRecordTest(unittest.TestCase):
    def setUp(self):
        sys.path.insert(0, __file__.rsplit("/", 1)[0])
        self.producer = _reload_producer_with_env()

    def test_build_trip_record_matches_deserializer_shape(self):
        record = self.producer.build_trip_record(trip_id=5)
        self.assertEqual(record["tripId"], 5)
        self.assertIn("timestamp", record)
        self.assertIn(record["fromLocationName"], self.producer.FROM_LOCATIONS)
        self.assertIn(record["toLocationName"], self.producer.TO_LOCATIONS)
        self.assertIn(record["category"], self.producer.CATEGORIES)
        # Values must stay inside the bounds JsonDeserializationSchema enforces.
        self.assertGreaterEqual(record["distance"], 0)
        self.assertLessEqual(record["distance"], 10000)
        self.assertGreaterEqual(record["amount"], 0)
        self.assertLessEqual(record["amount"], 100000)


class ChunkTest(unittest.TestCase):
    def setUp(self):
        sys.path.insert(0, __file__.rsplit("/", 1)[0])
        self.producer = _reload_producer_with_env()

    def test_chunk_splits_into_requested_batch_size(self):
        items = list(range(1203))
        batches = list(self.producer._chunk(items, 500))
        self.assertEqual(len(batches), 3)
        self.assertEqual([len(b) for b in batches], [500, 500, 203])

    def test_chunk_handles_fewer_items_than_batch_size(self):
        items = list(range(10))
        batches = list(self.producer._chunk(items, 500))
        self.assertEqual(batches, [items])


class PutRecordsWithRetryTest(unittest.TestCase):
    """Partial Kinesis writes must be retried, bounded, and the caller must
    learn how many records remain unwritten after retries."""

    def setUp(self):
        sys.path.insert(0, __file__.rsplit("/", 1)[0])
        self.producer = _reload_producer_with_env()
        self.producer.INITIAL_BACKOFF_SECONDS = 0  # keep tests fast
        self.records = [{"Data": f"record-{i}".encode(), "PartitionKey": str(i)} for i in range(5)]

    def test_full_success_on_first_attempt_returns_zero_failed(self):
        mock_kinesis = MagicMock()
        mock_kinesis.put_records.return_value = {"FailedRecordCount": 0, "Records": []}

        with patch.object(self.producer, "kinesis", mock_kinesis):
            failed = self.producer.put_records_with_retry("test-stream", self.records)

        self.assertEqual(failed, 0)
        mock_kinesis.put_records.assert_called_once()

    def test_retries_only_failed_records_then_succeeds(self):
        # First attempt: records at index 1 and 3 fail. Second attempt (with
        # only those 2 resent): both succeed.
        first_response = {
            "FailedRecordCount": 2,
            "Records": [
                {"SequenceNumber": "1", "ShardId": "s"},
                {"ErrorCode": "ProvisionedThroughputExceededException"},
                {"SequenceNumber": "2", "ShardId": "s"},
                {"ErrorCode": "InternalFailure"},
                {"SequenceNumber": "3", "ShardId": "s"},
            ],
        }
        second_response = {"FailedRecordCount": 0, "Records": []}

        mock_kinesis = MagicMock()
        mock_kinesis.put_records.side_effect = [first_response, second_response]

        with patch.object(self.producer, "kinesis", mock_kinesis):
            failed = self.producer.put_records_with_retry("test-stream", self.records)

        self.assertEqual(failed, 0)
        self.assertEqual(mock_kinesis.put_records.call_count, 2)
        # The retry call should contain only the 2 records that failed.
        retried_records = mock_kinesis.put_records.call_args_list[1].kwargs["Records"]
        self.assertEqual(len(retried_records), 2)
        self.assertEqual(retried_records, [self.records[1], self.records[3]])

    def test_gives_up_after_max_retries_and_reports_remaining_failures(self):
        always_fails = {
            "FailedRecordCount": 5,
            "Records": [{"ErrorCode": "InternalFailure"} for _ in range(5)],
        }
        mock_kinesis = MagicMock()
        mock_kinesis.put_records.return_value = always_fails

        with patch.object(self.producer, "kinesis", mock_kinesis):
            failed = self.producer.put_records_with_retry("test-stream", self.records)

        self.assertEqual(failed, 5)
        self.assertEqual(mock_kinesis.put_records.call_count, self.producer.MAX_RETRIES)


class LambdaHandlerTest(unittest.TestCase):
    """NUM_RECORDS above the 500 PutRecords limit must still be handled
    correctly by batching. A handler that cannot write all records must
    raise, not return a misleadingly successful result."""

    def setUp(self):
        sys.path.insert(0, __file__.rsplit("/", 1)[0])

    def test_batches_when_num_records_exceeds_kinesis_limit(self):
        producer = _reload_producer_with_env(num_records="1203")
        mock_kinesis = MagicMock()
        mock_kinesis.put_records.return_value = {"FailedRecordCount": 0, "Records": []}

        with patch.object(producer, "kinesis", mock_kinesis):
            result = producer.lambda_handler({}, None)

        self.assertEqual(result["requested"], 1203)
        self.assertEqual(result["failed"], 0)
        # 1203 records at a 500 cap batches into 3 PutRecords calls.
        self.assertEqual(mock_kinesis.put_records.call_count, 3)

    def test_raises_when_records_remain_unwritten_after_retries(self):
        producer = _reload_producer_with_env(num_records="10")
        producer.INITIAL_BACKOFF_SECONDS = 0
        mock_kinesis = MagicMock()
        mock_kinesis.put_records.return_value = {
            "FailedRecordCount": 10,
            "Records": [{"ErrorCode": "InternalFailure"} for _ in range(10)],
        }

        with patch.object(producer, "kinesis", mock_kinesis):
            with self.assertRaises(RuntimeError):
                producer.lambda_handler({}, None)

    def test_returns_success_result_when_all_records_written(self):
        producer = _reload_producer_with_env(num_records="10")
        mock_kinesis = MagicMock()
        mock_kinesis.put_records.return_value = {"FailedRecordCount": 0, "Records": []}

        with patch.object(producer, "kinesis", mock_kinesis):
            result = producer.lambda_handler({}, None)

        self.assertEqual(result["failed"], 0)
        self.assertEqual(result["requested"], 10)
        self.assertIn("sample", result)

    def test_num_records_zero_does_not_raise_indexerror(self):
        # Regression test: records[0] was accessed unconditionally when
        # building the result dict. A direct invocation overriding
        # NUM_RECORDS to 0 left `records` empty, so records[0] raised
        # IndexError before any diagnostics could be returned.
        producer = _reload_producer_with_env(num_records="0")
        mock_kinesis = MagicMock()

        with patch.object(producer, "kinesis", mock_kinesis):
            result = producer.lambda_handler({}, None)

        self.assertEqual(result["requested"], 0)
        self.assertEqual(result["failed"], 0)
        self.assertIsNone(result["sample"])
        # No records means no batches, so put_records should never be called.
        mock_kinesis.put_records.assert_not_called()


if __name__ == "__main__":
    unittest.main()
