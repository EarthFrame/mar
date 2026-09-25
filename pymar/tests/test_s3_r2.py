import os
from unittest.mock import MagicMock
import pytest
import pymar
from pymar.remote import RemoteRangeReader

def test_s3_and_r2_url_formats():
    # Test configuring standard S3 / Cloudflare R2 / Backblaze B2 URLs
    s3_url = "https://my-bucket.s3.us-east-1.amazonaws.com/model.mar"
    r2_url = "https://account-id.r2.cloudflarestorage.com/boltz/mols.mar"
    b2_url = "https://s3.us-west-002.backblazeb2.com/datasets/mols.mar"

    reader_s3 = RemoteRangeReader(s3_url, headers={"Authorization": "AWS4-HMAC-SHA256 ..."})
    reader_r2 = RemoteRangeReader(r2_url)
    reader_b2 = RemoteRangeReader(b2_url)

    assert reader_s3.url == s3_url
    assert reader_r2.url == r2_url
    assert reader_b2.url == b2_url
    assert reader_s3.bytes_transferred == 0

def test_s3_uri_parsing():
    mock_s3 = MagicMock()
    reader = RemoteRangeReader(
        "s3://my-bio-bucket/datasets/mols.mar",
        s3_client=mock_s3
    )
    assert reader.is_s3 is True
    assert reader.bucket == "my-bio-bucket"
    assert reader.key == "datasets/mols.mar"
    assert reader.s3_client is mock_s3

def test_s3_fetch_range_and_head():
    mock_s3 = MagicMock()
    mock_body = MagicMock()
    mock_body.read.return_value = b"MAR_BLOCK_DATA_XYZ"
    mock_s3.get_object.return_value = {"Body": mock_body}
    mock_s3.head_object.return_value = {"ETag": '"abc123etag"', "ContentLength": 54321}

    reader = RemoteRangeReader(
        "s3://deep-learning-data/weights.mar",
        s3_client=mock_s3
    )

    # Test HEAD
    hdrs = reader.head()
    assert hdrs.get("ETag") == "abc123etag"
    assert hdrs.get("Content-Length") == "54321"
    mock_s3.head_object.assert_called_once_with(Bucket="deep-learning-data", Key="weights.mar")

    # Test fetch_range [0, 48) -> bytes=0-47
    data = reader.fetch_range(0, 48)
    assert data == b"MAR_BLOCK_DATA_XYZ"
    assert reader.bytes_transferred == len(b"MAR_BLOCK_DATA_XYZ")
    assert reader.read_count == 1
    mock_s3.get_object.assert_called_once_with(
        Bucket="deep-learning-data",
        Key="weights.mar",
        Range="bytes=0-47"
    )

def test_r2_custom_endpoint_initialization():
    mock_s3 = MagicMock()
    reader = RemoteRangeReader(
        "s3://r2-bucket/mols.mar",
        endpoint_url="https://<account-id>.r2.cloudflarestorage.com",
        s3_client=mock_s3
    )
    assert reader.endpoint_url == "https://<account-id>.r2.cloudflarestorage.com"
    assert reader.bucket == "r2-bucket"
    assert reader.key == "mols.mar"
