import os
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
