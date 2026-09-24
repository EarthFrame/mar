import os
import io
import pytest
import http.server
import threading
import urllib.request
import pymar
from pymar import RemoteArchive, RemoteRangeReader

class RangeRequestHandler(http.server.SimpleHTTPRequestHandler):
    """HTTP Request Handler supporting 206 Partial Content Range requests."""
    requested_ranges = []

    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        path = self.translate_path(self.path)
        if os.path.exists(path):
            self.send_header("Content-Length", str(os.path.getsize(path)))
            self.send_header("ETag", "test-etag-123")
            self.send_header("Accept-Ranges", "bytes")
        self.end_headers()

    def do_GET(self):
        path = self.translate_path(self.path)
        if not os.path.exists(path):
            self.send_error(404, "File not found")
            return

        file_size = os.path.getsize(path)
        range_header = self.headers.get("Range")

        if range_header:
            RangeRequestHandler.requested_ranges.append(range_header)
            prefix, range_spec = range_header.split("=")
            start_str, end_str = range_spec.split("-")
            start = int(start_str)
            end = int(end_str) if end_str else file_size - 1
            length = end - start + 1

            self.send_response(206)
            self.send_header("Content-Range", f"bytes {start}-{end}/{file_size}")
            self.send_header("Content-Length", str(length))
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("ETag", "test-etag-123")
            self.end_headers()

            with open(path, "rb") as f:
                f.seek(start)
                self.wfile.write(f.read(length))
        else:
            self.send_response(200)
            self.send_header("Content-Length", str(file_size))
            self.send_header("ETag", "test-etag-123")
            self.end_headers()
            with open(path, "rb") as f:
                self.wfile.write(f.read())

@pytest.fixture(scope="module")
def http_server(tmp_path_factory):
    tmp_dir = tmp_path_factory.mktemp("remote_mar")
    
    # Create an archive with multiple files
    mar_path = str(tmp_dir / "sample.mar")
    opts = pymar._mar.WriteOptions()
    writer = pymar._mar.MarWriter(mar_path, opts)
    writer.add_memory("doc1.txt", b"First file content with some repetitive text to compress." * 50)
    writer.add_memory("doc2.txt", b"Second file content with different data!" * 50)
    writer.add_memory("doc3.txt", b"Third file payload data." * 100)
    writer.finish()

    # Serve directory over HTTP
    handler = lambda *args, **kwargs: RangeRequestHandler(*args, directory=str(tmp_dir), **kwargs)
    server = http.server.HTTPServer(("127.0.0.1", 0), handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    yield f"http://127.0.0.1:{port}/sample.mar"
    server.shutdown()

def test_remote_two_read_index_retrieval(http_server):
    RangeRequestHandler.requested_ranges.clear()

    # Open remote archive
    remote = pymar.open(http_server)
    
    # Verify index contains all 3 files
    files = remote.list_files()
    assert "doc1.txt" in files
    assert "doc2.txt" in files
    assert "doc3.txt" in files

    # Verify Read 1 (header, bytes=0-47) and Read 2 (metadata range)
    assert len(RangeRequestHandler.requested_ranges) == 2
    assert "bytes=0-47" in RangeRequestHandler.requested_ranges[0]

def test_remote_selective_block_streaming(http_server):
    RangeRequestHandler.requested_ranges.clear()

    remote = pymar.open(http_server)
    initial_reads = len(RangeRequestHandler.requested_ranges)

    # Read ONLY doc2.txt
    content2 = remote.read_file("doc2.txt")
    assert b"Second file content" in content2

    # Egress check: Ensure we only downloaded the requested file's blocks, NOT the whole archive
    assert remote.client.bytes_transferred < 100000

    # Read again: verify cached without additional requests
    cached_reads = remote.client.read_count
    content2_again = remote["doc2.txt"]
    assert content2 == content2_again
    assert remote.client.read_count == cached_reads
