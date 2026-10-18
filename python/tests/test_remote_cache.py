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

@pytest.fixture(autouse=True)
def setup_test_cache(tmp_path, monkeypatch):
    test_cache = str(tmp_path / "global_test_cache")
    monkeypatch.setenv("MAR_CACHE_DIR", test_cache)

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


def test_cache_dir_configuration(tmp_path, monkeypatch):
    from pymar import get_cache_dir

    # 1. Default fallback: ~/.cache/earthframe/mar
    monkeypatch.delenv("MAR_CACHE_DIR", raising=False)
    monkeypatch.delenv("XDG_CACHE_HOME", raising=False)
    expected_default = os.path.expanduser("~/.cache/earthframe/mar")
    assert get_cache_dir() == expected_default

    # 2. XDG_CACHE_HOME fallback
    xdg_dir = str(tmp_path / "xdg_cache")
    monkeypatch.setenv("XDG_CACHE_HOME", xdg_dir)
    assert get_cache_dir() == os.path.join(xdg_dir, "earthframe", "mar")

    # 3. MAR_CACHE_DIR environment variable override
    env_dir = str(tmp_path / "custom_env_cache")
    monkeypatch.setenv("MAR_CACHE_DIR", env_dir)
    assert get_cache_dir() == env_dir

    # 4. Explicit parameter overrides environment variable
    explicit_dir = str(tmp_path / "explicit_cache")
    assert get_cache_dir(explicit_dir) == explicit_dir

    # 5. IndexCacheManager respects explicit and env config
    from pymar import _mar
    mgr_explicit = _mar.IndexCacheManager(explicit_dir)
    assert mgr_explicit.cache_dir() == explicit_dir

    mgr_env = _mar.IndexCacheManager()
    assert mgr_env.cache_dir() == env_dir


def test_hdr_cache_and_archive_destinations(http_server, tmp_path, monkeypatch):
    """
    Verify:
    1. The cache directory contains ONLY .hdr files (only 2-read index metadata, no .mar archives or data blocks).
    2. MAR archives are written to specified directory or current working directory.
    3. download() / materialize() fetches all blocks into the archive.
    """
    cache_dir = str(tmp_path / "hdr_cache")
    archive_out_dir = str(tmp_path / "archives")
    os.makedirs(archive_out_dir, exist_ok=True)

    # 1. Open remote archive with specific archive_dir
    remote = pymar.RemoteArchive(http_server, cache_dir=cache_dir, archive_dir=archive_out_dir)
    assert os.path.dirname(remote.archive_path) == os.path.abspath(archive_out_dir)
    assert os.path.basename(remote.archive_path) == "sample.mar"
    assert os.path.exists(remote.archive_path)

    # Read a file to populate block data into archive
    data = remote.read_file("doc1.txt")
    assert b"First file content" in data

    # Verify cache directory contains ONLY .hdr files and NO .mar archives
    cache_files = os.listdir(cache_dir)
    assert len(cache_files) > 0
    for f in cache_files:
        assert f.endswith(".hdr"), f"Found non-.hdr file in cache: {f}"
        assert not f.endswith(".mar"), f"Found .mar file in cache: {f}"

    # 2. Open with custom archive_path
    custom_archive_file = str(tmp_path / "custom_named.mar")
    remote2 = pymar.RemoteArchive(http_server, cache_dir=cache_dir, archive_path=custom_archive_file)
    assert remote2.archive_path == os.path.abspath(custom_archive_file)
    assert os.path.exists(custom_archive_file)
    remote2.download()
    assert os.path.exists(custom_archive_file)
    assert os.path.getsize(custom_archive_file) > 0
    # Validate the materialized archive integrity
    assert pymar.mar_validate(custom_archive_file)

    # 3. Default to current working directory if neither is specified
    work_dir = str(tmp_path / "cwd_work")
    os.makedirs(work_dir, exist_ok=True)
    monkeypatch.chdir(work_dir)

    remote3 = pymar.open(http_server, cache_dir=cache_dir)
    assert remote3.archive_path == os.path.join(work_dir, "sample.mar")
    assert os.path.exists(os.path.join(work_dir, "sample.mar"))

    # Verify that cache dir still strictly has only .hdr files
    for f in os.listdir(cache_dir):
        assert f.endswith(".hdr"), f"Found non-.hdr file in cache: {f}"
        assert not f.endswith(".mar"), f"Found .mar file in cache: {f}"


def test_remote_worker_clamping_and_stats():
    from pymar.remote import clamp_workers, format_bytes
    # Test worker clamping
    assert clamp_workers(0) == 8
    assert clamp_workers(-5) == 8
    assert clamp_workers(4) == 4
    assert clamp_workers(32) == 32
    assert clamp_workers(100) == 32
    assert clamp_workers(100, max_limit=16) == 16

    # Test format bytes
    assert format_bytes(500) == "500 B"
    assert format_bytes(2048) == "2.0 KB"
    assert format_bytes(1048576 * 5) == "5.00 MB"


def test_remote_logging_and_full_archive_warning(http_server, tmp_path, capsys):
    cache_dir = str(tmp_path / "warn_cache")
    out_archive = str(tmp_path / "out_warn.mar")

    # Slice with verbose=1 and NO filter
    remote = pymar.RemoteArchive(http_server, cache_dir=cache_dir, verbose=1)
    assert remote.header_downloaded is True
    assert remote.header_size == 48
    assert remote.total_header_size >= 48
    remote.slice(out_archive, threads=2, verbose=1)
    remote.close()

    captured = capsys.readouterr()
    err = captured.err
    assert "mar: inspecting remote archive" in err
    assert "mar: downloaded mar header and index" in err
    assert "warning: no filter specified; slicing all" in err
    assert "range requests" in err
    assert "transferred" in err
    assert os.path.exists(out_archive)

    # Second open: cache hit, header_downloaded should be False
    remote_cached = pymar.RemoteArchive(http_server, cache_dir=cache_dir, verbose=1)
    assert remote_cached.header_downloaded is False
    assert remote_cached.header_size == 48
    assert remote_cached.total_header_size == remote.total_header_size
    remote_cached.close()

    captured2 = capsys.readouterr()
    assert "mar: cache hit" in captured2.err
    assert "mar: downloaded mar header and index" not in captured2.err

    # Test mar_header on remote URL
    hdr = pymar.mar_header(http_server)
    assert "files" in hdr
    assert "blocks" in hdr
    assert "version" in hdr


def test_remote_slice_unfiltered_kwargs(http_server, tmp_path):
    """Verify that passing verbose or threads to slice() does not fail on WriteOptions."""
    out_archive = str(tmp_path / "sliced_kwargs.mar")
    remote = pymar.RemoteArchive(http_server, cache_dir=str(tmp_path / "cache"))
    # Should not raise AttributeError or TypeError on verbose/threads in opts
    remote.slice(out_archive, verbose=1, threads=2)
    assert os.path.exists(out_archive)
    assert pymar.mar_validate(out_archive)
    remote.close()




