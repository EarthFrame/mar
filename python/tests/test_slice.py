import os
import sys
import io
import tempfile
import pytest
import http.server
import threading
import subprocess
from pathlib import Path

import pymar
from pymar import MarArchive, RemoteArchive, slice_archive, mar_slice, mar_validate
from pymar.core import glob_match, AlgebraicFilter


def test_glob_match_basics():
    assert glob_match("*.pdb", "protein.pdb")
    assert glob_match("*.pdb", "models/protein.pdb")
    assert not glob_match("*.pdb", "protein.cif")
    assert glob_match("AF-**/*.cif", "AF-1234/model.cif")
    assert glob_match("AF-**/*.cif", "AF-1234/deep/nested/model.cif")
    assert not glob_match("AF-**/*.cif", "OTHER-1234/model.cif")
    assert glob_match("file[0-9].txt", "file5.txt")
    assert not glob_match("file[0-9].txt", "fileA.txt")
    assert glob_match("file[!0-9].txt", "fileA.txt")
    assert glob_match("test?.dat", "test1.dat")
    assert not glob_match("test?.dat", "test12.dat")


def test_algebraic_filter():
    filt = AlgebraicFilter()
    filt.add_include("*.pdb")
    filt.add_exclude("*_bad.pdb")

    all_names = ["1a.pdb", "1a_bad.pdb", "2b.cif", "dir/3c.pdb", "dir/3c_bad.pdb"]
    matched = filt.filter_names(all_names)
    assert matched == ["1a.pdb", "dir/3c.pdb"]

    # Test file list loading
    with tempfile.NamedTemporaryFile("w", delete=False) as f:
        f.write("# comment\nfile1.txt\nfile2.txt\n\n# another\nfile3.txt\n")
        tmp_name = f.name
    try:
        filt2 = AlgebraicFilter()
        filt2.load_includes_from_file(tmp_name)
        assert filt2.filter_names(["file1.txt", "file2.txt", "other.txt"]) == ["file1.txt", "file2.txt"]
    finally:
        os.remove(tmp_name)


def test_local_archive_slice(tmp_path):
    src_archive = str(tmp_path / "source.mar")
    dst_archive = str(tmp_path / "sliced.mar")

    # Create source archive with various files
    opts = pymar._mar.WriteOptions()
    writer = pymar._mar.MarWriter(src_archive, opts)
    writer.add_memory("AF-A001-F1.pdb", b"HEADER PDB 1" * 10)
    writer.add_memory("AF-A001-F1.json", b'{"plddt": 95.0}' * 10)
    writer.add_memory("AF-A002-F1.pdb", b"HEADER PDB 2" * 10)
    writer.add_memory("AF-A002-F1.json", b'{"plddt": 88.0}' * 10)
    writer.add_memory("metadata.txt", b"AlphaFold batch run metadata" * 5)
    writer.finish()

    # Slice out only *.pdb files
    mar_slice(src_archive, dst_archive, patterns=["*.pdb"])

    assert os.path.exists(dst_archive)
    assert mar_validate(dst_archive)

    sliced = MarArchive(dst_archive)
    assert sorted(sliced.get_names()) == ["AF-A001-F1.pdb", "AF-A002-F1.pdb"]
    assert b"HEADER PDB 1" in sliced.read_file("AF-A001-F1.pdb")
    sliced.close()

    # Slice with include and exclude
    dst2 = str(tmp_path / "sliced2.mar")
    slice_archive(src_archive, dst2, includes=["AF-*"], excludes=["*.json", "*A002*"])
    sliced2 = MarArchive(dst2)
    assert sliced2.get_names() == ["AF-A001-F1.pdb"]
    sliced2.close()


class MockHttpRangeHandler(http.server.SimpleHTTPRequestHandler):
    range_requests = []

    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        path = self.translate_path(self.path)
        if os.path.exists(path):
            self.send_header("Content-Length", str(os.path.getsize(path)))
            self.send_header("ETag", "slice-etag-test")
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
            MockHttpRangeHandler.range_requests.append(range_header)
            prefix, range_spec = range_header.split("=")
            start_str, end_str = range_spec.split("-")
            start = int(start_str)
            end = int(end_str) if end_str else file_size - 1
            length = end - start + 1

            self.send_response(206)
            self.send_header("Content-Range", f"bytes {start}-{end}/{file_size}")
            self.send_header("Content-Length", str(length))
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("ETag", "slice-etag-test")
            self.end_headers()

            with open(path, "rb") as f:
                f.seek(start)
                self.wfile.write(f.read(length))
        else:
            self.send_response(200)
            self.send_header("Content-Length", str(file_size))
            self.send_header("ETag", "slice-etag-test")
            self.end_headers()
            with open(path, "rb") as f:
                self.wfile.write(f.read())

    def log_message(self, format, *args):
        pass


@pytest.fixture(scope="module")
def remote_2000_server(tmp_path_factory):
    tmp_dir = tmp_path_factory.mktemp("remote_2000_mar")
    mar_path = str(tmp_dir / "alphafold_huge.mar")

    opts = pymar._mar.WriteOptions()
    opts.block_size = 64 * 1024  # 64KB blocks so 2500 files create multiple blocks
    writer = pymar._mar.MarWriter(mar_path, opts)

    # Add 2500 files
    for i in range(2500):
        name = f"structures/AF-{i:05d}-F1-model.pdb"
        data = f"ATOM  {i:5d}  CA  ALA A   1      11.111  22.222  33.333  1.00 90.00           C\n".encode() * 10
        writer.add_memory(name, data)
    writer.finish()

    class Handler(MockHttpRangeHandler):
        def translate_path(self, path):
            return mar_path

    server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    url = f"http://127.0.0.1:{port}/alphafold_huge.mar"
    yield url, server, mar_path
    server.shutdown()


def test_remote_2000_file_batch_slice(remote_2000_server, tmp_path):
    url, server, local_src = remote_2000_server
    MockHttpRangeHandler.range_requests.clear()

    # Create target list of 2000 files
    target_names = [f"structures/AF-{i:05d}-F1-model.pdb" for i in range(2000)]
    target_file = str(tmp_path / "targets_2000.txt")
    with open(target_file, "w") as f:
        for name in target_names:
            f.write(f"{name}\n")

    dst_slice = str(tmp_path / "subset_2000.mar")
    cache_dir = str(tmp_path / "remote_cache")

    # Slice via RemoteArchive
    archive = RemoteArchive(url, cache_dir=cache_dir)
    archive.slice(dst_slice, files_from=target_file)
    archive.close()

    assert os.path.exists(dst_slice)
    assert mar_validate(dst_slice)

    sliced = MarArchive(dst_slice)
    assert sliced.file_count == 2000
    sliced_names = set(sliced.get_names())
    assert len(sliced_names) == 2000
    assert "structures/AF-00000-F1-model.pdb" in sliced_names
    assert "structures/AF-01999-F1-model.pdb" in sliced_names
    assert "structures/AF-02000-F1-model.pdb" not in sliced_names
    # Verify content of first and last
    data0 = sliced.read_file("structures/AF-00000-F1-model.pdb")
    assert b"ATOM      0  CA  ALA" in data0
    sliced.close()

    # Verify that range request coalescing saved massive roundtrips:
    # Instead of fetching per-file (2 requests * 2000 = 4000 GET requests),
    # the coalesced batch requests should be much fewer (e.g. < 50 requests).
    assert len(MockHttpRangeHandler.range_requests) < 50


def test_cli_slice_local(tmp_path):
    src = str(tmp_path / "cli_src.mar")
    dst = str(tmp_path / "cli_dst.mar")

    opts = pymar._mar.WriteOptions()
    writer = pymar._mar.MarWriter(src, opts)
    writer.add_memory("f1.txt", b"hello world")
    writer.add_memory("f2.pdb", b"pdb content")
    writer.add_memory("f3.json", b'{"key": "val"}')
    writer.finish()

    # Run python -m pymar slice
    res = subprocess.run(
        [sys.executable, "-m", "pymar", "slice", src, "-o", dst, "-i", "*.pdb", "-i", "*.txt"],
        capture_output=True,
        text=True,
    )
    assert res.returncode == 0, f"stdout: {res.stdout}, stderr: {res.stderr}"
    assert os.path.exists(dst)
    out_arc = MarArchive(dst)
    assert sorted(out_arc.get_names()) == ["f1.txt", "f2.pdb"]
    out_arc.close()
