import os
import pytest
import shutil
import pymar
from pymar import _mar
from pymar.core import MarArchive, create_archive, index_archive, get_hash, get_version
from pymar.tools import (
    mar_create, mar_index, mar_list, mar_get, mar_search, 
    mar_hash, mar_validate, mar_header, mar_version
)

@pytest.fixture
def test_data(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "file1.txt").write_text("Hello World!")
    (data_dir / "file2.txt").write_text("MAR format is great.")
    sub_dir = data_dir / "subdir"
    sub_dir.mkdir()
    (sub_dir / "file3.txt").write_text("Nested file content.")
    return data_dir

@pytest.fixture
def mar_file(test_data, tmp_path):
    path = str(tmp_path / "test.mar")
    mar_create(path, [str(test_data)])
    return path

def test_version():
    assert mar_version() == _mar.VERSION
    assert get_version() == _mar.VERSION

def test_create_and_list(test_data, tmp_path):
    path = str(tmp_path / "create_test.mar")
    msg = mar_create(path, [str(test_data)])
    assert "Successfully created" in msg
    assert os.path.exists(path)
    
    files = mar_list(path)
    assert any("file1.txt" in f for f in files)
    assert any("file2.txt" in f for f in files)
    assert any("subdir/file3.txt" in f for f in files)

def test_mar_get(mar_file):
    files = mar_list(mar_file)
    file1_name = [f for f in files if "file1.txt" in f][0]
    
    content = mar_get(mar_file, file1_name)
    assert content == "Hello World!"
    
    file2_name = [f for f in files if "file2.txt" in f][0]
    content2 = mar_get(mar_file, file2_name)
    assert content2 == "MAR format is great."

def test_mar_header(mar_file):
    header = mar_header(mar_file)
    assert "version" in header
    assert "files" in header
    assert "blocks" in header
    assert header["files"] >= 3

def test_mar_hash(mar_file):
    h1 = mar_hash(mar_file, "xxhash64")
    assert len(h1) == 16
    h2 = mar_hash(mar_file, "blake3")
    assert len(h2) == 64
    
    with pytest.raises(RuntimeError, match="Unsupported hash algorithm"):
        mar_hash(mar_file, "md5")

def test_mar_validate(mar_file):
    assert mar_validate(mar_file) is True

def test_mar_index_minhash(mar_file, test_data, tmp_path):
    index_path = str(tmp_path / "test.minhash.mai")
    msg = mar_index(mar_file, "minhash", index_path)
    assert "Successfully created minhash index" in msg
    assert os.path.exists(index_path)
    
    query_file = str(test_data / "file1.txt")
    results = mar_search(mar_file, index_path, query_file)
    assert isinstance(results, list)
    assert len(results) > 0
    assert "file1.txt" in results[0]["filename"]

def test_mar_archive_class(mar_file):
    archive = MarArchive(mar_file)
    files = archive.list_files()
    assert len(files) >= 3
    
    file1_name = [f for f in files if "file1.txt" in f][0]
    info = archive.get_file_info(file1_name)
    assert info.name == file1_name
    assert info.size == len("Hello World!")
    assert info.type == "file"
    
    content = archive.read_file(file1_name)
    assert content == b"Hello World!"
    
    header = archive.get_header()
    assert header.files >= 3
    
    assert archive.validate() is True

def test_low_level_writer(tmp_path):
    path = str(tmp_path / "low_level.mar")
    opts = _mar.WriteOptions()
    opts.compression = _mar.CompressionAlgo.LZ4
    writer = _mar.MarWriter(path, opts)
    
    writer.add_memory("mem1.txt", b"Memory content 1")
    writer.add_memory("mem2.txt", b"Memory content 2", mode=0o644)
    writer.add_directory_entry("empty_dir")
    writer.finish()
    
    assert writer.is_finished()
    
    reader = _mar.MarReader(path)
    assert reader.file_count() == 3
    assert reader.read_file("mem1.txt") == b"Memory content 1"
    assert reader.read_file("mem2.txt") == b"Memory content 2"

def test_mai_io(tmp_path):
    mai_path = str(tmp_path / "test.mai")
    writer = _mar.MAIWriter("fake.mar", _mar.MAIIndexType.MINHASH, 0x12345678)
    writer.add_section(1, b"section data")
    writer.write_to_file(mai_path)
    
    reader = _mar.MAIReader.open(mai_path)
    assert reader.header().archive_hash == 0x12345678
    assert reader.header().index_type == int(_mar.MAIIndexType.MINHASH)
    assert reader.has_section(1)
    assert reader.read_section(1) == b"section data"

def test_index_registry():
    types = _mar.list_index_types()
    assert "minhash" in types
    
    indexer = _mar.get_indexer("minhash")
    assert indexer.type_name() == "minhash"
    assert indexer.index_type() == _mar.MAIIndexType.MINHASH

def test_error_handling(tmp_path):
    with pytest.raises(FileNotFoundError):
        MarArchive("non_existent.mar")
    
    path = str(tmp_path / "invalid.mar")
    with open(path, "w") as f:
        f.write("not a mar file")
    
    with pytest.raises(RuntimeError):
        _mar.MarReader(path)

def test_extract(mar_file, tmp_path):
    extract_dir = tmp_path / "extracted"
    archive = MarArchive(mar_file)
    archive.extract(str(extract_dir))
    
    files = archive.list_files()
    for f in files:
        info = archive.get_file_info(f)
        if info.type == "file":
            assert (extract_dir / f).exists()

def test_create_compressions(test_data, tmp_path):
    for comp in ["zstd", "lz4", "gzip", "bzip2", "none"]:
        path = str(tmp_path / f"test_{comp}.mar")
        mar_create(path, [str(test_data)], compression=comp)
        assert os.path.exists(path)
        archive = MarArchive(path)
        assert len(archive.list_files()) >= 3
        files = archive.list_files()
        f1 = [f for f in files if "file1.txt" in f][0]
        assert archive.read_file(f1) == b"Hello World!"

def test_binary_data(tmp_path):
    path = str(tmp_path / "binary.mar")
    bin_file = tmp_path / "bin.dat"
    bin_data = bytes(range(256))
    bin_file.write_bytes(bin_data)
    
    mar_create(path, [str(bin_file)])
    
    content = mar_get(path, "bin.dat")
    assert "<Binary data: 256 bytes>" in content
    
    archive = MarArchive(path)
    assert archive.read_file("bin.dat") == bin_data

def test_extract_specific_files(mar_file, tmp_path):
    extract_dir = tmp_path / "extracted_specific"
    archive = MarArchive(mar_file)
    files = archive.list_files()
    f1 = [f for f in files if "file1.txt" in f][0]
    
    archive.extract(str(extract_dir), files=[f1])
    assert (extract_dir / f1).exists()
    # file2.txt should not be extracted
    file2_name = [f for f in files if "file2.txt" in f][0]
    assert not (extract_dir / file2_name).exists()

def test_file_info_non_existent(mar_file):
    archive = MarArchive(mar_file)
    assert archive.get_file_info("non_existent.txt") is None

def test_read_directory_error(mar_file):
    archive = MarArchive(mar_file)
    files = archive.list_files()
    dirs = [f for f in files if archive.get_file_info(f).type == "directory"]
    if dirs:
        with pytest.raises(RuntimeError, match="Not a regular file"):
            archive.read_file(dirs[0])

def test_symlinks(tmp_path):
    if os.name == 'nt':
        pytest.skip("Symlinks not supported on Windows in this test")
        
    path = str(tmp_path / "symlink.mar")
    opts = _mar.WriteOptions()
    writer = _mar.MarWriter(path, opts)
    writer.add_memory("target.txt", b"target content")
    writer.add_symlink("link.txt", "target.txt")
    writer.finish()
    
    archive = MarArchive(path)
    info = archive.get_file_info("link.txt")
    assert info.type == "symlink"
    
    with pytest.raises(RuntimeError, match="Not a regular file"):
        archive.read_file("link.txt")

def test_mar_index_bm25(mar_file, tmp_path):
    index_path = str(tmp_path / "test.bm25.mai")
    if "bm25" in _mar.list_index_types():
        msg = mar_index(mar_file, "bm25", index_path)
        assert "Successfully created bm25 index" in msg
        assert os.path.exists(index_path)
        
        with pytest.raises(RuntimeError, match="No searcher available for this index type"):
            mar_search(mar_file, index_path, "Hello")

if __name__ == "__main__":
    pytest.main([__file__])
