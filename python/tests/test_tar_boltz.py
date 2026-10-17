import os
import io
import tarfile
import pickle
import pytest
import pymar
from pymar import from_tar, to_tar, MarDataset

def test_from_tar_and_to_tar(tmp_path):
    tar_path = str(tmp_path / "test.tar")
    mar_path = str(tmp_path / "test.mar")
    tar_out = str(tmp_path / "out.tar")

    # Create dummy tar file
    with tarfile.open(tar_path, "w") as tar:
        for name in ["mols/ATP.pkl", "mols/ALA.pkl", "mols/GTP.pkl"]:
            data = pickle.dumps({"id": name, "atoms": 42})
            ti = tarfile.TarInfo(name=name)
            ti.size = len(data)
            tar.addfile(ti, io.BytesIO(data))

    # Convert tar -> mar
    from_tar(tar_path, mar_path, compression="zstd")
    assert os.path.exists(mar_path)

    # Open archive and read molecules directly
    archive = pymar.open(mar_path)
    assert "mols/ATP.pkl" in archive
    assert "mols/ALA.pkl" in archive

    atp = pickle.loads(archive["mols/ATP.pkl"])
    assert atp["id"] == "mols/ATP.pkl"
    assert atp["atoms"] == 42

    # Export mar -> tar
    to_tar(mar_path, tar_out)
    assert os.path.exists(tar_out)
    with tarfile.open(tar_out, "r") as tar:
        names = tar.getnames()
        assert "mols/ATP.pkl" in names

def test_from_tar_compressed_and_multiple(tmp_path):
    import gzip
    t1_path = str(tmp_path / "part1.tar.gz")
    t2_path = str(tmp_path / "part2.tar")
    mar_path = str(tmp_path / "merged.mar")

    with tarfile.open(t1_path, "w:gz") as tar:
        data = b"part1_content"
        ti = tarfile.TarInfo(name="part1/data.bin")
        ti.size = len(data)
        tar.addfile(ti, io.BytesIO(data))

    with tarfile.open(t2_path, "w") as tar:
        data = b"part2_content"
        ti = tarfile.TarInfo(name="part2/data.bin")
        ti.size = len(data)
        tar.addfile(ti, io.BytesIO(data))

    # Ingest multiple archives into one MAR
    from_tar([t1_path, t2_path], mar_path, compression="lz4")
    assert os.path.exists(mar_path)

    archive = pymar.open(mar_path)
    assert "part1/data.bin" in archive
    assert "part2/data.bin" in archive
    assert archive["part1/data.bin"] == b"part1_content"
    assert archive["part2/data.bin"] == b"part2_content"

def test_mar_dataset(tmp_path):
    mar_path = str(tmp_path / "dataset.mar")
    opts = pymar._mar.WriteOptions()
    writer = pymar._mar.MarWriter(mar_path, opts)
    for i in range(10):
        writer.add_memory(f"sample_{i}.dat", f"payload_{i}".encode("utf-8"))
    writer.finish()

    dataset = MarDataset(mar_path)
    assert len(dataset) == 10
    name, data = dataset[3]
    assert name == "sample_3.dat"
    assert data == b"payload_3"
