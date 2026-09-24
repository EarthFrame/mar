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
