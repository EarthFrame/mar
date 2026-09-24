import os
import io
import tarfile
from typing import Optional, Callable
from .core import MarArchive, create_archive
from . import _mar

def from_tar(tar_path: str, mar_path: str, compression: str = "zstd", verbose: bool = False, chunk_size: int = 4 * 1024 * 1024):
    """
    Convert a .tar archive into a high-performance .mar archive without untarring to disk.
    Preserves exact file structure, names, and bytes.
    """
    opts = _mar.WriteOptions()
    if compression == "zstd":
        opts.compression = _mar.CompressionAlgo.ZSTD
    elif compression == "lz4":
        opts.compression = _mar.CompressionAlgo.LZ4
    elif compression == "gzip":
        opts.compression = _mar.CompressionAlgo.GZIP
    elif compression == "bzip2":
        opts.compression = _mar.CompressionAlgo.BZIP2
    elif compression == "none":
        opts.compression = _mar.CompressionAlgo.NONE

    writer = _mar.MarWriter(mar_path, opts)

    with tarfile.open(tar_path, "r:*") as tar:
        for member in tar:
            if member.isfile():
                f = tar.extractfile(member)
                if f is not None:
                    data = f.read()
                    writer.add_memory(member.name, data)
            elif member.isdir():
                writer.add_directory(member.name)
            elif member.issym():
                writer.add_symlink(member.name, member.linkname)

    writer.finish()

def to_tar(mar_path: str, tar_path: str):
    """
    Export a .mar archive back to a standard .tar file.
    """
    archive = MarArchive(mar_path)
    with tarfile.open(tar_path, "w") as tar:
        for name in archive.list_files():
            info = archive.get_file_info(name)
            if not info:
                continue
            if info.type == "file":
                data = archive.read_file(name)
                ti = tarfile.TarInfo(name=name)
                ti.size = len(data)
                tar.addfile(ti, io.BytesIO(data))
            elif info.type == "directory":
                ti = tarfile.TarInfo(name=name)
                ti.type = tarfile.DIRTYPE
                tar.addfile(ti)
            elif info.type == "directory":
                ti = tarfile.TarInfo(name=name)
                ti.type = tarfile.DIRTYPE
                tar.addfile(ti)
