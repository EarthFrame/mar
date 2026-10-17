import os
from typing import Optional, Union, List
from . import _mar

def from_tar(
    tar_path: Union[str, List[str]],
    mar_path: str,
    compression: str = "zstd",
    verbose: bool = False,
    chunk_size: int = 4 * 1024 * 1024,
    options: Optional[_mar.WriteOptions] = None,
):
    """
    Convert one or more .tar archives (.tar, .tar.gz, .tgz, .tar.zst, .tar.bz2, .tar.lz4)
    into a high-performance .mar archive without untarring to disk and with O(1) bounded memory.
    Preserves exact file structure, names, and bytes.
    """
    if options is None:
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
        opts.block_size = chunk_size
    else:
        opts = options

    paths = [tar_path] if isinstance(tar_path, str) else list(tar_path)
    _mar.import_tar(paths, mar_path, opts)

def to_tar(mar_path: str, tar_path: str):
    """
    Export a .mar archive back to a standard .tar file with O(1) bounded memory.
    """
    _mar.export_tar(mar_path, tar_path)
