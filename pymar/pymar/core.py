import os
from typing import List, Optional, Dict, Any, Union
from pydantic import BaseModel, Field
from . import _mar

class SearchResult(BaseModel):
    file_id: int
    filename: str
    score: float
    content: str = ""
    metadata: Dict[str, str] = {}

class FileInfo(BaseModel):
    name: str
    size: int
    type: str

class HeaderInfo(BaseModel):
    version: str
    files: int
    blocks: int
    meta_compression: str

class MarArchive:
    """High-level wrapper for a MAR archive."""
    
    def __init__(self, path: str, mode: str = "r"):
        self.path = path
        self.mode = mode
        self._reader = None
        self._writer = None
        
        if mode == "r":
            if not os.path.exists(path):
                raise FileNotFoundError(f"Archive not found: {path}")
            self._reader = _mar.MarReader(path)
        elif mode == "w":
            # Writer is initialized on first add or via create
            pass
        else:
            raise ValueError("Mode must be 'r' or 'w'")

    def list_files(self) -> List[str]:
        """List all filenames in the archive."""
        if not self._reader:
            raise RuntimeError("Archive not open for reading")
        return self._reader.get_names()

    def get_file_info(self, name: str) -> Optional[FileInfo]:
        """Get information about a specific file."""
        if not self._reader:
            raise RuntimeError("Archive not open for reading")
        found = self._reader.find_file(name)
        if not found:
            return None
        idx, entry = found
        
        type_str = "file"
        if entry.entry_type == _mar.EntryType.DIRECTORY:
            type_str = "directory"
        elif entry.entry_type == _mar.EntryType.SYMLINK:
            type_str = "symlink"
            
        return FileInfo(name=name, size=entry.logical_size, type=type_str)

    def read_file(self, name: str) -> bytes:
        """Read file contents from the archive."""
        if not self._reader:
            raise RuntimeError("Archive not open for reading")
        return self._reader.read_file(name)

    def extract(self, output_dir: str, files: Optional[List[str]] = None):
        """Extract files to a directory."""
        if not self._reader:
            raise RuntimeError("Archive not open for reading")
        
        os.makedirs(output_dir, exist_ok=True)
        if files is None:
            files = self.list_files()
            
        for name in files:
            found = self._reader.find_file(name)
            if not found:
                continue
            idx, entry = found
            if entry.entry_type != _mar.EntryType.REGULAR_FILE:
                continue
                
            content = self.read_file(name)
            out_path = os.path.join(output_dir, name)
            os.makedirs(os.path.dirname(out_path), exist_ok=True)
            with open(out_path, "wb") as f:
                f.write(content)

    def get_header(self) -> HeaderInfo:
        """Get archive header information."""
        if not self._reader:
            raise RuntimeError("Archive not open for reading")
        h = self._reader.header()
        return HeaderInfo(
            version=f"{h.version_major}.{h.version_minor}.{h.version_patch}",
            files=self._reader.file_count(),
            blocks=self._reader.block_count(),
            meta_compression=str(h.meta_comp_algo)
        )

    def validate(self, threads: int = 0) -> bool:
        """Validate archive integrity."""
        if not self._reader:
            raise RuntimeError("Archive not open for reading")
        return self._reader.validate_parallel(threads)

    def search(self, index_path: str, query: str, topk: int = 10, **params) -> List[SearchResult]:
        """Search the archive using a sidecar index."""
        opts = _mar.IndexOptions()
        opts.params = {k: str(v) for k, v in params.items()}
        opts.params["topk"] = str(topk)
        
        results = _mar.search(self.path, index_path, query, opts)
        return [SearchResult(
            file_id=r.file_id,
            filename=r.filename,
            score=r.score,
            content=r.content,
            metadata=r.metadata
        ) for r in results]

def create_archive(path: str, files: List[str], compression: str = "zstd", **kwargs):
    """Create a new MAR archive from a list of files."""
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
    
    for k, v in kwargs.items():
        if hasattr(opts, k):
            setattr(opts, k, v)
            
    writer = _mar.MarWriter(path, opts)
    for f in files:
        if os.path.isdir(f):
            writer.add_directory(f)
        else:
            writer.add_file(f, os.path.basename(f))
    writer.finish()

def index_archive(archive_path: str, index_type: str, output_path: Optional[str] = None, **params):
    """Create a sidecar index for an archive."""
    indexer = _mar.get_indexer(index_type)
    if not indexer:
        raise ValueError(f"Unsupported index type: {index_type}")
        
    reader = _mar.MarReader(archive_path)
    
    # Compute archive hash (simplified version of C++ logic)
    # In a real scenario, we might want to use the C++ hash_file function
    archive_hash_hex = _mar.hash_file(archive_path, "xxhash64")
    archive_hash = int(archive_hash_hex, 16)
    
    # MAIWriter needs the enum type
    # We can get it from the indexer
    writer = _mar.MAIWriter(archive_path, indexer.index_type(), archive_hash)
    
    opts = _mar.IndexOptions()
    opts.params = {k: str(v) for k, v in params.items()}
    
    indexer.build(reader, writer, opts)
    
    if output_path is None:
        output_path = f"{archive_path}.{index_type}.mai"
        
    writer.write_to_file(output_path)
    return output_path

def get_version() -> str:
    """Get MAR version."""
    return _mar.VERSION

def get_hash(path: str, algo: str = "xxhash64") -> str:
    """Compute archive hash."""
    return _mar.hash_file(path, algo)
