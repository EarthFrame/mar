from typing import List, Optional, Dict, Any
from .core import (
    MarArchive,
    create_archive,
    index_archive,
    get_hash,
    get_version,
    get_spec_version,
    get_tool_version,
    SearchResult,
    FileInfo,
    HeaderInfo
)

def mar_create(path: str, files: List[str], compression: str = "zstd") -> str:
    """
    Create a new MAR archive.
    
    Args:
        path: Path where the archive will be created (e.g., 'data.mar').
        files: List of files or directories to include in the archive.
        compression: Compression algorithm to use ('zstd', 'lz4', 'gzip', 'bzip2', 'none').
        
    Returns:
        A success message.
    """
    create_archive(path, files, compression=compression)
    return f"Successfully created archive at {path} with {len(files)} inputs."

def mar_index(path: str, index_type: str, output_path: Optional[str] = None, **params) -> str:
    """
    Create a sidecar index for a MAR archive.
    
    Args:
        path: Path to the .mar archive.
        index_type: Type of index to create (e.g., 'minhash', 'vector').
        output_path: Optional custom path for the .mai index file.
        **params: Type-specific indexing parameters.
        
    Returns:
        The path to the created index file.
    """
    actual_output = index_archive(path, index_type, output_path, **params)
    return f"Successfully created {index_type} index at {actual_output}."

def mar_list(path: str) -> List[str]:
    """
    List all files contained in a MAR archive.
    
    Args:
        path: Path to the .mar archive.
        
    Returns:
        A list of filenames.
    """
    archive = MarArchive(path)
    return archive.list_files()

def mar_get(path: str, filename: str) -> str:
    """
    Retrieve the contents of a specific file from a MAR archive.
    
    Args:
        path: Path to the .mar archive.
        filename: Name of the file to retrieve.
        
    Returns:
        The file contents as a string (decoded as utf-8 if possible).
    """
    archive = MarArchive(path)
    content = archive.read_file(filename)
    try:
        return content.decode('utf-8')
    except UnicodeDecodeError:
        return f"<Binary data: {len(content)} bytes>"

def mar_extract(path: str, output_dir: str, files: Optional[List[str]] = None, threads: int = 0) -> str:
    """
    Extract files from a MAR archive into a directory.

    Args:
        path: Path to the .mar archive.
        output_dir: Directory where files should be extracted.
        files: Optional list of specific file names to extract.
        threads: Number of worker threads (0 = auto-detect hardware concurrency).

    Returns:
        A success message with the destination directory.
    """
    archive = MarArchive(path)
    archive.extract(output_dir, files=files, threads=threads)
    return f"Successfully extracted {path} to {output_dir}."

def mar_search(path: str, index_path: str, query: str, topk: int = 5, params: Optional[Dict[str, Any]] = None) -> List[Dict]:
    """
    Search a MAR archive using a sidecar index.
    
    Args:
        path: Path to the .mar archive.
        index_path: Path to the .mai index file.
        query: Search query string.
        topk: Number of top results to return.
        params: Optional dictionary of index-specific search parameters.
        
    Returns:
        A list of search results with scores and snippets.
    """
    archive = MarArchive(path)
    search_params = params or {}
    results = archive.search(index_path, query, topk=topk, **search_params)
    return [r.model_dump() for r in results]

def mar_fasta_get(path: str, index_path: str, query: str, file: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Retrieve a FASTA record from an archive using a .fasta.mai index.

    Args:
        path: Path to the .mar archive.
        index_path: Path to the .fasta.mai index file.
        query: Sequence accession ID.
        file: Optional filename within the archive to scope search.

    Returns:
        Dictionary with record details ('id', 'filename', 'seq_len', 'header', 'sequence') or None.
    """
    archive = MarArchive(path)
    return archive.get_fasta_record(index_path, query, file=file)

def mar_hash(path: str, algo: str = "xxhash64") -> str:
    """
    Compute a deterministic hash of a MAR archive.
    
    Args:
        path: Path to the .mar archive.
        algo: Hash algorithm ('xxhash64' or 'blake3').
        
    Returns:
        The computed hash as a hex string.
    """
    return get_hash(path, algo)

def mar_validate(path: str, threads: int = 0) -> bool:
    """
    Validate the integrity of a MAR archive.
    
    Args:
        path: Path to the .mar archive.
        threads: Number of worker threads (0 = auto-detect hardware concurrency).
        
    Returns:
        True if the archive is valid, False otherwise.
    """
    archive = MarArchive(path)
    return archive.validate(threads=threads)

def mar_header(path: str) -> Dict:
    """
    Get header information and metadata for a MAR archive.
    
    Args:
        path: Path to the .mar archive.
        
    Returns:
        A dictionary containing version, file count, and block count.
    """
    archive = MarArchive(path)
    return archive.get_header().model_dump()

def mar_version() -> str:
    """
    Get the version of the MAR tool.
    
    Returns:
        Version string.
    """
    return get_tool_version()

def mar_spec_version() -> str:
    """
    Get the version of the MAR format specification.
    
    Returns:
        Version string.
    """
    return get_spec_version()

def mar_tool_version() -> str:
    """
    Get the version of the MAR tool.
    
    Returns:
        Version string.
    """
    return get_tool_version()
