from typing import List, Optional, Dict
from .core import MarArchive, create_archive, index_archive, get_hash, get_version, SearchResult, FileInfo, HeaderInfo

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

def mar_search(path: str, index_path: str, query: str, topk: int = 5) -> List[Dict]:
    """
    Search a MAR archive using a sidecar index.
    
    Args:
        path: Path to the .mar archive.
        index_path: Path to the .mai index file.
        query: Search query string.
        topk: Number of top results to return.
        
    Returns:
        A list of search results with scores and snippets.
    """
    archive = MarArchive(path)
    results = archive.search(index_path, query, topk=topk)
    return [r.model_dump() for r in results]

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

def mar_validate(path: str) -> bool:
    """
    Validate the integrity of a MAR archive.
    
    Args:
        path: Path to the .mar archive.
        
    Returns:
        True if the archive is valid, False otherwise.
    """
    archive = MarArchive(path)
    return archive.validate()

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
    Get the version of the MAR tool and format.
    
    Returns:
        Version string.
    """
    return get_version()
