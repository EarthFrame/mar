from typing import List, Optional, Dict, Any
from .core import (
    MarArchive,
    create_archive,
    slice_archive,
    get_hash,
    get_version,
    get_spec_version,
    get_tool_version,
    FileInfo,
    HeaderInfo
)

def mar_create(path: str, files: List[str], compression: str = "zstd", verbose: int = 0, **kwargs) -> str:
    """
    Create a new MAR archive.
    
    Args:
        path: Path where the archive will be created (e.g., 'data.mar').
        files: List of files or directories to include in the archive.
        compression: Compression algorithm to use ('zstd', 'lz4', 'gzip', 'bzip2', 'none').
        verbose: Verbosity level (0 = quiet, >0 = verbose).
        **kwargs: Additional options for archive creation.
        
    Returns:
        A success message.
    """
    create_archive(path, files, compression=compression, verbose=verbose, **kwargs)
    return f"Successfully created archive at {path} with {len(files)} inputs."

def mar_slice(
    path: str,
    output_path: str,
    files: Optional[List[str]] = None,
    patterns: Optional[List[str]] = None,
    includes: Optional[List[str]] = None,
    excludes: Optional[List[str]] = None,
    files_from: Optional[str] = None,
    exclude_from: Optional[str] = None,
    compression: str = "zstd",
    threads: int = 0,
    verbose: int = 0,
    force: bool = True,
    **kwargs
) -> str:
    """
    Extract a subset of files from a local or remote archive into a new MAR archive.

    Args:
        path: Path or URL (e.g. s3://bucket/key.mar) of the source archive.
        output_path: Destination path for the new sliced .mar archive.
        files: Optional list of file names or patterns to include.
        patterns: Optional list of positional glob patterns or filenames to include.
        includes: Optional list of glob patterns to include.
        excludes: Optional list of glob patterns to exclude.
        files_from: Optional path to a file containing inclusion patterns (- for stdin).
        exclude_from: Optional path to a file containing exclusion patterns.
        compression: Target compression algorithm ('zstd', 'lz4', 'gzip', 'bzip2', 'none').
        threads: Number of worker threads (0 = auto-detect hardware concurrency).
        verbose: Verbosity level (0 = quiet, >0 = verbose).
        force: Overwrite existing output archive if True.
        **kwargs: Additional options for archive creation.

    Returns:
        A success message with the destination path.
    """
    if patterns:
        files = list(files or []) + list(patterns)
    res = slice_archive(
        path,
        output_path,
        files=files,
        includes=includes,
        excludes=excludes,
        files_from=files_from,
        exclude_from=exclude_from,
        compression=compression,
        threads=threads,
        verbose=verbose,
        force=force,
        **kwargs
    )
    return f"Successfully sliced archive to {res}."

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

def mar_extract(
    path: str,
    output_dir: str,
    files: Optional[List[str]] = None,
    threads: int = 0,
    verbose: int = 0,
) -> str:
    """
    Extract files from a MAR archive into a directory.

    Args:
        path: Path to the .mar archive.
        output_dir: Directory where files should be extracted.
        files: Optional list of specific file names to extract.
        threads: Number of worker threads (0 = auto-detect hardware concurrency).
        verbose: Verbosity level (0 = quiet, >0 = verbose).

    Returns:
        A success message with the destination directory.
    """
    archive = MarArchive(path, verbose=verbose)
    archive.extract(output_dir, files=files, threads=threads, verbose=verbose)
    return f"Successfully extracted {path} to {output_dir}."

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

def mar_validate(path: str, threads: int = 0, verbose: int = 0) -> bool:
    """
    Validate the integrity of a MAR archive.
    
    Args:
        path: Path to the .mar archive.
        threads: Number of worker threads (0 = auto-detect hardware concurrency).
        verbose: Verbosity level (0 = quiet, >0 = verbose).
        
    Returns:
        True if the archive is valid, False otherwise.
    """
    archive = MarArchive(path, verbose=verbose)
    return archive.validate(threads=threads, verbose=verbose)

def mar_header(path: str) -> Dict:
    """
    Get header information and metadata for a MAR archive.
    
    Args:
        path: Path to the .mar archive or remote URL.
        
    Returns:
        A dictionary containing version, file count, and block count.
    """
    if path.startswith(("http://", "https://", "s3://")):
        from .remote import RemoteArchive
        archive = RemoteArchive(path)
    else:
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
