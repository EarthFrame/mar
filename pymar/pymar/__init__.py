from .core import (
    MarArchive,
    create_archive,
    index_archive,
    get_hash,
    get_version,
    get_spec_version,
    get_tool_version
)
from .remote import RemoteArchive, RemoteRangeReader
from .tar import from_tar, to_tar
from .torch import MarDataset
from .tools import (
    mar_create,
    mar_index,
    mar_list,
    mar_get,
    mar_extract,
    mar_search,
    mar_fasta_get,
    mar_hash,
    mar_validate,
    mar_header,
    mar_version,
    mar_spec_version,
    mar_tool_version
)

def open(path_or_url: str, **kwargs):
    """
    Open a local or remote MAR archive.
    """
    if path_or_url.startswith(("http://", "https://", "s3://")):
        return RemoteArchive(path_or_url, **kwargs)
    return MarArchive(path_or_url, **kwargs)

__version__ = get_version()
