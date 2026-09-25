import os
import io
import urllib.request
import urllib.error
from typing import Dict, List, Optional, Tuple, Any

try:
    import boto3
except ImportError:
    boto3 = None

from . import _mar

class RemoteRangeReader:
    """
    Client for reading byte ranges from HTTP(S), AWS S3, Cloudflare R2, or Backblaze B2 URLs.
    Supports S3 boto3 client integration, custom endpoint URLs (R2, VPC endpoints, MinIO),
    range request coalescing, and tracking transfer statistics.
    """
    def __init__(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        endpoint_url: Optional[str] = None,
        region_name: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        profile_name: Optional[str] = None,
        s3_client: Optional[Any] = None,
        **s3_kwargs
    ):
        self.url = url
        self.headers = headers or {}
        self.endpoint_url = endpoint_url
        self.region_name = region_name
        self.bytes_transferred = 0
        self.read_count = 0

        # Parse s3:// URI
        self.is_s3 = url.startswith("s3://")
        self.bucket = ""
        self.key = ""
        if self.is_s3:
            path = url[5:]
            parts = path.split("/", 1)
            self.bucket = parts[0]
            self.key = parts[1] if len(parts) > 1 else ""

        # Initialize S3 client if requested or applicable
        if s3_client is not None:
            self.s3_client = s3_client
        elif self.is_s3 or endpoint_url or aws_access_key_id or region_name:
            if boto3 is None:
                raise ImportError(
                    "boto3 is required for S3 / R2 operations with s3:// URIs or custom endpoints. "
                    "Install it via `pip install boto3` or `pip install 'pymar[s3]'`."
                )
            client_kwargs: Dict[str, Any] = dict(s3_kwargs)
            if endpoint_url:
                client_kwargs["endpoint_url"] = endpoint_url
            if region_name:
                client_kwargs["region_name"] = region_name
            if aws_access_key_id:
                client_kwargs["aws_access_key_id"] = aws_access_key_id
            if aws_secret_access_key:
                client_kwargs["aws_secret_access_key"] = aws_secret_access_key
            if aws_session_token:
                client_kwargs["aws_session_token"] = aws_session_token

            session = boto3.Session(profile_name=profile_name) if profile_name else boto3
            self.s3_client = session.client("s3", **client_kwargs)
        else:
            self.s3_client = None

    def fetch_range(self, start: int, end: int) -> bytes:
        """
        Fetch byte range [start, end) (exclusive of end).
        """
        if start >= end:
            return b""

        if self.s3_client is not None and self.bucket and self.key:
            try:
                # S3 Range header is inclusive: bytes=start-(end-1)
                range_header = f"bytes={start}-{end - 1}"
                resp = self.s3_client.get_object(
                    Bucket=self.bucket,
                    Key=self.key,
                    Range=range_header
                )
                data = resp["Body"].read()
                self.bytes_transferred += len(data)
                self.read_count += 1
                return data
            except Exception as e:
                raise RuntimeError(f"S3 range request failed for {self.url} [{start}..{end}): {e}") from e

        req = urllib.request.Request(self.url, headers={
            **self.headers,
            "Range": f"bytes={start}-{end - 1}"
        })
        try:
            with urllib.request.urlopen(req) as resp:
                data = resp.read()
                self.bytes_transferred += len(data)
                self.read_count += 1
                return data
        except urllib.error.HTTPError as e:
            raise RuntimeError(f"HTTP range request failed for {self.url} [{start}..{end}): {e}") from e

    def head(self) -> Dict[str, str]:
        """Perform HEAD request to inspect headers (ETag, Content-Length, etc)."""
        if self.s3_client is not None and self.bucket and self.key:
            try:
                resp = self.s3_client.head_object(Bucket=self.bucket, Key=self.key)
                etag = resp.get("ETag", "").strip('"')
                content_length = str(resp.get("ContentLength", 0))
                return {"ETag": etag, "Content-Length": content_length}
            except Exception:
                return {}

        req = urllib.request.Request(self.url, headers=self.headers, method="HEAD")
        try:
            with urllib.request.urlopen(req) as resp:
                return dict(resp.headers)
        except urllib.error.HTTPError:
            return {}


class RemoteArchive:
    """
    Random-access remote MAR archive reader with 2-read index retrieval,
    local ~/.cache validation, and selective block streaming.
    Supports HTTP(S), S3 URIs (s3://bucket/key), and custom S3 endpoints
    (Cloudflare R2, AWS VPC endpoints, MinIO).
    """
    def __init__(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        cache_dir: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        region_name: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        profile_name: Optional[str] = None,
        s3_client: Optional[Any] = None,
        **s3_kwargs
    ):
        self.url = url
        self.client = RemoteRangeReader(
            url,
            headers=headers,
            endpoint_url=endpoint_url,
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            profile_name=profile_name,
            s3_client=s3_client,
            **s3_kwargs
        )
        self.cache_mgr = _mar.IndexCacheManager()
        self._reader: Optional[_mar.MarReader] = None
        self._temp_archive_path: Optional[str] = None
        self._block_cache: Dict[int, bytes] = {}
        self._block_offsets: List[int] = []
        self._block_sizes: List[int] = []
        self._init_archive()

    def _init_archive(self):
        # 1. Inspect remote file or check cache
        head_headers = self.client.head()
        etag = head_headers.get("ETag", head_headers.get("etag", ""))
        content_length = head_headers.get("Content-Length", head_headers.get("content-length", "0"))
        validator = f"{etag}:{content_length}"

        cached_bytes = self.cache_mgr.get(self.url, validator) if validator != ":" else None
        
        if cached_bytes is not None:
            # Cache hit: parse metadata directly from cache
            self._load_from_metadata_bytes(cached_bytes)
        else:
            # 2-Read Index Retrieval:
            # Read 1: FixedHeader (bytes 0-47)
            header_bytes = self.client.fetch_range(0, _mar.FixedHeader.FIXED_HEADER_SIZE if hasattr(_mar.FixedHeader, "FIXED_HEADER_SIZE") else 48)
            if len(header_bytes) < 48:
                raise RuntimeError("Failed to read MAR fixed header from remote URL")
            
            # Read 2: Section Container [meta_offset .. meta_offset + meta_stored_size)
            meta_offset = int.from_bytes(header_bytes[16:24], "little")
            meta_stored_size = int.from_bytes(header_bytes[24:32], "little")
            
            meta_container_bytes = self.client.fetch_range(meta_offset, meta_offset + meta_stored_size)
            
            # Combine header + meta container into cached payload
            payload = header_bytes + meta_offset.to_bytes(8, "little") + meta_container_bytes
            if validator != ":":
                try:
                    self.cache_mgr.set(self.url, validator, payload)
                except Exception:
                    pass
            
            self._load_from_payload(header_bytes, meta_offset, meta_container_bytes)

    def _load_from_metadata_bytes(self, cached: bytes):
        header_bytes = cached[:48]
        meta_offset = int.from_bytes(cached[48:56], "little")
        meta_container = cached[56:]
        self._load_from_payload(header_bytes, meta_offset, meta_container)

    def _load_from_payload(self, header_bytes: bytes, meta_offset: int, meta_container: bytes):
        import tempfile
        # Create a sparse/header-stub file so MarReader can open and parse all sections
        tf = tempfile.NamedTemporaryFile(suffix=".mar", delete=False)
        self._temp_archive_path = tf.name
        
        # Write header
        tf.write(header_bytes)
        # Pad to meta_offset and write metadata
        tf.seek(meta_offset)
        tf.write(meta_container)
        tf.flush()
        tf.close()

        # Open with MarReader to parse all section directories, names, spans
        self._reader = _mar.MarReader(self._temp_archive_path)
        self._block_offsets = self._reader.block_offsets()

    def list_files(self) -> List[str]:
        if not self._reader:
            return []
        return self._reader.get_names()

    def get_file_info(self, name: str) -> Optional[Any]:
        if not self._reader:
            return None
        found = self._reader.find_file(name)
        if not found:
            return None
        idx, entry = found
        type_str = "file"
        if entry.entry_type == _mar.EntryType.DIRECTORY:
            type_str = "directory"
        elif entry.entry_type == _mar.EntryType.SYMLINK:
            type_str = "symlink"
        return {"name": name, "size": entry.logical_size, "type": type_str, "index": idx}

    def read_file(self, name: str) -> bytes:
        """
        Selective block download: download ONLY the blocks required for this file.
        """
        found = self._reader.find_file(name)
        if not found:
            raise FileNotFoundError(f"File '{name}' not found in remote archive")
        idx, entry = found
        if entry.entry_type != _mar.EntryType.REGULAR_FILE:
            raise RuntimeError(f"Not a regular file: {name}")
        if entry.logical_size == 0:
            return b""

        block_ids = self._reader.get_block_ids_for_file(idx)
        if not block_ids:
            # Fallback to reading from local reader if available
            return self._reader.read_file(name)

        # Download each required block range
        for b_id in block_ids:
            if b_id not in self._block_cache:
                offset = self._block_offsets[b_id]
                # First fetch block header (32 bytes)
                bhdr_bytes = self.client.fetch_range(offset, offset + 32)
                stored_size = int.from_bytes(bhdr_bytes[8:16], "little")
                # Fetch payload
                bpayload = self.client.fetch_range(offset + 32, offset + 32 + stored_size)
                
                # Write to temp archive so MarReader decompression & cache can process it
                with open(self._temp_archive_path, "r+b") as f:
                    f.seek(offset)
                    f.write(bhdr_bytes)
                    f.write(bpayload)
                self._block_cache[b_id] = bpayload

        # Reopen reader to refresh memory-map after block writes
        self._reader = _mar.MarReader(self._temp_archive_path)
        return self._reader.read_file(name)

    def __getitem__(self, name: str) -> bytes:
        return self.read_file(name)

    def __contains__(self, name: str) -> bool:
        return self._reader.find_file(name) is not None if self._reader else False

    def __del__(self):
        if self._temp_archive_path and os.path.exists(self._temp_archive_path):
            try:
                os.remove(self._temp_archive_path)
            except Exception:
                pass
