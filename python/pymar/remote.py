import os
import io
import sys
import time
import ssl
import urllib.request
import urllib.error
from typing import Dict, List, Optional, Tuple, Any, Callable, Union

try:
    import certifi
    _DEFAULT_SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())
except ImportError:
    try:
        _DEFAULT_SSL_CONTEXT = ssl.create_default_context()
    except Exception:
        _DEFAULT_SSL_CONTEXT = None

try:
    import boto3
except ImportError:
    boto3 = None

from . import _mar
from .core import get_cache_dir, format_bytes

def clamp_workers(threads: int, max_limit: int = 32, default_workers: int = 8) -> int:
    """
    Sanitize and bound worker thread count for remote operations.
    Ensures concurrency is between 1 and max_limit, defaulting sensibly.
    """
    if threads <= 0:
        return default_workers
    return max(1, min(threads, max_limit))

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
        ssl_context: Optional[ssl.SSLContext] = None,
        verify_ssl: bool = True,
        **s3_kwargs
    ):
        self.url = url
        self.headers = {"User-Agent": "pymar/0.3.0", **(headers or {})}
        self.endpoint_url = endpoint_url
        self.region_name = region_name
        self.bytes_transferred = 0
        self.read_count = 0
        self.time_spent = 0.0

        if not verify_ssl:
            self.ssl_context = ssl._create_unverified_context()
        elif ssl_context is not None:
            self.ssl_context = ssl_context
        else:
            self.ssl_context = _DEFAULT_SSL_CONTEXT

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

        t0 = time.perf_counter()
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
                elapsed = time.perf_counter() - t0
                self.bytes_transferred += len(data)
                self.read_count += 1
                self.time_spent += elapsed
                return data
            except Exception as e:
                raise RuntimeError(f"S3 range request failed for {self.url} [{start}..{end}): {e}") from e

        req = urllib.request.Request(self.url, headers={
            **self.headers,
            "Range": f"bytes={start}-{end - 1}"
        })
        try:
            with urllib.request.urlopen(req, context=self.ssl_context) as resp:
                data = resp.read()
                elapsed = time.perf_counter() - t0
                self.bytes_transferred += len(data)
                self.read_count += 1
                self.time_spent += elapsed
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
            with urllib.request.urlopen(req, context=self.ssl_context) as resp:
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
        archive_path: Optional[str] = None,
        archive_dir: Optional[str] = None,
        persist: bool = True,
        headers: Optional[Dict[str, str]] = None,
        cache_dir: Optional[str] = None,
        verbose: int = 0,
        endpoint_url: Optional[str] = None,
        region_name: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        profile_name: Optional[str] = None,
        s3_client: Optional[Any] = None,
        ssl_context: Optional[ssl.SSLContext] = None,
        verify_ssl: bool = True,
        **s3_kwargs
    ):
        self.url = url
        self.persist = persist
        self.verbose = verbose
        self._temp_archive_path: Optional[str] = None
        self._reader: Optional[_mar.MarReader] = None
        self._block_cache: Dict[int, bytes] = {}
        self._block_offsets: List[int] = []
        self._block_sizes: List[int] = []
        self.header_size: int = 0
        self.metadata_size: int = 0
        self.total_header_size: int = 0
        self.header_downloaded: bool = False

        # Resolve destination path for the MAR archive
        if archive_path is not None:
            self.archive_path = os.path.abspath(os.path.expanduser(archive_path))
        else:
            default_name = self._extract_filename(url)
            if archive_dir is not None:
                target_dir = os.path.abspath(os.path.expanduser(archive_dir))
            else:
                target_dir = os.getcwd()
            self.archive_path = os.path.join(target_dir, default_name)

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
            ssl_context=ssl_context,
            verify_ssl=verify_ssl,
            **s3_kwargs
        )
        self.cache_dir = get_cache_dir(cache_dir)
        try:
            os.makedirs(self.cache_dir, exist_ok=True)
        except OSError:
            pass
        self.cache_mgr = _mar.IndexCacheManager(self.cache_dir)
        self._init_archive()

    @staticmethod
    def _extract_filename(url: str) -> str:
        clean_url = url.split("?")[0].split("#")[0]
        basename = os.path.basename(clean_url.rstrip("/"))
        if not basename or basename == ".":
            return "archive.mar"
        if not basename.endswith(".mar"):
            basename = f"{basename}.mar"
        return basename

    def _init_archive(self):
        # 1. Inspect remote file or check cache
        if self.verbose > 0:
            sys.stderr.write(f"mar: inspecting remote archive {self.url}\n")
        head_headers = self.client.head()
        etag = head_headers.get("ETag", head_headers.get("etag", ""))
        content_length_str = head_headers.get("Content-Length", head_headers.get("content-length", "0"))
        try:
            remote_size = int(content_length_str)
        except (ValueError, TypeError):
            remote_size = 0
        validator = f"{etag}:{content_length_str}"

        cached_bytes = self.cache_mgr.get(self.url, validator) if validator != ":" else None
        
        if cached_bytes is not None:
            self.header_downloaded = False
            # Cache hit: parse metadata directly from cache
            self._load_from_metadata_bytes(cached_bytes)
            if self.verbose > 0:
                sz_str = f", {format_bytes(remote_size)}" if remote_size > 0 else ""
                sys.stderr.write(
                    f"mar: cache hit (index loaded from ~/.cache/mar; {self.file_count:,} files{sz_str}, {len(self._block_offsets):,} blocks)\n"
                )
        else:
            self.header_downloaded = True
            # 2-Read Index Retrieval:
            if self.verbose > 0:
                sz_str = f" (remote size: {format_bytes(remote_size)})" if remote_size > 0 else ""
                sys.stderr.write(f"mar: fetching 2-read index from remote server{sz_str}...\n")

            # Read 1: FixedHeader (bytes 0-47)
            header_bytes = self.client.fetch_range(0, _mar.FixedHeader.FIXED_HEADER_SIZE if hasattr(_mar.FixedHeader, "FIXED_HEADER_SIZE") else 48)
            if len(header_bytes) < 48:
                raise RuntimeError("Failed to read MAR fixed header from remote URL")
            
            # Read 2: Section Container [meta_offset .. meta_offset + meta_stored_size)
            meta_offset = int.from_bytes(header_bytes[16:24], "little")
            meta_stored_size = int.from_bytes(header_bytes[24:32], "little")
            
            meta_container_bytes = self.client.fetch_range(meta_offset, meta_offset + meta_stored_size)
            total_hdr_size = len(header_bytes) + len(meta_container_bytes)
            
            if self.verbose > 0:
                sys.stderr.write(
                    f"mar: downloaded mar header and index ({format_bytes(total_hdr_size)}; fixed header: {len(header_bytes)} B, metadata index: {format_bytes(len(meta_container_bytes))})\n"
                )

            # Combine header + meta container into cached payload
            payload = header_bytes + meta_offset.to_bytes(8, "little") + meta_container_bytes
            if validator != ":":
                try:
                    self.cache_mgr.set(self.url, validator, payload)
                except Exception:
                    pass
            
            self._load_from_payload(header_bytes, meta_offset, meta_container_bytes)
            if self.verbose > 0:
                sys.stderr.write(
                    f"mar: cached index to ~/.cache/mar ({self.file_count:,} files, {len(self._block_offsets):,} blocks)\n"
                )

    def _load_from_metadata_bytes(self, cached: bytes):
        header_bytes = cached[:48]
        meta_offset = int.from_bytes(cached[48:56], "little")
        meta_container = cached[56:]
        self._load_from_payload(header_bytes, meta_offset, meta_container)

    def _load_from_payload(self, header_bytes: bytes, meta_offset: int, meta_container: bytes):
        import tempfile
        self.header_size = len(header_bytes)
        self.metadata_size = len(meta_container)
        self.total_header_size = len(header_bytes) + len(meta_container)

        if self.persist:
            if os.path.dirname(self.archive_path):
                os.makedirs(os.path.dirname(self.archive_path), exist_ok=True)
            self._temp_archive_path = self.archive_path
            # Open and write header + metadata container
            mode = "r+b" if os.path.exists(self._temp_archive_path) else "w+b"
            with open(self._temp_archive_path, mode) as f:
                f.seek(0)
                f.write(header_bytes)
                f.seek(meta_offset)
                f.write(meta_container)
                f.flush()
        else:
            tf = tempfile.NamedTemporaryFile(suffix=".mar", delete=False)
            self._temp_archive_path = tf.name
            tf.write(header_bytes)
            tf.seek(meta_offset)
            tf.write(meta_container)
            tf.flush()
            tf.close()

        # Open with MarReader to parse all section directories, names, spans
        self._reader = _mar.MarReader(self._temp_archive_path)
        self._block_offsets = self._reader.block_offsets()

    def get_header(self):
        """Get archive header information."""
        if not self._reader:
            raise RuntimeError("Archive not open for reading")
        from .core import HeaderInfo
        h = self._reader.header()
        return HeaderInfo(
            version=f"{h.version_major}.{h.version_minor}.{h.version_patch}",
            files=self._reader.file_count(),
            blocks=self._reader.block_count(),
            meta_compression=str(h.meta_comp_algo)
        )

    def list_files(self) -> List[str]:
        if not self._reader:
            return []
        return self._reader.get_names()

    @property
    def names(self) -> List[str]:
        """List all filenames in the remote archive."""
        return self.list_files()

    def get_names(self) -> List[str]:
        """List all filenames in the remote archive."""
        return self.list_files()

    @property
    def file_count(self) -> int:
        """Get the number of files in the remote archive."""
        if not self._reader:
            return 0
        return self._reader.file_count()

    def __len__(self) -> int:
        return self.file_count

    def close(self):
        """Close backing file and reader."""
        self._reader = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

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

    def batch_fetch_blocks(
        self,
        block_ids: Any,
        max_workers: int = 8,
        verbose: Optional[int] = None,
        progress_callback: Optional[Callable[[int, int, int, int], None]] = None
    ) -> None:
        """
        Download multiple blocks concurrently with HTTP range coalescing,
        writing them into the sparse local backing file and updating the reader.
        """
        verb = self.verbose if verbose is None else verbose
        needed_bids = sorted(list(set(b for b in block_ids if b not in self._block_cache)))
        if not needed_bids:
            return

        # Prepare block intervals: (b_id, offset, length)
        block_spans = []
        for b_id in needed_bids:
            if b_id >= len(self._block_offsets):
                continue
            offset = self._block_offsets[b_id]
            info = self._reader.get_block_info(b_id) if hasattr(self._reader, "get_block_info") else None
            if info and info[1] > 0:
                stored_size = info[1]
                block_len = 32 + stored_size
            else:
                if b_id + 1 < len(self._block_offsets):
                    block_len = self._block_offsets[b_id + 1] - offset
                else:
                    block_len = 32
            block_spans.append((b_id, offset, block_len))

        if not block_spans:
            return

        # Coalesce contiguous or near-contiguous blocks (gap <= 4096 bytes)
        coalesced: List[Dict[str, Any]] = []
        for b_id, offset, length in block_spans:
            if coalesced and offset >= coalesced[-1]["end"] and (offset - coalesced[-1]["end"]) <= 4096:
                coalesced[-1]["end"] = max(coalesced[-1]["end"], offset + length)
                coalesced[-1]["blocks"].append((b_id, offset, length))
            else:
                coalesced.append({
                    "start": offset,
                    "end": offset + length,
                    "blocks": [(b_id, offset, length)]
                })

        total_bytes = sum(c["end"] - c["start"] for c in coalesced)
        total_chunks = len(coalesced)
        workers = clamp_workers(max_workers, max_limit=32, default_workers=8)
        workers = min(workers, max(1, total_chunks))

        if verb > 0:
            sys.stderr.write(
                f"mar: coalesced {len(needed_bids):,} blocks into {total_chunks:,} range requests ({format_bytes(total_bytes)} to transfer)\n"
            )
            sys.stderr.write(f"mar: downloading with {workers} worker thread{'s' if workers != 1 else ''}...\n")

        # Fetch ranges concurrently using ThreadPoolExecutor
        import concurrent.futures
        def fetch_chunk(chunk):
            data = self.client.fetch_range(chunk["start"], chunk["end"])
            return chunk, data

        results = []
        completed_chunks = 0
        transferred_bytes = 0
        t_download_start = time.perf_counter()

        def on_chunk_complete(chunk, data):
            nonlocal completed_chunks, transferred_bytes
            completed_chunks += 1
            transferred_bytes += len(data)
            if progress_callback:
                progress_callback(completed_chunks, total_chunks, transferred_bytes, total_bytes)
            if verb > 0:
                pct = (transferred_bytes / total_bytes * 100.0) if total_bytes > 0 else 100.0
                elapsed = time.perf_counter() - t_download_start
                rate = (transferred_bytes / (1024 * 1024) / elapsed) if elapsed > 0.05 else 0.0
                rate_str = f" - {rate:.1f} MB/s" if rate > 0 else ""
                sys.stderr.write(
                    f"\rmar: [batch {completed_chunks}/{total_chunks}] {format_bytes(transferred_bytes)} / {format_bytes(total_bytes)} ({pct:.1f}%){rate_str}"
                )
                if completed_chunks == total_chunks:
                    sys.stderr.write("\n")
                sys.stderr.flush()

        if workers > 1:
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
                futures = [executor.submit(fetch_chunk, c) for c in coalesced]
                for fut in concurrent.futures.as_completed(futures):
                    chunk, data = fut.result()
                    results.append((chunk, data))
                    on_chunk_complete(chunk, data)
        else:
            for c in coalesced:
                chunk, data = fetch_chunk(c)
                results.append((chunk, data))
                on_chunk_complete(chunk, data)

        # Write data to backing sparse file and update block cache
        with open(self._temp_archive_path, "r+b") as f:
            for chunk, data in results:
                f.seek(chunk["start"])
                f.write(data)
                for b_id, b_offset, b_len in chunk["blocks"]:
                    rel_offset = b_offset - chunk["start"]
                    if rel_offset + 32 <= len(data):
                        bhdr = data[rel_offset:rel_offset + 32]
                        stored_sz = int.from_bytes(bhdr[8:16], "little")
                        payload_end = rel_offset + 32 + stored_sz
                        if payload_end <= len(data):
                            self._block_cache[b_id] = data[rel_offset + 32:payload_end]
                        else:
                            self._block_cache[b_id] = data[rel_offset + 32:]
                    else:
                        self._block_cache[b_id] = data[rel_offset:]

        # Reopen reader once for all downloaded blocks
        self._reader = _mar.MarReader(self._temp_archive_path)

    def batch_fetch_files(self, file_names_or_indices: Any, max_workers: int = 8, verbose: Optional[int] = None) -> None:
        """
        Pre-fetch all compressed blocks required for the given list of files.
        Dramatically accelerates slicing or reading large batches of files (e.g. 2000 AlphaFold PDBs).
        """
        needed_blocks = set()
        for item in file_names_or_indices:
            if isinstance(item, str):
                found = self._reader.find_file(item)
                if found:
                    idx = found[0]
                    needed_blocks.update(self._reader.get_block_ids_for_file(idx))
            else:
                needed_blocks.update(self._reader.get_block_ids_for_file(item))

        self.batch_fetch_blocks(needed_blocks, max_workers=max_workers, verbose=verbose)

    def slice(
        self,
        output_path: str,
        files: Optional[List[str]] = None,
        patterns: Optional[List[str]] = None,
        includes: Optional[List[str]] = None,
        excludes: Optional[List[str]] = None,
        files_from: Optional[str] = None,
        exclude_from: Optional[str] = None,
        compression: str = "zstd",
        threads: int = 0,
        verbose: Optional[int] = None,
        force: bool = True,
        **kwargs
    ) -> str:
        """
        Extract a subset of files from the remote archive into a new local archive.
        Optimized for large extractions (e.g. 2,000 AlphaFold PDB files) using parallel
        block pre-fetching and HTTP range coalescing.
        """
        verb = self.verbose if verbose is None else verbose
        t_slice_start = time.perf_counter()
        initial_bytes = self.client.bytes_transferred
        initial_reads = self.client.read_count

        from .core import AlgebraicFilter

        af = AlgebraicFilter()
        if patterns:
            for p in patterns:
                af.add_include(p)
        if files:
            for f in files:
                af.add_include(f)
        if includes:
            for inc in includes:
                af.add_include(inc)
        if excludes:
            for exc in excludes:
                af.add_exclude(exc)
        if files_from:
            af.load_literal_includes_from_file(files_from)
        if exclude_from:
            af.load_literal_excludes_from_file(exclude_from)

        all_names = self.list_files()
        total_files = len(all_names)

        # Warn if no filter was passed, as this slices the entire archive
        if not af.rules:
            if verb > 0:
                sys.stderr.write(
                    f"mar: warning: no filter specified; slicing all {total_files:,} files (full archive)\n"
                )

        matched_names = af.filter_names(all_names)
        if not matched_names:
            raise ValueError("No files in remote archive matched the specified criteria.")

        # Compute total logical size and block count for matched files
        matched_logical_size = 0
        needed_blocks = set()
        for name in matched_names:
            found = self._reader.find_file(name)
            if found:
                idx, entry = found
                matched_logical_size += entry.logical_size
                needed_blocks.update(self._reader.get_block_ids_for_file(idx))

        if verb > 0:
            sys.stderr.write(
                f"mar: matched {len(matched_names):,} of {total_files:,} files ({format_bytes(matched_logical_size)}) requiring {len(needed_blocks):,} blocks\n"
            )

        # Batch prefetch all needed blocks in parallel
        worker_count = clamp_workers(threads, max_limit=32, default_workers=8)
        self.batch_fetch_blocks(needed_blocks, max_workers=worker_count, verbose=verb)

        if verb > 0:
            sys.stderr.write(f"mar: writing destination archive {output_path}...\n")

        # Write to destination archive using MarWriter
        opts = _mar.WriteOptions()
        comp_map = {
            "zstd": _mar.CompressionAlgo.ZSTD,
            "lz4": _mar.CompressionAlgo.LZ4,
            "gzip": _mar.CompressionAlgo.GZIP,
            "bzip2": _mar.CompressionAlgo.BZIP2,
            "none": _mar.CompressionAlgo.NONE,
        }
        if compression in comp_map:
            opts.compression = comp_map[compression]

        if "verbose" in kwargs:
            kwargs.pop("verbose")
        if "threads" in kwargs:
            kwargs.pop("threads")

        for k, v in kwargs.items():
            if hasattr(opts, k):
                setattr(opts, k, v)

        if os.path.exists(output_path) and not force:
            raise FileExistsError(f"Destination archive already exists: {output_path}")

        writer = _mar.MarWriter(output_path, opts)
        for name in matched_names:
            found = self._reader.find_file(name)
            if not found:
                continue
            idx, entry = found
            if entry.entry_type == _mar.EntryType.REGULAR_FILE:
                data = self.read_file(name)
                writer.add_memory(name, data)
            elif entry.entry_type == _mar.EntryType.DIRECTORY:
                writer.add_directory_entry(name)

        writer.finish()

        elapsed_total = time.perf_counter() - t_slice_start
        delta_bytes = self.client.bytes_transferred - initial_bytes
        delta_reads = self.client.read_count - initial_reads
        rate = (delta_bytes / (1024 * 1024) / elapsed_total) if elapsed_total > 0 else 0.0

        if verb > 0:
            sys.stderr.write(
                f"mar: completed in {elapsed_total:.2f}s ({delta_reads} range requests, {format_bytes(delta_bytes)} transferred, {rate:.2f} MB/s)\n"
            )

        return output_path

    def __getitem__(self, name: str) -> bytes:
        return self.read_file(name)

    def __contains__(self, name: str) -> bool:
        return self._reader.find_file(name) is not None if self._reader else False

    def download(self, output_path: Optional[str] = None, directory: Optional[str] = None, max_workers: int = 8, verbose: Optional[int] = None) -> str:
        """
        Download and materialize the complete remote MAR archive to a local file.
        If output_path is provided, writes to output_path.
        If directory is provided, writes to directory/<archive_name>.
        Otherwise, writes to self.archive_path (specified directory or current working directory).
        """
        verb = self.verbose if verbose is None else verbose
        target_path = self.archive_path
        if output_path is not None:
            target_path = os.path.abspath(os.path.expanduser(output_path))
        elif directory is not None:
            target_path = os.path.join(os.path.abspath(os.path.expanduser(directory)), self._extract_filename(self.url))

        # Ensure all blocks are fetched
        total_blocks = len(self._block_offsets)
        if total_blocks > 0:
            worker_count = clamp_workers(max_workers, max_limit=32, default_workers=8)
            self.batch_fetch_blocks(list(range(total_blocks)), max_workers=worker_count, verbose=verb)

        if target_path != self._temp_archive_path:
            import shutil
            if os.path.dirname(target_path):
                os.makedirs(os.path.dirname(target_path), exist_ok=True)
            shutil.copyfile(self._temp_archive_path, target_path)

        return target_path

    def materialize(self, max_workers: int = 8, verbose: Optional[int] = None) -> str:
        """Materialize all blocks in the local archive file."""
        return self.download(max_workers=max_workers, verbose=verbose)

    def __del__(self):
        if not self.persist and self._temp_archive_path and os.path.exists(self._temp_archive_path):
            try:
                os.remove(self._temp_archive_path)
            except Exception:
                pass
