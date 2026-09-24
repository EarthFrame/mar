#![allow(non_camel_case_types)]
use crate::checksum::*;
use crate::format::*;
use crate::mai::*;
use crate::reader::MarReader;
use crate::remote::IndexCacheManager;
use crate::writer::{MarWriter, WriteOptions};
use pyo3::exceptions::{PyFileNotFoundError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;

#[pyclass(name = "CompressionAlgo", eq, eq_int)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum PyCompressionAlgo {
    NONE = 0,
    GZIP = 1,
    ZSTD = 2,
    LZ4 = 3,
    BZIP2 = 4,
}

#[pyclass(name = "ChecksumType", eq, eq_int)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum PyChecksumType {
    NONE = 0,
    BLAKE3 = 1,
    XXHASH32 = 2,
    CRC32C = 3,
    XXHASH3 = 4,
}

#[pyclass(name = "NameTableFormat", eq, eq_int)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum PyNameTableFormat {
    FRONT_CODED = 0,
    RAW_ARRAY = 1,
    COMPACT_TRIE = 2,
}

#[pyclass(name = "EntryType", eq, eq_int)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum PyEntryType {
    REGULAR_FILE = 0,
    DIRECTORY = 1,
    SYMLINK = 2,
    CHAR_DEVICE = 3,
    BLOCK_DEVICE = 4,
    FIFO = 5,
    SOCKET = 6,
    UNKNOWN = 255,
}

#[pyclass(name = "MAIIndexType", eq, eq_int)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum PyMAIIndexType {
    VECTOR = 1,
    MINHASH = 2,
    GENERIC = 3,
    GENOMIC = 4,
    EMAIL = 5,
    TIMESERIES = 6,
    BM25 = 7,
}

#[pyclass(name = "WriteOptions")]
#[derive(Clone)]
pub struct PyWriteOptions {
    #[pyo3(get, set)]
    pub compression: PyCompressionAlgo,
    #[pyo3(get, set)]
    pub checksum: PyChecksumType,
    #[pyo3(get, set)]
    pub multiblock: bool,
    #[pyo3(get, set)]
    pub block_size: u64,
    #[pyo3(get, set)]
    pub compress_meta: bool,
    #[pyo3(get, set)]
    pub deterministic: bool,
    #[pyo3(get, set)]
    pub include_posix: bool,
    #[pyo3(get, set)]
    pub compute_hashes: bool,
    #[pyo3(get, set)]
    pub dedup_by_hash: bool,
    #[pyo3(get, set)]
    pub compression_level: i32,
    #[pyo3(get, set)]
    pub name_table_format: Option<PyNameTableFormat>,
    #[pyo3(get, set)]
    pub num_threads: usize,
}

#[pymethods]
impl PyWriteOptions {
    #[new]
    fn new() -> Self {
        Self {
            compression: PyCompressionAlgo::ZSTD,
            checksum: PyChecksumType::XXHASH3,
            multiblock: true,
            block_size: DEFAULT_BLOCK_SIZE,
            compress_meta: true,
            deterministic: false,
            include_posix: true,
            compute_hashes: true,
            dedup_by_hash: false,
            compression_level: -1,
            name_table_format: None,
            num_threads: 0,
        }
    }
}

impl From<PyWriteOptions> for WriteOptions {
    fn from(o: PyWriteOptions) -> Self {
        Self {
            compression: match o.compression {
                PyCompressionAlgo::NONE => CompressionAlgo::None,
                PyCompressionAlgo::GZIP => CompressionAlgo::Gzip,
                PyCompressionAlgo::ZSTD => CompressionAlgo::Zstd,
                PyCompressionAlgo::LZ4 => CompressionAlgo::Lz4,
                PyCompressionAlgo::BZIP2 => CompressionAlgo::Bzip2,
            },
            checksum: match o.checksum {
                PyChecksumType::NONE => ChecksumType::None,
                PyChecksumType::BLAKE3 => ChecksumType::Blake3,
                PyChecksumType::XXHASH32 => ChecksumType::XXHash32,
                PyChecksumType::CRC32C => ChecksumType::Crc32c,
                PyChecksumType::XXHASH3 => ChecksumType::XXHash3,
            },
            multiblock: o.multiblock,
            block_size: o.block_size,
            compress_meta: o.compress_meta,
            deterministic: o.deterministic,
            include_posix: o.include_posix,
            compute_hashes: o.compute_hashes,
            dedup_by_hash: o.dedup_by_hash,
            compression_level: o.compression_level,
            name_table_format: o.name_table_format.map(|f| match f {
                PyNameTableFormat::FRONT_CODED => NameTableFormat::FrontCoded,
                PyNameTableFormat::RAW_ARRAY => NameTableFormat::RawArray,
                PyNameTableFormat::COMPACT_TRIE => NameTableFormat::CompactTrie,
            }),
            num_threads: o.num_threads,
            align_log2: DEFAULT_ALIGN_LOG2,
        }
    }
}

#[pyclass(name = "IndexOptions")]
#[derive(Clone, Default)]
pub struct PyIndexOptions {
    #[pyo3(get, set)]
    pub params: HashMap<String, String>,
    #[pyo3(get, set)]
    pub aux_files: Vec<String>,
}

#[pymethods]
impl PyIndexOptions {
    #[new]
    fn new() -> Self {
        Self::default()
    }
}

#[pyclass(name = "SearchResult")]
pub struct PySearchResult {
    #[pyo3(get)]
    pub file_id: usize,
    #[pyo3(get)]
    pub filename: String,
    #[pyo3(get)]
    pub score: f64,
    #[pyo3(get)]
    pub content: String,
    #[pyo3(get)]
    pub metadata: HashMap<String, String>,
}

#[pyclass(name = "FileEntry")]
#[derive(Clone)]
pub struct PyFileEntry {
    #[pyo3(get)]
    pub entry_type: PyEntryType,
    #[pyo3(get)]
    pub logical_size: u64,
}

#[pyclass(name = "FixedHeader")]
pub struct PyFixedHeader {
    #[pyo3(get)]
    pub version_major: u8,
    #[pyo3(get)]
    pub version_minor: u8,
    #[pyo3(get)]
    pub version_patch: u8,
    #[pyo3(get)]
    pub header_size_bytes: u64,
    #[pyo3(get)]
    pub meta_comp_algo: u8,
}

#[pyclass(name = "MarWriter")]
pub struct PyMarWriter {
    inner: MarWriter,
}

#[pymethods]
impl PyMarWriter {
    #[new]
    #[pyo3(signature = (path, options = None))]
    fn new(path: &str, options: Option<PyWriteOptions>) -> Self {
        let opts = options.unwrap_or_else(PyWriteOptions::new).into();
        Self {
            inner: MarWriter::new(path, opts),
        }
    }

    #[pyo3(signature = (path, archive_name = ""))]
    fn add_file(&mut self, path: &str, archive_name: &str) -> PyResult<()> {
        self.inner.add_file(path, archive_name).map_err(PyRuntimeError::new_err)
    }

    #[pyo3(signature = (path, prefix = ""))]
    fn add_directory(&mut self, path: &str, prefix: &str) -> PyResult<()> {
        self.inner.add_directory(path, prefix).map_err(PyRuntimeError::new_err)
    }

    #[pyo3(signature = (name, content, mode = DEFAULT_FILE_MODE, mtime = 0))]
    fn add_memory(&mut self, name: &str, content: &[u8], mode: u32, mtime: i64) {
        self.inner.add_memory(name, content, mode, mtime);
    }

    #[pyo3(signature = (name, mode = DEFAULT_DIR_MODE, mtime = 0))]
    fn add_directory_entry(&mut self, name: &str, mode: u32, mtime: i64) {
        self.inner.add_directory_entry(name, mode, mtime);
    }

    #[pyo3(signature = (name, target, mode = 0o120777, mtime = 0))]
    fn add_symlink(&mut self, name: &str, target: &str, mode: u32, mtime: i64) {
        self.inner.add_symlink(name, target, mode, mtime);
    }

    fn finish(&mut self) -> PyResult<()> {
        self.inner.finish().map_err(PyRuntimeError::new_err)
    }

    fn is_finished(&self) -> bool {
        self.inner.is_finished()
    }
}

#[pyclass(name = "MarReader")]
pub struct PyMarReader {
    inner: MarReader,
}

#[pymethods]
impl PyMarReader {
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        let reader = MarReader::open(path).map_err(|e| {
            if !std::path::Path::new(path).exists() {
                PyFileNotFoundError::new_err(format!("Archive not found: {}", path))
            } else {
                PyRuntimeError::new_err(e)
            }
        })?;
        Ok(Self { inner: reader })
    }

    fn header(&self) -> PyFixedHeader {
        let h = self.inner.header();
        PyFixedHeader {
            version_major: h.version_major,
            version_minor: h.version_minor,
            version_patch: h.version_patch,
            header_size_bytes: h.header_size_bytes,
            meta_comp_algo: h.meta_comp_algo as u8,
        }
    }

    fn file_count(&self) -> usize {
        self.inner.file_count()
    }

    fn block_count(&self) -> usize {
        self.inner.block_count()
    }

    fn get_name(&self, index: usize) -> Option<String> {
        self.inner.get_name(index)
    }

    fn get_file_entry(&self, index: usize) -> Option<PyFileEntry> {
        self.inner.get_file_entry(index).map(|e| PyFileEntry {
            entry_type: match e.entry_type {
                EntryType::RegularFile => PyEntryType::REGULAR_FILE,
                EntryType::Directory => PyEntryType::DIRECTORY,
                EntryType::Symlink => PyEntryType::SYMLINK,
                EntryType::CharDevice => PyEntryType::CHAR_DEVICE,
                EntryType::BlockDevice => PyEntryType::BLOCK_DEVICE,
                EntryType::Fifo => PyEntryType::FIFO,
                EntryType::Socket => PyEntryType::SOCKET,
                EntryType::Unknown => PyEntryType::UNKNOWN,
            },
            logical_size: e.logical_size,
        })
    }

    fn get_names(&self) -> Vec<String> {
        self.inner.get_names().to_vec()
    }

    fn get_file_entries(&self) -> Vec<PyFileEntry> {
        self.inner.get_file_entries().iter().map(|e| PyFileEntry {
            entry_type: match e.entry_type {
                EntryType::RegularFile => PyEntryType::REGULAR_FILE,
                EntryType::Directory => PyEntryType::DIRECTORY,
                EntryType::Symlink => PyEntryType::SYMLINK,
                EntryType::CharDevice => PyEntryType::CHAR_DEVICE,
                EntryType::BlockDevice => PyEntryType::BLOCK_DEVICE,
                EntryType::Fifo => PyEntryType::FIFO,
                EntryType::Socket => PyEntryType::SOCKET,
                EntryType::Unknown => PyEntryType::UNKNOWN,
            },
            logical_size: e.logical_size,
        }).collect()
    }

    fn find_file(&self, name: &str) -> Option<(usize, PyFileEntry)> {
        self.inner.find_file(name).map(|(idx, e)| {
            (
                idx,
                PyFileEntry {
                    entry_type: match e.entry_type {
                        EntryType::RegularFile => PyEntryType::REGULAR_FILE,
                        EntryType::Directory => PyEntryType::DIRECTORY,
                        EntryType::Symlink => PyEntryType::SYMLINK,
                        EntryType::CharDevice => PyEntryType::CHAR_DEVICE,
                        EntryType::BlockDevice => PyEntryType::BLOCK_DEVICE,
                        EntryType::Fifo => PyEntryType::FIFO,
                        EntryType::Socket => PyEntryType::SOCKET,
                        EntryType::Unknown => PyEntryType::UNKNOWN,
                    },
                    logical_size: e.logical_size,
                },
            )
        })
    }

    fn read_file<'py>(&self, py: Python<'py>, arg: &Bound<'_, PyAny>) -> PyResult<Bound<'py, PyBytes>> {
        let bytes_vec = if let Ok(idx) = arg.extract::<usize>() {
            self.inner.read_file_by_index(idx).map_err(PyRuntimeError::new_err)?
        } else if let Ok(name) = arg.extract::<String>() {
            self.inner.read_file_by_name(&name).map_err(PyRuntimeError::new_err)?
        } else {
            return Err(PyValueError::new_err("Argument must be int index or str filename"));
        };
        Ok(PyBytes::new_bound(py, &bytes_vec))
    }

    #[pyo3(signature = (output_dir, files = None, num_threads = 0))]
    fn extract(&self, output_dir: &str, files: Option<Vec<String>>, num_threads: usize) -> PyResult<()> {
        let f_slice = files.as_deref();
        self.inner.extract_parallel(output_dir, f_slice, num_threads).map_err(PyRuntimeError::new_err)
    }

    #[pyo3(signature = (num_threads = 0, verbose = false))]
    fn validate_parallel(&self, num_threads: usize, verbose: bool) -> bool {
        let _ = verbose;
        self.inner.validate_parallel(num_threads)
    }

    fn get_block_ids_for_file(&self, index: usize) -> Vec<u32> {
        self.inner.get_block_ids_for_file(index)
    }

    fn block_offsets(&self) -> Vec<u64> {
        self.inner.block_offsets().to_vec()
    }
}

#[pyclass(name = "MAIWriter")]
pub struct PyMAIWriter {
    inner: MAIWriter,
}

#[pymethods]
impl PyMAIWriter {
    #[new]
    fn new(archive_path: &str, index_type: PyMAIIndexType, archive_hash: u64) -> Self {
        let it = match index_type {
            PyMAIIndexType::VECTOR => MAIIndexType::Vector,
            PyMAIIndexType::MINHASH => MAIIndexType::MinHash,
            PyMAIIndexType::GENERIC => MAIIndexType::Generic,
            PyMAIIndexType::GENOMIC => MAIIndexType::Genomic,
            PyMAIIndexType::EMAIL => MAIIndexType::Email,
            PyMAIIndexType::TIMESERIES => MAIIndexType::TimeSeries,
            PyMAIIndexType::BM25 => MAIIndexType::BM25,
        };
        Self {
            inner: MAIWriter::new(archive_path, it, archive_hash),
        }
    }

    #[pyo3(signature = (section_type, data, flags = 0))]
    fn add_section(&mut self, section_type: u32, data: &[u8], flags: u32) {
        self.inner.add_section(section_type, data.to_vec(), flags);
    }

    #[pyo3(signature = (path, align_log2 = 0))]
    fn write_to_file(&mut self, path: &str, align_log2: u8) -> PyResult<()> {
        self.inner.write_to_file(path, align_log2).map_err(PyRuntimeError::new_err)
    }
}

#[pyclass(name = "MAIFixedHeader")]
pub struct PyMAIFixedHeader {
    #[pyo3(get)]
    pub magic: u32,
    #[pyo3(get)]
    pub version: u8,
    #[pyo3(get)]
    pub index_type: u8,
    #[pyo3(get)]
    pub align_log2: u8,
    #[pyo3(get)]
    pub archive_hash: u64,
    #[pyo3(get)]
    pub archive_name_len: u32,
    #[pyo3(get)]
    pub flags: u32,
    #[pyo3(get)]
    pub timestamp: u64,
    #[pyo3(get)]
    pub index_data_offset: u64,
}

#[pyclass(name = "MAIReader")]
pub struct PyMAIReader {
    inner: MAIReader,
}

#[pymethods]
impl PyMAIReader {
    #[staticmethod]
    fn open(path: &str) -> PyResult<Self> {
        let reader = MAIReader::open(path).ok_or_else(|| PyRuntimeError::new_err(format!("Failed to open index: {}", path)))?;
        Ok(Self { inner: reader })
    }

    fn header(&self) -> PyMAIFixedHeader {
        let h = self.inner.header();
        PyMAIFixedHeader {
            magic: h.magic,
            version: h.version,
            index_type: h.index_type,
            align_log2: h.align_log2,
            archive_hash: h.archive_hash,
            archive_name_len: h.archive_name_len,
            flags: h.flags,
            timestamp: h.timestamp,
            index_data_offset: h.index_data_offset,
        }
    }

    fn archive_name(&self) -> String {
        self.inner.archive_name().to_string()
    }

    fn has_section(&self, section_type: u32) -> bool {
        self.inner.has_section(section_type)
    }

    fn read_section<'py>(&self, py: Python<'py>, section_type: u32) -> Bound<'py, PyBytes> {
        let data = self.inner.read_section(section_type);
        PyBytes::new_bound(py, &data)
    }
}

#[pyclass(name = "Indexer")]
pub struct PyIndexer {
    type_name: String,
    index_type: PyMAIIndexType,
}

#[pymethods]
impl PyIndexer {
    fn type_name(&self) -> &str {
        &self.type_name
    }

    fn index_type(&self) -> PyMAIIndexType {
        self.index_type
    }

    fn build(&self, reader: &PyMarReader, writer: &mut PyMAIWriter, opts: &PyIndexOptions) -> PyResult<()> {
        let rust_opts = IndexOptions {
            params: opts.params.clone(),
            aux_files: opts.aux_files.clone(),
        };
        match self.index_type {
            PyMAIIndexType::MINHASH => {
                build_minhash_index(&reader.inner, &mut writer.inner, &rust_opts).map_err(PyRuntimeError::new_err)
            }
            PyMAIIndexType::BM25 => {
                build_bm25_index(&reader.inner, &mut writer.inner, &rust_opts).map_err(PyRuntimeError::new_err)
            }
            PyMAIIndexType::EMAIL => {
                build_email_index(&reader.inner, &mut writer.inner, &rust_opts).map_err(PyRuntimeError::new_err)
            }
            PyMAIIndexType::TIMESERIES => {
                build_timeseries_index(&reader.inner, &mut writer.inner, &rust_opts).map_err(PyRuntimeError::new_err)
            }
            PyMAIIndexType::GENOMIC => {
                build_genomic_index(&reader.inner, &mut writer.inner, &rust_opts).map_err(PyRuntimeError::new_err)
            }
            _ => Err(PyRuntimeError::new_err(format!("Unsupported indexer type: {}", self.type_name))),
        }
    }
}

#[pyclass(name = "IndexCacheManager")]
pub struct PyIndexCacheManager {
    inner: IndexCacheManager,
}

#[pymethods]
impl PyIndexCacheManager {
    #[new]
    fn new() -> Self {
        Self {
            inner: IndexCacheManager::new(),
        }
    }

    fn get<'py>(&self, py: Python<'py>, uri: &str, validator: &str) -> Option<Bound<'py, PyBytes>> {
        self.inner.get(uri, validator).map(|bytes| PyBytes::new_bound(py, &bytes))
    }

    fn set(&self, uri: &str, validator: &str, meta_data: &[u8]) -> PyResult<()> {
        self.inner.set(uri, validator, meta_data).map_err(PyRuntimeError::new_err)
    }

    fn cache_dir(&self) -> String {
        self.inner.cache_dir_path()
    }
}

pub fn register_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyCompressionAlgo>()?;
    m.add_class::<PyChecksumType>()?;
    m.add_class::<PyNameTableFormat>()?;
    m.add_class::<PyEntryType>()?;
    m.add_class::<PyMAIIndexType>()?;
    m.add_class::<PyWriteOptions>()?;
    m.add_class::<PyIndexOptions>()?;
    m.add_class::<PySearchResult>()?;
    m.add_class::<PyFileEntry>()?;
    m.add_class::<PyFixedHeader>()?;
    m.add_class::<PyMarWriter>()?;
    m.add_class::<PyMarReader>()?;
    m.add_class::<PyMAIWriter>()?;
    m.add_class::<PyMAIFixedHeader>()?;
    m.add_class::<PyMAIReader>()?;
    m.add_class::<PyIndexer>()?;
    m.add_class::<PyIndexCacheManager>()?;

    let spec_v = format!("{}.{}.{}", MAR_SPEC_MAJOR, MAR_SPEC_MINOR, MAR_SPEC_PATCH);
    let tool_v = format!("{}.{}.{}", TOOL_VERSION_MAJOR, TOOL_VERSION_MINOR, TOOL_VERSION_PATCH);
    m.add("SPEC_VERSION", &spec_v)?;
    m.add("TOOL_VERSION", &tool_v)?;
    m.add("VERSION", &tool_v)?;

    m.add_function(wrap_pyfunction!(py_get_indexer, m)?)?;
    m.add_function(wrap_pyfunction!(py_list_index_types, m)?)?;
    m.add_function(wrap_pyfunction!(py_search, m)?)?;
    m.add_function(wrap_pyfunction!(py_hash_file, m)?)?;

    Ok(())
}

#[pyfunction(name = "get_indexer")]
fn py_get_indexer(type_name: &str) -> PyResult<PyIndexer> {
    match type_name {
        "minhash" => Ok(PyIndexer {
            type_name: "minhash".to_string(),
            index_type: PyMAIIndexType::MINHASH,
        }),
        "bm25" => Ok(PyIndexer {
            type_name: "bm25".to_string(),
            index_type: PyMAIIndexType::BM25,
        }),
        "email" => Ok(PyIndexer {
            type_name: "email".to_string(),
            index_type: PyMAIIndexType::EMAIL,
        }),
        "timeseries" => Ok(PyIndexer {
            type_name: "timeseries".to_string(),
            index_type: PyMAIIndexType::TIMESERIES,
        }),
        "genomic" => Ok(PyIndexer {
            type_name: "genomic".to_string(),
            index_type: PyMAIIndexType::GENOMIC,
        }),
        _ => Err(PyRuntimeError::new_err(format!("Unknown indexer: {}", type_name))),
    }
}

#[pyfunction(name = "list_index_types")]
fn py_list_index_types() -> Vec<String> {
    vec![
        "minhash".to_string(),
        "bm25".to_string(),
        "email".to_string(),
        "timeseries".to_string(),
        "genomic".to_string(),
        "vector".to_string(),
    ]
}

#[pyfunction(name = "search")]
fn py_search(archive_path: &str, index_path: &str, query: &str, opts: &PyIndexOptions) -> PyResult<Vec<PySearchResult>> {
    let reader = MarReader::open(archive_path).map_err(PyRuntimeError::new_err)?;
    let index = MAIReader::open(index_path).ok_or_else(|| PyRuntimeError::new_err("Failed to open index"))?;

    let itype = index.header().index_type;
    let rust_opts = IndexOptions {
        params: opts.params.clone(),
        aux_files: opts.aux_files.clone(),
    };
    if itype == MAIIndexType::MinHash as u8 {
        let results = search_minhash(&reader, &index, query, &rust_opts).map_err(PyRuntimeError::new_err)?;
        Ok(results.into_iter().map(|r| PySearchResult {
            file_id: r.file_id,
            filename: r.filename,
            score: r.score,
            content: r.content,
            metadata: r.metadata,
        }).collect())
    } else if itype == MAIIndexType::BM25 as u8 {
        let results = search_bm25(&reader, &index, query, &rust_opts).map_err(PyRuntimeError::new_err)?;
        Ok(results.into_iter().map(|r| PySearchResult {
            file_id: r.file_id,
            filename: r.filename,
            score: r.score,
            content: r.content,
            metadata: r.metadata,
        }).collect())
    } else if itype == MAIIndexType::Email as u8 {
        let results = search_email(&reader, &index, query, &rust_opts).map_err(PyRuntimeError::new_err)?;
        Ok(results.into_iter().map(|r| PySearchResult {
            file_id: r.file_id,
            filename: r.filename,
            score: r.score,
            content: r.content,
            metadata: r.metadata,
        }).collect())
    } else if itype == MAIIndexType::TimeSeries as u8 {
        let results = search_timeseries(&reader, &index, query, &rust_opts).map_err(PyRuntimeError::new_err)?;
        Ok(results.into_iter().map(|r| PySearchResult {
            file_id: r.file_id,
            filename: r.filename,
            score: r.score,
            content: r.content,
            metadata: r.metadata,
        }).collect())
    } else if itype == MAIIndexType::Genomic as u8 {
        let results = search_genomic(&reader, &index, query, &rust_opts).map_err(PyRuntimeError::new_err)?;
        Ok(results.into_iter().map(|r| PySearchResult {
            file_id: r.file_id,
            filename: r.filename,
            score: r.score,
            content: r.content,
            metadata: r.metadata,
        }).collect())
    } else {
        Err(PyRuntimeError::new_err("Unsupported index type in index file"))
    }
}

#[pyfunction(name = "hash_file")]
fn py_hash_file(path: &str, algo: &str) -> PyResult<String> {
    let mut file = File::open(path).map_err(|e| PyRuntimeError::new_err(format!("Failed to open {}: {}", path, e)))?;
    let mut buffer = vec![0u8; 65536];

    match algo {
        "xxhash64" => {
            let mut hasher = XXHash3_64::new(0);
            loop {
                let n = file.read(&mut buffer).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                if n == 0 {
                    break;
                }
                hasher.update(&buffer[..n]);
            }
            Ok(format!("{:016x}", hasher.finalize()))
        }
        "blake3" => {
            let mut hasher = blake3::Hasher::new();
            loop {
                let n = file.read(&mut buffer).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
                if n == 0 {
                    break;
                }
                hasher.update(&buffer[..n]);
            }
            Ok(hasher.finalize().to_hex().to_string())
        }
        _ => Err(PyRuntimeError::new_err("Unsupported hash algorithm")),
    }
}
