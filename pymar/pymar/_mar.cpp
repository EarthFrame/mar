#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/numpy.h>
#include "mar/mar.hpp"
#include "mar/reader.hpp"
#include "mar/writer.hpp"
#include "mar/index_registry.hpp"
#include "mar/checksum.hpp"
#include "mar/constants.hpp"

namespace py = pybind11;
using namespace mar;

PYBIND11_MODULE(_mar, m) {
    m.doc() = "C++ implementation of MAR archive format";

    // Enums
    py::enum_<CompressionAlgo>(m, "CompressionAlgo")
        .value("NONE", CompressionAlgo::None)
        .value("ZSTD", CompressionAlgo::Zstd)
        .value("LZ4", CompressionAlgo::Lz4)
        .value("GZIP", CompressionAlgo::Gzip)
        .value("BZIP2", CompressionAlgo::Bzip2)
        .export_values();

    py::enum_<ChecksumType>(m, "ChecksumType")
        .value("NONE", ChecksumType::None)
        .value("XXHASH3", ChecksumType::XXHash3)
        .value("XXHASH32", ChecksumType::XXHash32)
        .value("BLAKE3", ChecksumType::Blake3)
        .value("CRC32C", ChecksumType::Crc32c)
        .export_values();

    py::enum_<NameTableFormat>(m, "NameTableFormat")
        .value("RAW_ARRAY", NameTableFormat::RawArray)
        .value("FRONT_CODED", NameTableFormat::FrontCoded)
        .value("COMPACT_TRIE", NameTableFormat::CompactTrie)
        .export_values();

    py::enum_<EntryType>(m, "EntryType")
        .value("REGULAR_FILE", EntryType::RegularFile)
        .value("DIRECTORY", EntryType::Directory)
        .value("SYMLINK", EntryType::Symlink)
        .value("CHAR_DEVICE", EntryType::CharDevice)
        .value("BLOCK_DEVICE", EntryType::BlockDevice)
        .value("FIFO", EntryType::Fifo)
        .value("SOCKET", EntryType::Socket)
        .value("UNKNOWN", EntryType::Unknown)
        .export_values();

    py::enum_<MAIIndexType>(m, "MAIIndexType")
        .value("MINHASH", MAIIndexType::MinHash)
        .value("VECTOR", MAIIndexType::Vector)
        .value("GENOMIC", MAIIndexType::Genomic)
        .value("EMAIL", MAIIndexType::Email)
        .value("TIMESERIES", MAIIndexType::TimeSeries)
        .value("BM25", MAIIndexType::BM25)
        .export_values();

    py::class_<Indexer>(m, "Indexer")
        .def("type_name", &Indexer::type_name)
        .def("index_type", &Indexer::index_type)
        .def("build", &Indexer::build);

    // Options
    py::class_<WriteOptions>(m, "WriteOptions")
        .def(py::init<>())
        .def_readwrite("compression", &WriteOptions::compression)
        .def_readwrite("checksum", &WriteOptions::checksum)
        .def_readwrite("multiblock", &WriteOptions::multiblock)
        .def_readwrite("block_size", &WriteOptions::block_size)
        .def_readwrite("compress_meta", &WriteOptions::compress_meta)
        .def_readwrite("deterministic", &WriteOptions::deterministic)
        .def_readwrite("include_posix", &WriteOptions::include_posix)
        .def_readwrite("compute_hashes", &WriteOptions::compute_hashes)
        .def_readwrite("dedup_by_hash", &WriteOptions::dedup_by_hash)
        .def_readwrite("compression_level", &WriteOptions::compression_level)
        .def_readwrite("name_table_format", &WriteOptions::name_table_format)
        .def_readwrite("num_threads", &WriteOptions::num_threads);

    py::class_<IndexOptions>(m, "IndexOptions")
        .def(py::init<>())
        .def_readwrite("params", &IndexOptions::params)
        .def_readwrite("aux_files", &IndexOptions::aux_files);

    // Results
    py::class_<SearchResult>(m, "SearchResult")
        .def_readonly("file_id", &SearchResult::file_id)
        .def_readonly("filename", &SearchResult::filename)
        .def_readonly("score", &SearchResult::score)
        .def_readonly("content", &SearchResult::content)
        .def_readonly("metadata", &SearchResult::metadata);

    py::class_<FileEntry>(m, "FileEntry")
        .def_readonly("entry_type", &FileEntry::entry_type)
        .def_readonly("logical_size", &FileEntry::logical_size);

    py::class_<FixedHeader>(m, "FixedHeader")
        .def_readonly("version_major", &FixedHeader::version_major)
        .def_readonly("version_minor", &FixedHeader::version_minor)
        .def_readonly("version_patch", &FixedHeader::version_patch)
        .def_readonly("header_size_bytes", &FixedHeader::header_size_bytes)
        .def_readonly("meta_comp_algo", &FixedHeader::meta_comp_algo);

    // MarWriter
    py::class_<MarWriter>(m, "MarWriter")
        .def(py::init<const std::string&, const WriteOptions&>(), py::arg("path"), py::arg("options") = WriteOptions())
        .def("add_file", &MarWriter::add_file, py::arg("path"), py::arg("archive_name") = "")
        .def("add_directory", &MarWriter::add_directory, py::arg("path"), py::arg("prefix") = "")
        .def("add_memory", [](MarWriter& self, const std::string& name, py::bytes content, u32 mode, i64 mtime) {
            std::string s = content;
            std::vector<u8> v(s.begin(), s.end());
            self.add_memory(name, v, mode, mtime);
        }, py::arg("name"), py::arg("content"), py::arg("mode") = DEFAULT_FILE_MODE, py::arg("mtime") = 0)
        .def("add_directory_entry", &MarWriter::add_directory_entry, py::arg("name"), py::arg("mode") = DEFAULT_DIR_MODE, py::arg("mtime") = 0)
        .def("add_symlink", &MarWriter::add_symlink, py::arg("name"), py::arg("target"), py::arg("mode") = 0120777, py::arg("mtime") = 0)
        .def("finish", &MarWriter::finish)
        .def("is_finished", &MarWriter::is_finished);

    // MarReader
    py::class_<MarReader>(m, "MarReader")
        .def(py::init<const std::string&>())
        .def("header", &MarReader::header)
        .def("file_count", &MarReader::file_count)
        .def("block_count", &MarReader::block_count)
        .def("get_name", &MarReader::get_name)
        .def("get_file_entry", &MarReader::get_file_entry)
        .def("get_names", &MarReader::get_names)
        .def("get_file_entries", &MarReader::get_file_entries)
        .def("find_file", &MarReader::find_file)
        .def("read_file", [](MarReader& self, size_t index) {
            auto v = self.read_file(index);
            return py::bytes(reinterpret_cast<const char*>(v.data()), v.size());
        })
        .def("read_file", [](MarReader& self, const std::string& name) {
            auto v = self.read_file(name);
            return py::bytes(reinterpret_cast<const char*>(v.data()), v.size());
        })
        .def("validate_parallel", &MarReader::validate_parallel, py::arg("num_threads") = 0, py::arg("verbose") = false);

    // MAI I/O
    py::class_<MAIWriter>(m, "MAIWriter")
        .def(py::init<const std::string&, MAIIndexType, u64>())
        .def("add_section", [](MAIWriter& self, u32 section_type, py::bytes data, u32 flags) {
            std::string s = data;
            std::vector<u8> v(s.begin(), s.end());
            self.add_section(section_type, v, flags);
        }, py::arg("section_type"), py::arg("data"), py::arg("flags") = 0)
        .def("write_to_file", &MAIWriter::write_to_file, py::arg("path"), py::arg("align_log2") = 0);

    py::class_<MAIFixedHeader>(m, "MAIFixedHeader")
        .def_readonly("magic", &MAIFixedHeader::magic)
        .def_readonly("version", &MAIFixedHeader::version)
        .def_readonly("index_type", &MAIFixedHeader::index_type)
        .def_readonly("align_log2", &MAIFixedHeader::align_log2)
        .def_readonly("archive_hash", &MAIFixedHeader::archive_hash)
        .def_readonly("archive_name_len", &MAIFixedHeader::archive_name_len)
        .def_readonly("flags", &MAIFixedHeader::flags)
        .def_readonly("timestamp", &MAIFixedHeader::timestamp)
        .def_readonly("index_data_offset", &MAIFixedHeader::index_data_offset);

    py::class_<MAIReader>(m, "MAIReader")
        .def_static("open", &MAIReader::open)
        .def("header", &MAIReader::header)
        .def("archive_name", &MAIReader::archive_name)
        .def("has_section", &MAIReader::has_section)
        .def("read_section", [](MAIReader& self, u32 section_type) {
            auto v = self.read_section(section_type);
            return py::bytes(reinterpret_cast<const char*>(v.data()), v.size());
        });

    // Indexing and Search
    m.def("get_indexer", [](const std::string& type_name) {
        return IndexRegistry::instance().get_indexer(type_name);
    }, py::return_value_policy::reference);

    m.def("get_searcher", [](int type) {
        return IndexRegistry::instance().get_searcher(static_cast<MAIIndexType>(type));
    }, py::return_value_policy::reference);

    m.def("list_index_types", []() {
        return IndexRegistry::instance().list_index_types();
    });

    // High-level search function
    m.def("search", [](const std::string& archive_path, const std::string& index_path, const std::string& query, const IndexOptions& opts) {
        MarReader reader(archive_path);
        auto index = MAIReader::open(index_path);
        if (!index) throw std::runtime_error("Failed to open index");
        
        auto searcher = IndexRegistry::instance().get_searcher(static_cast<MAIIndexType>(index->header().index_type));
        if (!searcher) throw std::runtime_error("No searcher available for this index type");
        
        return searcher->search(reader, *index, query, opts);
    });

    // Version
    m.attr("SPEC_VERSION") = std::to_string(MAR_SPEC_MAJOR) + "." + std::to_string(MAR_SPEC_MINOR) + "." + std::to_string(MAR_SPEC_PATCH);
    m.attr("TOOL_VERSION") = std::to_string(TOOL_VERSION_MAJOR) + "." + std::to_string(TOOL_VERSION_MINOR) + "." + std::to_string(TOOL_VERSION_PATCH);
    m.attr("VERSION") = m.attr("SPEC_VERSION");

    // Hashing
    m.def("hash_file", [](const std::string& path, const std::string& algo) {
        if (algo == "xxhash64") {
            mar::xxhash3::XXHash3_64 hasher(0);
            std::ifstream in(path, std::ios::binary);
            std::vector<u8> buffer(65536);
            while (in.read(reinterpret_cast<char*>(buffer.data()), buffer.size())) {
                hasher.update(buffer.data(), in.gcount());
            }
            if (in.gcount() > 0) hasher.update(buffer.data(), in.gcount());
            u64 digest = hasher.finalize();
            char buf[17];
            snprintf(buf, sizeof(buf), "%016llx", (unsigned long long)digest);
            return std::string(buf);
        } else if (algo == "blake3") {
            mar::Blake3Hasher hasher;
            std::ifstream in(path, std::ios::binary);
            std::vector<u8> buffer(65536);
            while (in.read(reinterpret_cast<char*>(buffer.data()), buffer.size())) {
                hasher.update(buffer.data(), in.gcount());
            }
            if (in.gcount() > 0) hasher.update(buffer.data(), in.gcount());
            auto digest = hasher.finalize();
            return mar::hash_to_hex(digest.data(), digest.size());
        }
        throw std::runtime_error("Unsupported hash algorithm");
    });
}
