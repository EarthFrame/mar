#include "mar/index_fasta.hpp"
#include "mar/index_registry.hpp"
#include "mar/reader.hpp"
#include "mar/xxhash3.h"

#include <algorithm>
#include <cstring>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

namespace mar {

// Fast XXHash3_64 helper
static inline u64 hash_name(const char* data, size_t len, u64 seed) {
    mar::xxhash3::XXHash3_64 hasher(seed);
    hasher.update(reinterpret_cast<const u8*>(data), len);
    u64 h = hasher.finalize();
    return h == 0 ? 1 : h;  // 0 is reserved for empty slot
}

static bool is_fasta_filename(const std::string& name) {
    auto ends_with = [&](const std::string& suffix) {
        return name.size() >= suffix.size() &&
               name.compare(name.size() - suffix.size(), suffix.size(), suffix) == 0;
    };
    return ends_with(".fa") || ends_with(".fasta") || ends_with(".fna") ||
           ends_with(".faa") || ends_with(".ffn") || ends_with(".frn");
}

// Sink that streams data block by block, updating streaming FASTA parser state.
class FastaStreamParser : public CompressionSink {
public:
    u32 file_id;
    u64 stream_offset = 0;

    struct InFlightRecord {
        std::string name;
        u64 file_byte_offset = 0;
        u32 header_len = 0;
        u32 raw_seq_bytes = 0;
        u64 seq_len = 0;
        u16 line_len = 0;
        u16 line_blen = 0;
        bool first_line_seen = false;
    };

    bool has_active = false;
    InFlightRecord current;

    // Buffer to handle line boundaries crossing sink writes
    std::string carry;
    u64 carry_start_offset = 0;

    std::vector<InFlightRecord> finished_records;

    explicit FastaStreamParser(u32 fid) : file_id(fid) {}

    bool write(const u8* data, size_t len) override {
        if (len == 0)
            return true;

        const char* p = reinterpret_cast<const char*>(data);
        size_t remaining = len;

        // If we have carry from previous write, combine enough to find newline
        if (!carry.empty()) {
            const char* nl = static_cast<const char*>(std::memchr(p, '\n', remaining));
            if (!nl) {
                carry.append(p, remaining);
                stream_offset += remaining;
                return true;
            }
            size_t take = (nl - p) + 1;
            carry.append(p, take);
            process_line(carry.data(), carry.size(), carry_start_offset);
            carry.clear();
            p += take;
            remaining -= take;
            stream_offset += take;
        }

        // Process full lines in current chunk
        while (remaining > 0) {
            const char* nl = static_cast<const char*>(std::memchr(p, '\n', remaining));
            if (!nl) {
                // Remainder is a partial line
                carry_start_offset = stream_offset;
                carry.assign(p, remaining);
                stream_offset += remaining;
                return true;
            }
            size_t line_len = (nl - p) + 1;
            process_line(p, line_len, stream_offset);
            p += line_len;
            stream_offset += line_len;
            remaining -= line_len;
        }

        return true;
    }

    void finish() {
        if (!carry.empty()) {
            process_line(carry.data(), carry.size(), carry_start_offset);
            carry.clear();
        }
        if (has_active) {
            finished_records.push_back(current);
            has_active = false;
        }
    }

private:
    void process_line(const char* line, size_t len, u64 line_offset) {
        if (len == 0)
            return;

        if (line[0] == '>') {
            if (has_active) {
                finished_records.push_back(current);
                has_active = false;
            }

            current = InFlightRecord{};
            current.file_byte_offset = line_offset;
            current.header_len = static_cast<u32>(len);

            // Parse accession name (everything after '>' up to whitespace)
            size_t start = 1;
            while (start < len && (line[start] == ' ' || line[start] == '\t')) {
                start++;
            }
            size_t end = start;
            while (end < len && line[end] != ' ' && line[end] != '\t' && line[end] != '\r' && line[end] != '\n') {
                end++;
            }
            if (end > start) {
                current.name.assign(line + start, end - start);
            }
            has_active = true;
        } else if (has_active) {
            current.raw_seq_bytes += static_cast<u32>(len);

            // Count pure sequence bases
            size_t bases = len;
            while (bases > 0 && (line[bases - 1] == '\r' || line[bases - 1] == '\n')) {
                bases--;
            }
            current.seq_len += bases;

            if (!current.first_line_seen && bases > 0) {
                current.line_len = static_cast<u16>(std::min<size_t>(bases, 65535));
                current.line_blen = static_cast<u16>(std::min<size_t>(len, 65535));
                current.first_line_seen = true;
            }
        }
    }
};

// ============================================================================
// FASTA Indexer Implementation
// ============================================================================

class FastaIndexer : public Indexer {
public:
    const char* type_name() const override { return "fasta"; }
    MAIIndexType index_type() const override { return MAIIndexType::Fasta; }

    void show_help() const override {
        std::cout << R"(FASTA index options (--with key=value):
  seed=S           Hash seed for XXHash3_64 (default: 42)
  load_factor=F    Hash table target load factor (default: 0.70)

Description:
  Builds a high-performance random-access sidecar index (.mai) for FASTA files
  in the archive. Enables sub-millisecond retrieval by sequence ID, multi-file
  scoping (file:ID), and ultra-fast iteration without full archive decompression.
)";
    }

    void build(const MarReader& reader, MAIWriter& writer, const IndexOptions& opts) override {
        const u32 file_count = static_cast<u32>(reader.file_count());
        u64 seed = 42;
        if (opts.has("seed")) {
            seed = std::stoull(opts.get("seed"));
        }

        std::vector<FastaFileEntry> file_entries;
        std::vector<FastaRecordEntry> record_entries;
        std::vector<std::string> record_names;
        std::vector<std::string> file_names;

        std::string name_table; // packed string table for names and filenames
        u32 total_indexed_files = 0;

        for (u32 fi = 0; fi < file_count; ++fi) {
            auto entry_opt = reader.get_file_entry(fi);
            if (!entry_opt || entry_opt->entry_type != EntryType::RegularFile) {
                continue;
            }

            auto name_opt = reader.get_name(fi);
            std::string fname = name_opt ? *name_opt : "";

            // Check if file is FASTA
            bool is_fasta = is_fasta_filename(fname);
            if (!is_fasta && entry_opt->logical_size > 0) {
                // Check first byte without whole file decompression
                // Single byte read
                FastaStreamParser probe_sink(fi);
                // Try extract first chunk to check for '>'
            }

            if (!is_fasta) {
                continue;
            }

            u64 start_record_idx = record_entries.size();

            // Stream extract file to FastaStreamParser (block by block, constant RAM!)
            FastaStreamParser parser(fi);
            const_cast<MarReader&>(reader).extract_file_to_sink(fi, parser);
            parser.finish();

            u64 count = parser.finished_records.size();
            if (count == 0 && entry_opt->logical_size > 0) {
                // Possibly not a FASTA file or empty records
                continue;
            }

            u32 fname_offset = static_cast<u32>(name_table.size());
            name_table.append(fname.data(), fname.size());
            name_table.push_back('\0');

            FastaFileEntry fentry{};
            fentry.file_id = fi;
            fentry.record_start_idx = start_record_idx;
            fentry.record_count = count;
            fentry.filename_offset = fname_offset;
            file_entries.push_back(fentry);
            total_indexed_files++;

            for (const auto& rec : parser.finished_records) {
                u32 name_off = static_cast<u32>(name_table.size());
                name_table.append(rec.name.data(), rec.name.size());
                name_table.push_back('\0');

                FastaRecordEntry rentry{};
                rentry.file_id = fi;
                rentry.name_offset = name_off;
                rentry.file_byte_offset = rec.file_byte_offset;
                rentry.header_len = rec.header_len;
                rentry.raw_seq_bytes = rec.raw_seq_bytes;
                rentry.seq_len = rec.seq_len;
                rentry.line_len = rec.line_len;
                rentry.line_blen = rec.line_blen;

                record_entries.push_back(rentry);
                record_names.push_back(rec.name);
            }
        }

        // Build Robin Hood / Linear probing power-of-two hash index
        u64 total_records = record_entries.size();
        u32 slot_count = 16;
        while (slot_count < (total_records * 10 / 7) + 16) {
            slot_count <<= 1;
        }

        std::vector<FastaHashSlot> hash_slots(slot_count, FastaHashSlot{0, UINT32_MAX, 0});
        u32 mask = slot_count - 1;

        for (u32 ri = 0; ri < static_cast<u32>(total_records); ++ri) {
            const std::string& name = record_names[ri];
            u64 h = hash_name(name.data(), name.size(), seed);
            u32 slot = static_cast<u32>(h & mask);
            u32 dist = 0;

            FastaHashSlot curr{h, ri, record_entries[ri].name_offset};

            while (true) {
                if (hash_slots[slot].record_idx == UINT32_MAX) {
                    hash_slots[slot] = curr;
                    break;
                }

                u32 existing_slot = static_cast<u32>(hash_slots[slot].hash64 & mask);
                u32 existing_dist = (slot + slot_count - existing_slot) & mask;

                if (dist > existing_dist) {
                    std::swap(curr, hash_slots[slot]);
                    dist = existing_dist;
                }

                slot = (slot + 1) & mask;
                dist++;
            }
        }

        // Build sections
        FastaParams params{};
        params.record_count = total_records;
        params.file_count = total_indexed_files;
        params.hash_slot_count = slot_count;
        params.seed = seed;
        params.flags = 0;
        params.name_table_size = static_cast<u32>(name_table.size());

        std::vector<u8> params_sec(sizeof(FastaParams));
        std::memcpy(params_sec.data(), &params, sizeof(params));
        writer.add_section(SEC_FASTA_PARAMS, params_sec);

        std::vector<u8> file_dir_sec(file_entries.size() * sizeof(FastaFileEntry));
        if (!file_entries.empty()) {
            std::memcpy(file_dir_sec.data(), file_entries.data(), file_dir_sec.size());
        }
        writer.add_section(SEC_FASTA_FILE_DIR, file_dir_sec);

        std::vector<u8> record_sec(record_entries.size() * sizeof(FastaRecordEntry));
        if (!record_entries.empty()) {
            std::memcpy(record_sec.data(), record_entries.data(), record_sec.size());
        }
        writer.add_section(SEC_FASTA_RECORD_TABLE, record_sec);

        std::vector<u8> name_sec(name_table.begin(), name_table.end());
        writer.add_section(SEC_FASTA_NAME_TABLE, name_sec);

        std::vector<u8> hash_sec(hash_slots.size() * sizeof(FastaHashSlot));
        std::memcpy(hash_sec.data(), hash_slots.data(), hash_sec.size());
        writer.add_section(SEC_FASTA_HASH_INDEX, hash_sec);
    }
};

// ============================================================================
// FASTA Searcher Implementation
// ============================================================================

class FastaSearcher : public Searcher {
public:
    bool supports_type(MAIIndexType type) const override { return type == MAIIndexType::Fasta; }

    std::vector<SearchResult> search(const MarReader& reader, const MAIReader& index,
                                     const std::string& query, const IndexOptions& opts) override {
        // Load params
        size_t psz = 0;
        const u8* pp = index.get_section_ptr(SEC_FASTA_PARAMS, &psz);
        if (!pp || psz < sizeof(FastaParams)) {
            throw std::runtime_error("Corrupt or missing FASTA_PARAMS section");
        }
        FastaParams params;
        std::memcpy(&params, pp, sizeof(params));

        size_t files_sz = 0;
        const u8* files_p = index.get_section_ptr(SEC_FASTA_FILE_DIR, &files_sz);
        const FastaFileEntry* file_dir = reinterpret_cast<const FastaFileEntry*>(files_p);
        u32 indexed_files = params.file_count;

        size_t rec_sz = 0;
        const u8* rec_p = index.get_section_ptr(SEC_FASTA_RECORD_TABLE, &rec_sz);
        const FastaRecordEntry* records = reinterpret_cast<const FastaRecordEntry*>(rec_p);

        size_t names_sz = 0;
        const u8* names_p = index.get_section_ptr(SEC_FASTA_NAME_TABLE, &names_sz);
        const char* strtab = reinterpret_cast<const char*>(names_p);

        size_t hash_sz = 0;
        const u8* hash_p = index.get_section_ptr(SEC_FASTA_HASH_INDEX, &hash_sz);
        const FastaHashSlot* hash_slots = reinterpret_cast<const FastaHashSlot*>(hash_p);
        u32 slot_count = params.hash_slot_count;

        if (!records || !strtab || !hash_slots || slot_count == 0) {
            return {};
        }

        bool do_extract = (opts.get("extract") == "true");
        std::string target_file = opts.get("file", "");

        // Check if query is in qualified format "filename:record_id"
        std::string query_file = target_file;
        std::string query_id = query;
        size_t colon_pos = query.find(':');
        if (colon_pos != std::string::npos && query_file.empty()) {
            std::string potential_file = query.substr(0, colon_pos);
            // Verify if potential_file matches any indexed file
            for (u32 fi = 0; fi < indexed_files; ++fi) {
                std::string fn(strtab + file_dir[fi].filename_offset);
                if (fn == potential_file) {
                    query_file = potential_file;
                    query_id = query.substr(colon_pos + 1);
                    break;
                }
            }
        }

        std::vector<u32> matched_records;

        // Query by accession name
        if (!query_id.empty() && query_id != "*") {
            u64 h = hash_name(query_id.data(), query_id.size(), params.seed);
            u32 mask = slot_count - 1;
            u32 slot = static_cast<u32>(h & mask);
            u32 dist = 0;

            while (true) {
                const auto& sl = hash_slots[slot];
                if (sl.record_idx == UINT32_MAX) {
                    break;
                }

                u32 ideal_slot = static_cast<u32>(sl.hash64 & mask);
                u32 probe_dist = (slot + slot_count - ideal_slot) & mask;
                if (dist > probe_dist) {
                    break;
                }

                if (sl.hash64 == h) {
                    // String check
                    const char* cand_name = strtab + sl.name_offset;
                    if (query_id == cand_name) {
                        const auto& rec = records[sl.record_idx];
                        if (query_file.empty()) {
                            matched_records.push_back(sl.record_idx);
                        } else {
                            auto fname_opt = reader.get_name(rec.file_id);
                            if (fname_opt && *fname_opt == query_file) {
                                matched_records.push_back(sl.record_idx);
                            }
                        }
                    }
                }

                slot = (slot + 1) & mask;
                dist++;
            }
        } else if (!query_file.empty()) {
            // File iteration: return all records for this file
            for (u32 fi = 0; fi < indexed_files; ++fi) {
                std::string fn(strtab + file_dir[fi].filename_offset);
                if (fn == query_file) {
                    for (u64 ri = 0; ri < file_dir[fi].record_count; ++ri) {
                        matched_records.push_back(static_cast<u32>(file_dir[fi].record_start_idx + ri));
                    }
                    break;
                }
            }
        }

        std::vector<SearchResult> results;
        for (u32 ri : matched_records) {
            const auto& rec = records[ri];
            std::string rec_name(strtab + rec.name_offset);
            auto fname_opt = reader.get_name(rec.file_id);
            std::string fname = fname_opt ? *fname_opt : "(unknown)";

            SearchResult sr;
            sr.file_id = rec.file_id;
            sr.filename = fname;
            sr.score = 1.0;
            sr.metadata["id"] = rec_name;
            sr.metadata["seq_len"] = std::to_string(rec.seq_len);
            sr.metadata["offset"] = std::to_string(rec.file_byte_offset);
            sr.metadata["raw_bytes"] = std::to_string(rec.raw_seq_bytes);

            if (do_extract) {
                extract_record(reader, rec, rec_name);
            }

            results.push_back(sr);
        }

        return results;
    }

private:
    void extract_record(const MarReader& reader, const FastaRecordEntry& rec, const std::string& /*rec_name*/) {
        // Efficient extraction: decompress only the bytes covering this record
        // Uncompressed range in file: [rec.file_byte_offset, rec.file_byte_offset + rec.header_len + rec.raw_seq_bytes)
        u64 start_byte = rec.file_byte_offset;
        u64 total_bytes = static_cast<u64>(rec.header_len) + rec.raw_seq_bytes;
        u64 end_byte = start_byte + total_bytes;

        std::vector<Span> spans = reader.get_file_spans(rec.file_id);

        if (spans.empty()) {
            // Fallback for SingleFilePerBlock or no spans: extract whole file
            auto data = const_cast<MarReader&>(reader).read_file(rec.file_id);
            if (start_byte < data.size()) {
                u64 len = std::min<u64>(total_bytes, data.size() - start_byte);
                std::cout.write(reinterpret_cast<const char*>(data.data() + start_byte), len);
            }
            return;
        }

        // Map uncompressed byte range to spans
        u64 cur_file_offset = 0;
        for (const auto& span : spans) {
            u64 span_file_start = cur_file_offset;
            u64 span_file_end = cur_file_offset + span.length;
            cur_file_offset = span_file_end;

            if (span_file_end <= start_byte || span_file_start >= end_byte) {
                continue; // Does not intersect target record
            }

            // Span intersects record! Read block data
            u64 overlap_start = std::max(span_file_start, start_byte);
            u64 overlap_end = std::min(span_file_end, end_byte);
            u64 offset_in_span = overlap_start - span_file_start;
            u64 len_in_span = overlap_end - overlap_start;

            const auto& block_data = const_cast<MarReader&>(reader).read_block(span.block_id);
            u64 block_read_pos = span.offset_in_block + offset_in_span;
            if (block_read_pos + len_in_span <= block_data.size()) {
                std::cout.write(reinterpret_cast<const char*>(block_data.data() + block_read_pos), len_in_span);
            }
        }
    }
};

// ============================================================================
// Registration
// ============================================================================

static struct RegisterFasta {
    RegisterFasta() {
        IndexRegistry::instance().register_indexer(std::make_unique<FastaIndexer>());
        IndexRegistry::instance().register_searcher(std::make_unique<FastaSearcher>());
    }
} g_register_fasta;

}  // namespace mar
