// Genomic index: k-mer MinHash sketches + faidx/bin region index for FASTA/VCF/BAM.
//
// Sections:
//   1  GENOMIC_PARAMS        64-byte header
//   2  GENOMIC_KMER_SKETCHES file_count * num_hashes * 8 bytes (u64)
//   3  GENOMIC_CONTIG_TABLE  FileContigDir[] + ContigRecord[] + string table
//   4  GENOMIC_REGION_INDEX  counts + FastaRegionEntry[] + VcfBinEntry[] + BamBinEntry[]
//   5  GENOMIC_COMPAT_TABLE  FileCompatSummary[] + CompatIssue[]

#include "mar/index_format.hpp"
#include "mar/index_registry.hpp"
#include "mar/xxhash3.h"

#include <algorithm>
#include <cmath>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <regex>
#include <set>
#include <sstream>
#include <stdexcept>
#include <vector>

namespace mar {

// ============================================================================
// Section codes
// ============================================================================

constexpr u32 SEC_GENOMIC_PARAMS = 1;
constexpr u32 SEC_GENOMIC_SKETCHES = 2;
constexpr u32 SEC_GENOMIC_CONTIGS = 3;
constexpr u32 SEC_GENOMIC_REGIONS = 4;
constexpr u32 SEC_GENOMIC_COMPAT = 5;

// ============================================================================
// On-disk structs
// ============================================================================

#pragma pack(push, 1)

struct GenomicParams {  // 64 bytes
    u32 file_count;
    u32 k;           // k-mer size (default 21)
    u32 num_hashes;  // sketch size (default 256)
    u64 seed;
    u8 stranded;  // 0=canonical, 1=forward-only
    u8 reserved[3];
    u32 genomic_file_count;
    u32 region_entry_count;
    u8 padding[32];
};

struct FileContigDir {      // per-file directory entry in CONTIG_TABLE
    u32 contig_list_start;  // index of first ContigRecord for this file
    u32 contig_count;       // 0 for non-genomic
    u8 file_type;           // 0=none,1=FASTA,2=FASTQ,3=VCF,4=BAM,5=BCF
    u8 reserved[3];
};

struct ContigRecord {
    u32 name_offset;  // byte offset into string table
    u64 length_bp;
    u32 seq_checksum;  // CRC32 of bases (FASTA only; 0 otherwise)
};

struct FastaRegionEntry {  // 32 bytes
    u32 file_id;
    u32 contig_id;
    u64 seq_len_bp;
    u64 file_byte_offset;  // byte offset of first base in uncompressed stream
    u32 bases_per_line;
    u32 bytes_per_line;  // includes newline (bases_per_line + 1 or +2)
};

struct VcfBinEntry {  // 24 bytes
    u32 file_id;
    u32 contig_id;
    u32 bin_start_pos;  // 1-based inclusive
    u32 bin_end_pos;    // 1-based inclusive
    u64 file_byte_offset;
};

struct BamBinEntry {  // 24 bytes
    u32 file_id;
    u32 contig_id;
    u32 bin_start_pos;  // 0-based
    u32 bin_end_pos;
    u64 bgzf_virtual_offset;
};

struct FileCompatSummary {
    u32 contig_count;
    u8 naming_convention;  // 0=unknown, 1=ucsc (chr1), 2=ensembl (1)
    u8 file_type;
    u8 reserved[2];
};

struct CompatIssue {
    u32 file_a_id;
    u32 file_b_id;
    u8 issue_flags;
    //  0x01 = naming_convention_mismatch
    //  0x02 = length_mismatch
    //  0x04 = missing_in_b
    //  0x08 = extra_in_b
    u8 reserved[3];
    u32 contig_a_id;
    u32 contig_b_id;
    u64 len_a;
    u64 len_b;
};

#pragma pack(pop)

static_assert(sizeof(GenomicParams) == 64, "GenomicParams must be 64 bytes");
static_assert(sizeof(FastaRegionEntry) == 32, "FastaRegionEntry must be 32 bytes");
static_assert(sizeof(VcfBinEntry) == 24, "VcfBinEntry must be 24 bytes");
static_assert(sizeof(BamBinEntry) == 24, "BamBinEntry must be 24 bytes");

// ============================================================================
// Genomic file type detection
// ============================================================================

enum class GenomicFileType : u8 { None = 0, FASTA = 1, FASTQ = 2, VCF = 3, BAM = 4, BCF = 5 };

static GenomicFileType detect_file_type(const std::string& name, const std::vector<u8>& data) {
    // Extension-based
    auto ends_with = [&](const std::string& suffix) {
        return name.size() >= suffix.size() && name.compare(name.size() - suffix.size(), suffix.size(), suffix) == 0;
    };
    if (ends_with(".fa") || ends_with(".fasta") || ends_with(".fna") || ends_with(".ffn") || ends_with(".frn"))
        return GenomicFileType::FASTA;
    if (ends_with(".fq") || ends_with(".fastq"))
        return GenomicFileType::FASTQ;
    if (ends_with(".vcf") || ends_with(".vcf.gz"))
        return GenomicFileType::VCF;
    if (ends_with(".bam"))
        return GenomicFileType::BAM;
    if (ends_with(".bcf"))
        return GenomicFileType::BCF;

    // Magic byte fallback
    if (!data.empty()) {
        if (data[0] == '>')
            return GenomicFileType::FASTA;
        if (data[0] == '@')
            return GenomicFileType::FASTQ;
        if (data.size() >= 4 && data[0] == 'B' && data[1] == 'A' && data[2] == 'M' && data[3] == 1)
            return GenomicFileType::BAM;
        if (data.size() >= 16) {
            std::string head(reinterpret_cast<const char*>(data.data()), std::min(data.size(), size_t(16)));
            if (head.find("##fileformat=VCF") != std::string::npos)
                return GenomicFileType::VCF;
        }
    }
    return GenomicFileType::None;
}

// ============================================================================
// K-mer MinHash (canonical)
// ============================================================================

static u64 kmer_hash(const char* seq, u32 k, u64 seed, bool canonical) {
    mar::xxhash3::XXHash3_64 h1(seed);
    h1.update(reinterpret_cast<const u8*>(seq), k);
    u64 fwd = h1.finalize();
    if (!canonical)
        return fwd;

    // Reverse complement
    std::string rc(k, 'N');
    static const char comp[256] = {['A'] = 'T', ['T'] = 'A', ['C'] = 'G', ['G'] = 'C', ['a'] = 't',
                                   ['t'] = 'a', ['c'] = 'g', ['g'] = 'c', ['N'] = 'N', ['n'] = 'n'};
    for (u32 i = 0; i < k; ++i) rc[k - 1 - i] = comp[(u8)seq[i]];
    mar::xxhash3::XXHash3_64 h2(seed);
    h2.update(reinterpret_cast<const u8*>(rc.data()), k);
    u64 rev = h2.finalize();
    return std::min(fwd, rev);
}

static std::vector<u64> sketch_sequence(const std::string& seq, u32 k, u32 num_hashes, u64 seed, bool canonical) {
    std::vector<u64> sketch(num_hashes, 0xFFFFFFFFFFFFFFFFULL);
    if (seq.size() < k)
        return sketch;

    for (size_t i = 0; i <= seq.size() - k; ++i) {
        u64 h1 = kmer_hash(seq.data() + i, k, seed, canonical);
        u64 h2 = kmer_hash(seq.data() + i, k, seed + 1, canonical);
        for (u32 j = 0; j < num_hashes; ++j) {
            u64 v = h1 + static_cast<u64>(j) * h2;
            if (v < sketch[j])
                sketch[j] = v;
        }
    }
    return sketch;
}

static double jaccard_sketch(const u64* a, const u64* b, u32 n) {
    u32 match = 0, valid = 0;
    for (u32 i = 0; i < n; ++i) {
        if (a[i] != 0xFFFFFFFFFFFFFFFFULL) {
            ++valid;
            if (a[i] == b[i])
                ++match;
        }
    }
    return valid == 0 ? 0.0 : static_cast<double>(match) / n;
}

// ============================================================================
// FASTA contig + region parsing (simple, no BGZF)
// ============================================================================

struct ContigInfo {
    std::string name;
    u64 length_bp = 0;
    u64 file_byte_offset = 0;  // byte of first base
    u32 bases_per_line = 0;
    u32 bytes_per_line = 0;  // bases + 1 (LF) or bases + 2 (CRLF)
    u32 seq_checksum = 0;
    std::string full_sequence;  // kept for sketching; cleared after use
};

static std::vector<ContigInfo> parse_fasta(const std::vector<u8>& data) {
    std::vector<ContigInfo> contigs;
    const char* p = reinterpret_cast<const char*>(data.data());
    const char* end = p + data.size();

    ContigInfo cur;
    bool in_seq = false;
    u64 byte_pos = 0;

    while (p < end) {
        const char* nl = reinterpret_cast<const char*>(std::memchr(p, '\n', end - p));
        size_t line_len = nl ? static_cast<size_t>(nl - p) : static_cast<size_t>(end - p);

        if (line_len > 0 && p[line_len - 1] == '\r')
            --line_len;  // strip CR

        if (line_len > 0 && p[0] == '>') {
            if (in_seq)
                contigs.push_back(cur);
            cur = ContigInfo{};
            // Extract contig name (up to first space)
            size_t sp = 1;
            while (sp < line_len && p[sp] != ' ' && p[sp] != '\t') ++sp;
            cur.name = std::string(p + 1, sp - 1);
            in_seq = true;
        } else if (in_seq && line_len > 0) {
            u64 seq_start = byte_pos + static_cast<u64>(p - reinterpret_cast<const char*>(data.data()));
            if (cur.file_byte_offset == 0 && cur.length_bp == 0) {
                cur.file_byte_offset = seq_start;
                cur.bases_per_line = static_cast<u32>(line_len);
                cur.bytes_per_line = static_cast<u32>(line_len) + (nl && *(nl - 1) == '\r' ? 2 : 1);
            }
            cur.full_sequence.append(p, line_len);
            cur.length_bp += static_cast<u64>(line_len);
        }

        if (!nl)
            break;
        p = nl + 1;
    }
    if (in_seq)
        contigs.push_back(cur);
    return contigs;
}

// ============================================================================
// VCF contig header parsing
// ============================================================================

static std::vector<ContigInfo> parse_vcf_contigs(const std::vector<u8>& data) {
    std::vector<ContigInfo> contigs;
    const char* p = reinterpret_cast<const char*>(data.data());
    const char* end = p + data.size();

    while (p < end && *p == '#') {
        const char* nl = reinterpret_cast<const char*>(std::memchr(p, '\n', end - p));
        size_t line_len = nl ? static_cast<size_t>(nl - p) : static_cast<size_t>(end - p);
        std::string line(p, line_len);

        if (line.substr(0, 13) == "##contig=<ID=") {
            ContigInfo ci;
            size_t id_start = 13;
            size_t id_end = line.find(',', id_start);
            if (id_end == std::string::npos)
                id_end = line.find('>', id_start);
            ci.name = line.substr(id_start, id_end - id_start);

            size_t lp = line.find("length=");
            if (lp != std::string::npos) {
                lp += 7;
                size_t le = line.find_first_of(",>", lp);
                ci.length_bp = std::stoull(line.substr(lp, le - lp));
            }
            contigs.push_back(ci);
        }

        if (!nl)
            break;
        p = nl + 1;
    }
    return contigs;
}

// ============================================================================
// String table builder
// ============================================================================

class StringTable {
public:
    u32 add(const std::string& s) {
        auto it = offsets_.find(s);
        if (it != offsets_.end())
            return it->second;
        u32 off = static_cast<u32>(data_.size());
        offsets_[s] = off;
        data_.insert(data_.end(), s.begin(), s.end());
        data_.push_back('\0');
        return off;
    }
    const std::vector<u8>& data() const { return data_; }
    size_t size() const { return data_.size(); }

private:
    std::map<std::string, u32> offsets_;
    std::vector<u8> data_;
};

// ============================================================================
// Naming convention detection
// ============================================================================

static u8 detect_naming(const std::vector<ContigInfo>& contigs) {
    if (contigs.empty())
        return 0;
    u32 chr_count = 0;
    for (const auto& c : contigs) {
        if (c.name.size() >= 3 && c.name.substr(0, 3) == "chr")
            ++chr_count;
    }
    if (chr_count * 2 >= contigs.size())
        return 1;  // UCSC
    return 2;      // Ensembl
}

// ============================================================================
// GenomicIndexer
// ============================================================================

class GenomicIndexer : public Indexer {
public:
    const char* type_name() const override { return "genomic"; }
    MAIIndexType index_type() const override { return MAIIndexType::Genomic; }

    void build(const MarReader& reader, MAIWriter& writer, const IndexOptions& opts) override {
        const u32 k = static_cast<u32>(std::stoul(opts.get("k", "21")));
        const u32 num_hashes = static_cast<u32>(std::stoul(opts.get("num_hashes", "256")));
        const u64 seed = std::stoull(opts.get("seed", "42"));
        const bool stranded = (opts.get("stranded", "0") == "1");
        const u32 vcf_bin_sz = static_cast<u32>(std::stoul(opts.get("vcf_bin_size", "65536")));

        const size_t file_count = reader.file_count();

        std::cerr << "Building genomic index: k=" << k << " hashes=" << num_hashes << " "
                  << (stranded ? "stranded" : "canonical") << "\n";

        // Per-file data collected during indexing.
        struct FileData {
            GenomicFileType type = GenomicFileType::None;
            std::vector<u64> sketch;  // num_hashes u64s
            std::vector<ContigInfo> contigs;
            std::vector<FastaRegionEntry> fasta_entries;
            std::vector<VcfBinEntry> vcf_entries;
            // BAM region indexing omitted in this baseline (requires BGZF parsing).
        };
        std::vector<FileData> file_data(file_count);
        u32 genomic_count = 0;

        for (u32 fi = 0; fi < file_count; ++fi) {
            auto entry_opt = reader.get_file_entry(fi);
            if (!entry_opt || entry_opt->entry_type != EntryType::RegularFile) {
                file_data[fi].sketch.assign(num_hashes, 0xFFFFFFFFFFFFFFFFULL);
                continue;
            }

            auto name_opt = reader.get_name(fi);
            std::string name = name_opt ? *name_opt : "";
            auto data = const_cast<MarReader&>(reader).read_file(fi);

            GenomicFileType ftype = detect_file_type(name, data);
            file_data[fi].type = ftype;

            if (ftype == GenomicFileType::None) {
                file_data[fi].sketch.assign(num_hashes, 0xFFFFFFFFFFFFFFFFULL);
                continue;
            }

            ++genomic_count;
            std::cerr << "  [" << fi << "] " << name << " ("
                      << (ftype == GenomicFileType::FASTA   ? "FASTA"
                          : ftype == GenomicFileType::FASTQ ? "FASTQ"
                          : ftype == GenomicFileType::VCF   ? "VCF"
                          : ftype == GenomicFileType::BAM   ? "BAM"
                                                            : "BCF")
                      << ")\n";

            // --- FASTA ---
            if (ftype == GenomicFileType::FASTA || ftype == GenomicFileType::FASTQ) {
                auto contigs = parse_fasta(data);
                file_data[fi].contigs = contigs;

                // Build sketch from all sequence data.
                std::string all_seq;
                for (auto& c : contigs) all_seq += c.full_sequence;
                file_data[fi].sketch = sketch_sequence(all_seq, k, num_hashes, seed, !stranded);

                // Build FASTA region entries.
                for (u32 ci = 0; ci < contigs.size(); ++ci) {
                    FastaRegionEntry e{};
                    e.file_id = fi;
                    e.contig_id = ci;
                    e.seq_len_bp = contigs[ci].length_bp;
                    e.file_byte_offset = contigs[ci].file_byte_offset;
                    e.bases_per_line = contigs[ci].bases_per_line;
                    e.bytes_per_line = contigs[ci].bytes_per_line;
                    file_data[fi].fasta_entries.push_back(e);
                }
            }

            // --- VCF ---
            if (ftype == GenomicFileType::VCF) {
                auto contigs = parse_vcf_contigs(data);
                file_data[fi].contigs = contigs;

                // Build VCF bin entries by scanning data records.
                // Sketch from contig name concatenation (proxy for genomic content).
                std::string all_names;
                for (auto& c : contigs) all_names += c.name;
                file_data[fi].sketch = sketch_sequence(all_names, std::min(k, u32(4)), num_hashes, seed, false);

                // Scan VCF data lines to build bin index.
                const char* vp = reinterpret_cast<const char*>(data.data());
                const char* end = vp + data.size();
                u64 byte_offset = 0;

                // Map contig name -> index.
                std::map<std::string, u32> contig_idx;
                for (u32 ci = 0; ci < contigs.size(); ++ci) {
                    contig_idx[contigs[ci].name] = ci;
                }

                // Current bin per contig.
                std::map<u32, std::pair<u32, u64>> cur_bin;  // contig_id -> (bin_start, byte_off)

                while (vp < end) {
                    const char* nl = reinterpret_cast<const char*>(std::memchr(vp, '\n', end - vp));
                    size_t ll = nl ? (size_t)(nl - vp) : (size_t)(end - vp);
                    u64 line_start = byte_offset;
                    byte_offset += ll + (nl ? 1 : 0);

                    if (ll > 0 && vp[0] != '#') {
                        // Parse CHROM\tPOS
                        const char* tab1 = reinterpret_cast<const char*>(std::memchr(vp, '\t', ll));
                        if (tab1) {
                            std::string chrom(vp, tab1 - vp);
                            const char* tab2 =
                                reinterpret_cast<const char*>(std::memchr(tab1 + 1, '\t', ll - (tab1 + 1 - vp)));
                            if (tab2) {
                                u32 pos = static_cast<u32>(std::strtoul(tab1 + 1, nullptr, 10));
                                auto it = contig_idx.find(chrom);
                                if (it != contig_idx.end()) {
                                    u32 ci = it->second;
                                    u32 bin = (pos / vcf_bin_sz) * vcf_bin_sz + 1;
                                    auto& cb = cur_bin[ci];
                                    if (cb.first == 0) {
                                        cb = {bin, line_start};
                                    } else if (bin > cb.first + vcf_bin_sz) {
                                        VcfBinEntry e{};
                                        e.file_id = fi;
                                        e.contig_id = ci;
                                        e.bin_start_pos = cb.first;
                                        e.bin_end_pos = cb.first + vcf_bin_sz - 1;
                                        e.file_byte_offset = cb.second;
                                        file_data[fi].vcf_entries.push_back(e);
                                        cb = {bin, line_start};
                                    }
                                }
                            }
                        }
                    }

                    if (!nl)
                        break;
                    vp = nl + 1;
                }
                // Flush remaining bins.
                for (auto& [ci, cb] : cur_bin) {
                    VcfBinEntry e{};
                    e.file_id = fi;
                    e.contig_id = ci;
                    e.bin_start_pos = cb.first;
                    e.bin_end_pos = cb.first + vcf_bin_sz - 1;
                    e.file_byte_offset = cb.second;
                    file_data[fi].vcf_entries.push_back(e);
                }
            }

            // --- BAM/BCF: sketch only (region index requires BGZF; recorded as placeholder) ---
            if (ftype == GenomicFileType::BAM || ftype == GenomicFileType::BCF) {
                file_data[fi].sketch.assign(num_hashes, 0xFFFFFFFFFFFFFFFFULL);
            }
        }

        // ============================================================
        // Compatibility table
        // ============================================================
        std::vector<FileCompatSummary> compat_summaries(file_count);
        std::vector<CompatIssue> issues;

        for (u32 fi = 0; fi < file_count; ++fi) {
            compat_summaries[fi].file_type = static_cast<u8>(file_data[fi].type);
            compat_summaries[fi].contig_count = static_cast<u32>(file_data[fi].contigs.size());
            compat_summaries[fi].naming_convention = detect_naming(file_data[fi].contigs);
        }

        // Compare every pair of genomic files.
        for (u32 fa = 0; fa < file_count; ++fa) {
            if (file_data[fa].contigs.empty())
                continue;
            for (u32 fb = fa + 1; fb < file_count; ++fb) {
                if (file_data[fb].contigs.empty())
                    continue;

                u8 na = compat_summaries[fa].naming_convention;
                u8 nb = compat_summaries[fb].naming_convention;

                // Build contig maps.
                std::map<std::string, u32> map_a, map_b;
                for (u32 i = 0; i < file_data[fa].contigs.size(); ++i) map_a[file_data[fa].contigs[i].name] = i;
                for (u32 i = 0; i < file_data[fb].contigs.size(); ++i) map_b[file_data[fb].contigs[i].name] = i;

                // Naming convention mismatch.
                if (na != 0 && nb != 0 && na != nb) {
                    CompatIssue iss{};
                    iss.file_a_id = fa;
                    iss.file_b_id = fb;
                    iss.issue_flags = 0x01;
                    iss.contig_a_id = 0xFFFFFFFF;
                    iss.contig_b_id = 0xFFFFFFFF;
                    issues.push_back(iss);
                }

                // Length mismatches / missing contigs.
                for (auto& [name, ai] : map_a) {
                    auto it = map_b.find(name);
                    if (it == map_b.end()) {
                        // Try chr-prefix normalisation.
                        std::string alt = (name.substr(0, 3) == "chr") ? name.substr(3) : "chr" + name;
                        it = map_b.find(alt);
                    }
                    if (it == map_b.end()) {
                        CompatIssue iss{};
                        iss.file_a_id = fa;
                        iss.file_b_id = fb;
                        iss.issue_flags = 0x04;
                        iss.contig_a_id = ai;
                        iss.contig_b_id = 0xFFFFFFFF;
                        iss.len_a = file_data[fa].contigs[ai].length_bp;
                        issues.push_back(iss);
                    } else {
                        u32 bi = it->second;
                        u64 la = file_data[fa].contigs[ai].length_bp;
                        u64 lb = file_data[fb].contigs[bi].length_bp;
                        if (la != 0 && lb != 0 && la != lb) {
                            CompatIssue iss{};
                            iss.file_a_id = fa;
                            iss.file_b_id = fb;
                            iss.issue_flags = 0x02;
                            iss.contig_a_id = ai;
                            iss.contig_b_id = bi;
                            iss.len_a = la;
                            iss.len_b = lb;
                            issues.push_back(iss);
                        }
                    }
                }
            }
        }

        // ============================================================
        // Assemble sections
        // ============================================================

        // Count region entries.
        u32 fasta_count = 0, vcf_count = 0;
        for (auto& fd : file_data) {
            fasta_count += static_cast<u32>(fd.fasta_entries.size());
            vcf_count += static_cast<u32>(fd.vcf_entries.size());
        }
        u32 region_entry_count = fasta_count + vcf_count;

        // Sec 1: GENOMIC_PARAMS
        GenomicParams gp{};
        gp.file_count = static_cast<u32>(file_count);
        gp.k = k;
        gp.num_hashes = num_hashes;
        gp.seed = seed;
        gp.stranded = stranded ? 1 : 0;
        gp.genomic_file_count = genomic_count;
        gp.region_entry_count = region_entry_count;
        {
            std::vector<u8> sec(sizeof(gp));
            std::memcpy(sec.data(), &gp, sizeof(gp));
            writer.add_section(SEC_GENOMIC_PARAMS, sec);
        }

        // Sec 2: GENOMIC_KMER_SKETCHES
        {
            std::vector<u8> sec(file_count * num_hashes * sizeof(u64));
            for (u32 fi = 0; fi < file_count; ++fi) {
                std::memcpy(sec.data() + fi * num_hashes * sizeof(u64), file_data[fi].sketch.data(),
                            num_hashes * sizeof(u64));
            }
            writer.add_section(SEC_GENOMIC_SKETCHES, sec);
        }

        // Sec 3: GENOMIC_CONTIG_TABLE
        {
            StringTable strtab;
            // Total contig records
            u32 total_contigs = 0;
            for (auto& fd : file_data) total_contigs += static_cast<u32>(fd.contigs.size());

            // Pre-build string offsets
            std::vector<ContigRecord> all_cr;
            std::vector<FileContigDir> dirs(file_count);
            u32 contig_list_start = 0;
            for (u32 fi = 0; fi < file_count; ++fi) {
                dirs[fi].contig_list_start = contig_list_start;
                dirs[fi].contig_count = static_cast<u32>(file_data[fi].contigs.size());
                dirs[fi].file_type = static_cast<u8>(file_data[fi].type);
                for (auto& c : file_data[fi].contigs) {
                    ContigRecord cr{};
                    cr.name_offset = strtab.add(c.name);
                    cr.length_bp = c.length_bp;
                    cr.seq_checksum = c.seq_checksum;
                    all_cr.push_back(cr);
                }
                contig_list_start += dirs[fi].contig_count;
            }

            // Layout: u32 file_count | u32 contig_count | u32 string_table_offset
            //       + FileContigDir[file_count]
            //       + ContigRecord[total_contigs]
            //       + string table

            u32 header_bytes = 3 * sizeof(u32);
            u32 dirs_bytes = static_cast<u32>(file_count * sizeof(FileContigDir));
            u32 records_bytes = static_cast<u32>(all_cr.size() * sizeof(ContigRecord));
            u32 strtab_offset = header_bytes + dirs_bytes + records_bytes;

            std::vector<u8> sec(strtab_offset + strtab.size());
            u8* wp = sec.data();

            u32 fc = static_cast<u32>(file_count);
            u32 tc = total_contigs;
            u32 so = strtab_offset;
            std::memcpy(wp, &fc, 4);
            wp += 4;
            std::memcpy(wp, &tc, 4);
            wp += 4;
            std::memcpy(wp, &so, 4);
            wp += 4;
            for (auto& d : dirs) {
                std::memcpy(wp, &d, sizeof(d));
                wp += sizeof(d);
            }
            for (auto& r : all_cr) {
                std::memcpy(wp, &r, sizeof(r));
                wp += sizeof(r);
            }
            std::memcpy(wp, strtab.data().data(), strtab.size());

            writer.add_section(SEC_GENOMIC_CONTIGS, sec);
        }

        // Sec 4: GENOMIC_REGION_INDEX
        {
            u32 bam_count = 0;
            size_t sec_bytes =
                3 * sizeof(u32) + fasta_count * sizeof(FastaRegionEntry) + vcf_count * sizeof(VcfBinEntry);
            std::vector<u8> sec(sec_bytes);
            u8* wp = sec.data();

            std::memcpy(wp, &fasta_count, 4);
            wp += 4;
            std::memcpy(wp, &vcf_count, 4);
            wp += 4;
            std::memcpy(wp, &bam_count, 4);
            wp += 4;

            for (auto& fd : file_data) {
                for (auto& e : fd.fasta_entries) {
                    std::memcpy(wp, &e, sizeof(e));
                    wp += sizeof(e);
                }
            }
            for (auto& fd : file_data) {
                for (auto& e : fd.vcf_entries) {
                    std::memcpy(wp, &e, sizeof(e));
                    wp += sizeof(e);
                }
            }
            writer.add_section(SEC_GENOMIC_REGIONS, sec);
        }

        // Sec 5: GENOMIC_COMPAT_TABLE
        {
            u32 fc = static_cast<u32>(file_count);
            u32 ic = static_cast<u32>(issues.size());
            size_t sec_bytes =
                2 * sizeof(u32) + file_count * sizeof(FileCompatSummary) + issues.size() * sizeof(CompatIssue);
            std::vector<u8> sec(sec_bytes);
            u8* wp = sec.data();
            std::memcpy(wp, &fc, 4);
            wp += 4;
            std::memcpy(wp, &ic, 4);
            wp += 4;
            for (auto& s : compat_summaries) {
                std::memcpy(wp, &s, sizeof(s));
                wp += sizeof(s);
            }
            for (auto& i : issues) {
                std::memcpy(wp, &i, sizeof(i));
                wp += sizeof(i);
            }
            writer.add_section(SEC_GENOMIC_COMPAT, sec);
        }

        if (!issues.empty()) {
            std::cerr << "WARN  " << issues.size() << " compatibility issue(s) detected between genomic files.\n"
                      << "      Use 'mar search ... --with format=json' to inspect.\n";
        }
        std::cerr << "Genomic index complete: " << genomic_count << " genomic files\n";
    }

    void show_help() const override {
        std::cout << R"(Genomic index build options (--with key=value):
  k=N              K-mer size for similarity sketching (default: 21)
  num_hashes=N     Sketch size (default: 256)
  seed=S           Hash seed (default: 42)
  stranded=0|1     0=canonical k-mers (default), 1=forward only
  vcf_bin_size=N   VCF region bin size in bp (default: 65536)

Search options (--with key=value):
  topk=N           Maximum results (default: 10)
  format=X         text, json, filenames (default: text)
  file=NAME        Compare to in-archive file (similarity mode)
  strict_contigs=true  Disable automatic chr-prefix renaming
  --extract        Write raw sequence/records to stdout (region mode)

Query formats:
  FILE.fa          Similarity search (external reference)
  chr1:1000-2000   Region query (1-based inclusive; FASTA / VCF)
  chrM             Whole contig
  --with file=X    Similarity search (in-archive file)

Examples:
  mar index -i ref.mar --type genomic
  mar search -i ref.mar --index ref.genomic.mai chr1:1000000-2000000
  mar search -i ref.mar --index ref.genomic.mai chr1:1000000-2000000 --extract
  mar search -i ref.mar --index ref.genomic.mai query.fa --with topk=5
)";
    }
};

// ============================================================================
// GenomicSearcher
// ============================================================================

// Parse a region string like "chr1:1000000-2000000" or "chrM" or "chr1:1000000"
struct Region {
    std::string contig;
    u64 start = 1;         // 1-based inclusive
    u64 end = UINT64_MAX;  // 1-based inclusive; UINT64_MAX = whole contig
};

static bool parse_region(const std::string& q, Region& out) {
    static const std::regex re_region(R"(^([^:]+):(\d+)(?:-(\d+))?$)");
    std::smatch m;
    if (std::regex_match(q, m, re_region)) {
        out.contig = m[1];
        out.start = std::stoull(m[2]);
        out.end = m[3].matched ? std::stoull(m[3]) : UINT64_MAX;
        if (out.start == 0)
            out.start = 1;
        return true;
    }
    // Bare contig name with no colon.
    if (q.find(':') == std::string::npos && q.find('/') == std::string::npos && q.find('.') == std::string::npos) {
        out.contig = q;
        out.start = 1;
        out.end = UINT64_MAX;
        return true;
    }
    return false;
}

static bool looks_like_file(const std::string& q) {
    if (q.find('/') != std::string::npos)
        return true;
    static const char* exts[] = {".fa", ".fasta", ".fq", ".fastq", ".vcf", ".bam", nullptr};
    for (int i = 0; exts[i]; ++i) {
        size_t el = std::strlen(exts[i]);
        if (q.size() >= el && q.compare(q.size() - el, el, exts[i]) == 0)
            return true;
    }
    return false;
}

class GenomicSearcher : public Searcher {
public:
    bool supports_type(MAIIndexType type) const override { return type == MAIIndexType::Genomic; }

    std::vector<SearchResult> search(const MarReader& archive, const MAIReader& index, const std::string& query,
                                     const IndexOptions& opts) override {
        // Load params
        size_t psz = 0;
        const u8* pp = index.get_section_ptr(SEC_GENOMIC_PARAMS, &psz);
        if (!pp || psz < sizeof(GenomicParams)) {
            throw std::runtime_error("Genomic index: PARAMS section missing or corrupt");
        }
        GenomicParams gp;
        std::memcpy(&gp, pp, sizeof(gp));

        // Emit any compat warnings
        emit_compat_warnings(index, archive);

        // Dispatch based on query format
        Region region;
        bool is_region = (!query.empty()) && parse_region(query, region);
        bool is_file = (!query.empty()) && looks_like_file(query);
        bool have_file_param = opts.has("file");

        if (is_region) {
            return region_search(archive, index, region, opts, gp);
        } else if (is_file || have_file_param) {
            return similarity_search(archive, index, query, opts, gp);
        } else {
            throw std::runtime_error(
                "Genomic search: provide a region (e.g. chr1:1000-2000), "
                "a file path, or --with file=NAME");
        }
    }

private:
    // ----------------------------------------------------------------
    // Similarity search
    // ----------------------------------------------------------------
    std::vector<SearchResult> similarity_search(const MarReader& archive, const MAIReader& index,
                                                const std::string& query, const IndexOptions& opts,
                                                const GenomicParams& gp) {
        const u32 file_count = gp.file_count;
        const u32 num_hashes = gp.num_hashes;
        const u32 k = gp.k;
        const u64 seed = gp.seed;
        const bool stranded = (gp.stranded != 0);

        size_t sketch_sz = 0;
        const u8* sketch_data = index.get_section_ptr(SEC_GENOMIC_SKETCHES, &sketch_sz);
        if (!sketch_data)
            throw std::runtime_error("Genomic sketches section missing");

        // Build query sketch
        std::vector<u64> q_sketch;
        if (opts.has("file")) {
            auto found = archive.find_file(opts.get("file"));
            if (!found)
                throw std::runtime_error("File not found: " + opts.get("file"));
            u32 tid = static_cast<u32>(found->first);
            const u64* sp = reinterpret_cast<const u64*>(sketch_data) + tid * num_hashes;
            q_sketch.assign(sp, sp + num_hashes);
        } else {
            std::ifstream in(query, std::ios::binary);
            if (!in)
                throw std::runtime_error("Cannot open: " + query);
            std::vector<u8> data(std::istreambuf_iterator<char>(in), {});
            auto contigs = parse_fasta(data);
            std::string seq;
            for (auto& c : contigs) seq += c.full_sequence;
            q_sketch = sketch_sequence(seq, k, num_hashes, seed, !stranded);
        }

        std::vector<SearchResult> results;
        for (u32 fi = 0; fi < file_count; ++fi) {
            const u64* sp = reinterpret_cast<const u64*>(sketch_data) + fi * num_hashes;
            double score = jaccard_sketch(q_sketch.data(), sp, num_hashes);
            if (score <= 0.0)
                continue;

            SearchResult r;
            r.file_id = fi;
            auto n = archive.get_name(fi);
            r.filename = n ? *n : "(unknown)";
            r.score = score;
            r.metadata["jaccard"] = [&] {
                std::ostringstream ss;
                ss << std::fixed << std::setprecision(4) << score;
                return ss.str();
            }();
            results.push_back(std::move(r));
        }

        std::sort(results.begin(), results.end(),
                  [](const SearchResult& a, const SearchResult& b) { return a.score > b.score; });

        size_t topk = 10;
        if (opts.has("topk"))
            topk = std::stoul(opts.get("topk"));
        if (results.size() > topk)
            results.resize(topk);
        return results;
    }

    // ----------------------------------------------------------------
    // Region search / extraction
    // ----------------------------------------------------------------
    std::vector<SearchResult> region_search(const MarReader& archive, const MAIReader& index, const Region& region,
                                            const IndexOptions& opts, const GenomicParams& gp) {
        const bool do_extract = (opts.get("extract") == "true");
        const bool strict = (opts.get("strict_contigs") == "true");

        // Load contig table
        size_t csz = 0;
        const u8* contig_sec = index.get_section_ptr(SEC_GENOMIC_CONTIGS, &csz);
        if (!contig_sec)
            throw std::runtime_error("Genomic contig section missing");

        const u8* cp = contig_sec;
        u32 fc, tc, string_table_offset;
        std::memcpy(&fc, cp, 4);
        cp += 4;
        std::memcpy(&tc, cp, 4);
        cp += 4;
        std::memcpy(&string_table_offset, cp, 4);
        cp += 4;

        const FileContigDir* dirs = reinterpret_cast<const FileContigDir*>(cp);
        const ContigRecord* recs = reinterpret_cast<const ContigRecord*>(cp + fc * sizeof(FileContigDir));
        const char* strtab = reinterpret_cast<const char*>(contig_sec + string_table_offset);

        // Load region index
        size_t rsz = 0;
        const u8* region_sec = index.get_section_ptr(SEC_GENOMIC_REGIONS, &rsz);
        u32 fasta_count = 0, vcf_count = 0, bam_count = 0;
        const FastaRegionEntry* fasta_entries = nullptr;
        const VcfBinEntry* vcf_entries = nullptr;
        if (region_sec && rsz >= 12) {
            std::memcpy(&fasta_count, region_sec, 4);
            std::memcpy(&vcf_count, region_sec + 4, 4);
            std::memcpy(&bam_count, region_sec + 8, 4);
            fasta_entries = reinterpret_cast<const FastaRegionEntry*>(region_sec + 12);
            vcf_entries =
                reinterpret_cast<const VcfBinEntry*>(region_sec + 12 + fasta_count * sizeof(FastaRegionEntry));
        }

        std::vector<SearchResult> results;

        for (u32 fi = 0; fi < std::min(fc, gp.file_count); ++fi) {
            const FileContigDir& dir = dirs[fi];
            if (dir.contig_count == 0)
                continue;

            // Find the contig matching the region query.
            u32 matched_ci = UINT32_MAX;
            for (u32 ci = 0; ci < dir.contig_count; ++ci) {
                const ContigRecord& rec = recs[dir.contig_list_start + ci];
                std::string cname(strtab + rec.name_offset);

                bool match = (cname == region.contig);
                if (!match && !strict) {
                    // Try chr-prefix normalisation.
                    std::string alt = (cname.substr(0, 3) == "chr") ? cname.substr(3) : "chr" + cname;
                    match = (alt == region.contig);
                    if (match) {
                        std::cerr << "WARN  " << cname << " renamed to " << region.contig
                                  << " for matching (use --with strict_contigs=true to disable)\n";
                    }
                }
                if (match) {
                    matched_ci = ci;
                    break;
                }
            }

            if (matched_ci == UINT32_MAX)
                continue;

            const ContigRecord& crec = recs[dir.contig_list_start + matched_ci];
            u64 reg_end = (region.end == UINT64_MAX) ? crec.length_bp : region.end;

            auto name_opt = archive.get_name(fi);
            std::string fname = name_opt ? *name_opt : "(unknown)";

            SearchResult r;
            r.file_id = fi;
            r.filename = fname;
            r.score = 1.0;

            GenomicFileType ftype = static_cast<GenomicFileType>(dir.file_type);
            std::string type_str = (ftype == GenomicFileType::FASTA   ? "fasta"
                                    : ftype == GenomicFileType::FASTQ ? "fastq"
                                    : ftype == GenomicFileType::VCF   ? "vcf"
                                    : ftype == GenomicFileType::BAM   ? "bam"
                                                                      : "bcf");
            r.metadata["type"] = type_str;
            r.metadata["contig"] = region.contig;
            r.metadata["start"] = std::to_string(region.start);
            r.metadata["end"] = std::to_string(reg_end);

            if (do_extract && ftype == GenomicFileType::FASTA && fasta_entries) {
                // Find the FASTA region entry for this (file, contig) pair.
                for (u32 ei = 0; ei < fasta_count; ++ei) {
                    const FastaRegionEntry& fe = fasta_entries[ei];
                    if (fe.file_id != fi || fe.contig_id != matched_ci)
                        continue;

                    // Compute byte range within the uncompressed file stream.
                    u64 s0 = region.start - 1;  // 0-based
                    u64 e0 = reg_end;           // 0-based exclusive

                    u64 row0 = s0 / fe.bases_per_line;
                    u64 col0 = s0 % fe.bases_per_line;
                    u64 begin_byte = fe.file_byte_offset + row0 * fe.bytes_per_line + col0;

                    u64 row1 = (e0 - 1) / fe.bases_per_line;
                    u64 col1 = (e0 - 1) % fe.bases_per_line;
                    u64 end_byte = fe.file_byte_offset + row1 * fe.bytes_per_line + col1 + 1;

                    // Read uncompressed file content from archive.
                    auto file_data = const_cast<MarReader&>(archive).read_file(fi);
                    if (begin_byte < file_data.size()) {
                        end_byte = std::min(end_byte, (u64)file_data.size());
                        // Emit FASTA header + sequence to stdout.
                        std::cout << ">" << region.contig << ":" << region.start << "-" << reg_end << "\n";
                        // Extract bases (skip newlines in source).
                        const char* seq_start = reinterpret_cast<const char*>(file_data.data() + begin_byte);
                        size_t seq_bytes = static_cast<size_t>(end_byte - begin_byte);
                        std::string bases;
                        bases.reserve(reg_end - region.start + 1);
                        for (size_t bi = 0; bi < seq_bytes; ++bi) {
                            char c = seq_start[bi];
                            if (c != '\n' && c != '\r')
                                bases += c;
                        }
                        // Wrap at 60.
                        for (size_t bi = 0; bi < bases.size(); bi += 60) {
                            std::cout << bases.substr(bi, 60) << "\n";
                        }
                    }

                    r.metadata["bp"] = std::to_string(reg_end - region.start + 1);
                    break;
                }
            } else if (do_extract && ftype == GenomicFileType::VCF && vcf_entries) {
                // Find VCF bins covering the region and emit overlapping lines.
                auto file_data = const_cast<MarReader&>(archive).read_file(fi);
                for (u32 ei = 0; ei < vcf_count; ++ei) {
                    const VcfBinEntry& ve = vcf_entries[ei];
                    if (ve.file_id != fi || ve.contig_id != matched_ci)
                        continue;
                    if (ve.bin_end_pos < region.start || ve.bin_start_pos > reg_end)
                        continue;

                    const char* p = reinterpret_cast<const char*>(file_data.data() + ve.file_byte_offset);
                    const char* end = reinterpret_cast<const char*>(file_data.data() + file_data.size());
                    while (p < end) {
                        const char* nl = reinterpret_cast<const char*>(std::memchr(p, '\n', end - p));
                        size_t ll = nl ? (size_t)(nl - p) : (size_t)(end - p);
                        if (ll > 0 && p[0] != '#') {
                            const char* tab = reinterpret_cast<const char*>(std::memchr(p, '\t', ll));
                            if (tab) {
                                u32 pos = static_cast<u32>(std::strtoul(tab + 1, nullptr, 10));
                                if (pos >= region.start && pos <= reg_end) {
                                    std::cout.write(p, ll);
                                    std::cout << "\n";
                                }
                                if (pos > reg_end)
                                    break;
                            }
                        }
                        if (!nl)
                            break;
                        p = nl + 1;
                    }
                }
                r.metadata["type"] = "vcf";
            }

            results.push_back(std::move(r));
        }

        return results;
    }

    // ----------------------------------------------------------------
    // Compatibility warning emission
    // ----------------------------------------------------------------
    void emit_compat_warnings(const MAIReader& index, const MarReader& archive) {
        size_t csz = 0;
        const u8* cp = index.get_section_ptr(SEC_GENOMIC_COMPAT, &csz);
        if (!cp || csz < 8)
            return;

        u32 fc, ic;
        std::memcpy(&fc, cp, 4);
        cp += 4;
        std::memcpy(&ic, cp, 4);
        cp += 4;
        if (ic == 0)
            return;

        cp += fc * sizeof(FileCompatSummary);
        const CompatIssue* issues = reinterpret_cast<const CompatIssue*>(cp);

        for (u32 i = 0; i < ic; ++i) {
            const CompatIssue& iss = issues[i];
            auto na = archive.get_name(iss.file_a_id);
            auto nb = archive.get_name(iss.file_b_id);
            std::string fa = na ? *na : std::to_string(iss.file_a_id);
            std::string fb = nb ? *nb : std::to_string(iss.file_b_id);

            if (iss.issue_flags & 0x01) {
                std::cerr << "WARN  " << fa << " and " << fb
                          << " use different contig naming conventions (chr vs non-chr).\n"
                          << "      Automatic renaming applied. Use --with strict_contigs=true to disable.\n";
            }
            if (iss.issue_flags & 0x02) {
                std::cerr << "WARN  Contig length mismatch between " << fa << " and " << fb << " (" << iss.len_a
                          << " vs " << iss.len_b << " bp)."
                          << " These may be different assemblies.\n";
            }
        }
    }
};

// ============================================================================
// Registration
// ============================================================================

static struct RegisterGenomic {
    RegisterGenomic() {
        IndexRegistry::instance().register_indexer(std::make_unique<GenomicIndexer>());
        IndexRegistry::instance().register_searcher(std::make_unique<GenomicSearcher>());
    }
} g_register_genomic;

}  // namespace mar
