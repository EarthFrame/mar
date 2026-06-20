// Time series index: range and anomaly queries over tabular CSV/TSV data.
//
// No silent autodetection -- ts_col and ts_format are required build parameters.
//
// Sections:
//   1  TS_PARAMS         256-byte fixed header
//   2  TS_METADATA_TABLE TsFileMetadata * file_count
//   3  TS_COL_STATS      header + ColStats[] + string table

#include "mar/index_format.hpp"
#include "mar/index_registry.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstring>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <map>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace mar {

// ============================================================================
// Section codes
// ============================================================================

constexpr u32 SEC_TS_PARAMS = 1;
constexpr u32 SEC_TS_METADATA = 2;
constexpr u32 SEC_TS_COLSTATS = 3;

// ============================================================================
// On-disk structs
// ============================================================================

#pragma pack(push, 1)

struct TsParams {  // 256 bytes
    u32 file_count;
    u32 ts_col_idx;     // 0-based; 0xFFFFFFFF = name-based
    u8 ts_format_enum;  // 1=iso8601, 2=epoch_s, 3=epoch_ms, 4=epoch_us, 5=custom, 0=auto
    u8 delim;           // ASCII delimiter char
    u8 has_header;      // bool
    u8 reserved0;
    u32 skip_rows;
    char ts_col_name[64];
    char ts_format_str[128];  // strptime string for enum==5
    u8 reserved1[48];
};

struct TsFileMetadata {  // 64 bytes
    u64 ts_min;          // epoch ms
    u64 ts_max;          // epoch ms
    u32 row_count;
    u32 col_count;
    u32 value_col_count;
    u32 col_stats_start;       // index into COL_STATS array
    u32 col_names_str_offset;  // offset into COL_STATS string table
    u8 ts_indexed;             // 1=success, 0=skipped
    u8 reserved[27];
};

struct ColStats {
    double mean;
    double stddev;
    double vmin;
    double vmax;
    u64 count;
    u32 col_name_str_offset;
    u32 reserved;
};

#pragma pack(pop)

static_assert(sizeof(TsParams) == 256, "TsParams must be 256 bytes");
static_assert(sizeof(TsFileMetadata) == 64, "TsFileMetadata must be 64 bytes");
static_assert(sizeof(ColStats) == 48, "ColStats must be 48 bytes");

// ============================================================================
// Timestamp parsing
// ============================================================================

enum class TsFormatEnum : u8 {
    Auto = 0,
    ISO8601 = 1,
    EpochS = 2,
    EpochMs = 3,
    EpochUs = 4,
    Custom = 5,
};

static TsFormatEnum parse_format_enum(const std::string& s) {
    if (s == "iso8601")
        return TsFormatEnum::ISO8601;
    if (s == "epoch_s")
        return TsFormatEnum::EpochS;
    if (s == "epoch_ms")
        return TsFormatEnum::EpochMs;
    if (s == "epoch_us")
        return TsFormatEnum::EpochUs;
    if (s == "auto")
        return TsFormatEnum::Auto;
    return TsFormatEnum::Custom;  // custom strptime string
}

// Parse a timestamp value -> epoch milliseconds (i64).
// Returns INT64_MIN on failure.
static i64 parse_timestamp(const std::string& val, TsFormatEnum fmt, const std::string& fmt_str) {
    if (val.empty())
        return INT64_MIN;

    switch (fmt) {
        case TsFormatEnum::EpochS:
            try {
                return static_cast<i64>(std::stoll(val)) * 1000LL;
            } catch (...) {
            }
            break;
        case TsFormatEnum::EpochMs:
            try {
                return std::stoll(val);
            } catch (...) {
            }
            break;
        case TsFormatEnum::EpochUs:
            try {
                return std::stoll(val) / 1000LL;
            } catch (...) {
            }
            break;
        case TsFormatEnum::ISO8601:
        case TsFormatEnum::Custom: {
            const char* fmt_c = (fmt == TsFormatEnum::ISO8601) ? "%Y-%m-%dT%H:%M:%S" : fmt_str.c_str();
            struct tm t{};
            const char* end = strptime(val.c_str(), fmt_c, &t);
            if (end && end >= val.c_str() + val.size() - 6 /* allow timezone suffix */) {
                time_t ep = mktime(&t);
                if (ep >= 0)
                    return static_cast<i64>(ep) * 1000LL;
            }
            // Also try without time component
            fmt_c = "%Y-%m-%d";
            struct tm t2{};
            end = strptime(val.c_str(), fmt_c, &t2);
            if (end) {
                time_t ep = mktime(&t2);
                if (ep >= 0)
                    return static_cast<i64>(ep) * 1000LL;
            }
            break;
        }
        case TsFormatEnum::Auto: {
            // Try iso8601, then epoch
            i64 v = parse_timestamp(val, TsFormatEnum::ISO8601, "");
            if (v != INT64_MIN)
                return v;
            try {
                double d = std::stod(val);
                if (d > 1e12)
                    return static_cast<i64>(d);  // ms
                if (d > 1e9)
                    return static_cast<i64>(d) * 1000;  // s
            } catch (...) {
            }
            break;
        }
    }
    return INT64_MIN;
}

// ============================================================================
// CSV/TSV row splitter
// ============================================================================

static std::vector<std::string> split_row(const std::string& line, char delim) {
    std::vector<std::string> cols;
    std::string cur;
    bool in_quote = false;
    for (char c : line) {
        if (c == '"') {
            in_quote = !in_quote;
        } else if (!in_quote && c == delim) {
            cols.push_back(cur);
            cur.clear();
        } else {
            cur += c;
        }
    }
    cols.push_back(cur);
    return cols;
}

// ============================================================================
// Accumulator for single-pass mean/variance (Welford's)
// ============================================================================

struct RunningStats {
    u64 count = 0;
    double mean = 0.0;
    double m2 = 0.0;
    double vmin = std::numeric_limits<double>::max();
    double vmax = std::numeric_limits<double>::lowest();

    void update(double x) {
        ++count;
        double delta = x - mean;
        mean += delta / count;
        double delta2 = x - mean;
        m2 += delta * delta2;
        if (x < vmin)
            vmin = x;
        if (x > vmax)
            vmax = x;
    }

    double stddev() const { return count < 2 ? 0.0 : std::sqrt(m2 / (count - 1)); }
};

// ============================================================================
// TimeSeriesIndexer
// ============================================================================

class TimeSeriesIndexer : public Indexer {
public:
    const char* type_name() const override { return "timeseries"; }
    MAIIndexType index_type() const override { return MAIIndexType::TimeSeries; }

    void build(const MarReader& reader, MAIWriter& writer, const IndexOptions& opts) override {
        // ------------- Required parameters -------------
        const std::string ts_col_str = opts.get("ts_col");
        const std::string ts_format_str = opts.get("ts_format");

        if (ts_col_str.empty()) {
            throw std::runtime_error(
                "Error: --with ts_col=<name_or_index> is required for timeseries index.\n"
                "       Use --with ts_col=auto to enable best-effort autodetection "
                "(not recommended for production).");
        }
        if (ts_format_str.empty()) {
            throw std::runtime_error(
                "Error: --with ts_format=<format> is required for timeseries index.\n"
                "       Supported: iso8601, epoch_s, epoch_ms, epoch_us, auto, "
                "or a strptime string.");
        }

        // Warn if auto is used.
        if (ts_col_str == "auto" || ts_format_str == "auto") {
            std::cerr << "mar: warning: ts_col=auto or ts_format=auto is not recommended "
                         "for production use. Results may be incorrect.\n";
        }

        // Parse ts_col: index or name
        u32 ts_col_idx = 0xFFFFFFFF;
        bool ts_col_by_name = true;
        try {
            ts_col_idx = static_cast<u32>(std::stoul(ts_col_str));
            ts_col_by_name = false;
        } catch (...) {
            ts_col_by_name = true;  // it's a name
        }

        TsFormatEnum fmt_enum = parse_format_enum(ts_format_str);
        const std::string custom_fmt = (fmt_enum == TsFormatEnum::Custom) ? ts_format_str : "";

        const char delim_char = opts.get("delim", ",")[0];
        const bool has_header = (opts.get("has_header", "true") != "false");
        const u32 skip_rows = static_cast<u32>(std::stoul(opts.get("skip_rows", "0")));
        const std::string value_cols_str = opts.get("value_cols");

        const size_t file_count = reader.file_count();
        std::cerr << "Building timeseries index (ts_col=" << ts_col_str << " ts_format=" << ts_format_str << ")...\n";

        std::vector<TsFileMetadata> meta_table(file_count);
        std::vector<ColStats> all_stats;
        std::vector<u8> col_name_strtab;
        std::map<std::string, u32> strtab_cache;

        auto add_col_name = [&](const std::string& s) -> u32 {
            auto it = strtab_cache.find(s);
            if (it != strtab_cache.end())
                return it->second;
            u32 off = static_cast<u32>(col_name_strtab.size());
            strtab_cache[s] = off;
            col_name_strtab.insert(col_name_strtab.end(), s.begin(), s.end());
            col_name_strtab.push_back('\0');
            return off;
        };

        for (u32 fi = 0; fi < file_count; ++fi) {
            auto entry_opt = reader.get_file_entry(fi);
            if (!entry_opt || entry_opt->entry_type != EntryType::RegularFile)
                continue;

            auto name_opt = reader.get_name(fi);
            std::string name = name_opt ? *name_opt : "";

            // Only process files that look like tabular data.
            bool is_tabular = false;
            if (name.size() >= 4 && name.substr(name.size() - 4) == ".csv")
                is_tabular = true;
            if (name.size() >= 4 && name.substr(name.size() - 4) == ".tsv")
                is_tabular = true;
            if (name.size() >= 4 && name.substr(name.size() - 4) == ".txt")
                is_tabular = true;
            if (!is_tabular)
                continue;

            auto data = const_cast<MarReader&>(reader).read_file(fi);
            std::string text(reinterpret_cast<const char*>(data.data()), data.size());

            TsFileMetadata fm{};
            fm.ts_min = UINT64_MAX;
            fm.ts_max = 0;
            fm.col_stats_start = static_cast<u32>(all_stats.size());

            // Parse lines
            std::istringstream ss(text);
            std::string line;
            u32 line_num = 0;
            std::vector<std::string> col_names;
            u32 resolved_ts_idx = UINT32_MAX;
            std::vector<u32> value_indices;
            std::vector<RunningStats> col_stats_acc;

            while (std::getline(ss, line)) {
                if (!line.empty() && line.back() == '\r')
                    line.pop_back();
                if (line_num++ < skip_rows)
                    continue;

                auto cols = split_row(line, delim_char);

                // Header row
                if (has_header && col_names.empty()) {
                    col_names = cols;
                    fm.col_count = static_cast<u32>(cols.size());

                    // Resolve ts_col
                    if (ts_col_by_name) {
                        for (u32 ci = 0; ci < cols.size(); ++ci) {
                            if (cols[ci] == ts_col_str) {
                                resolved_ts_idx = ci;
                                break;
                            }
                        }
                        // Also try auto
                        if (resolved_ts_idx == UINT32_MAX && ts_col_str == "auto") {
                            for (u32 ci = 0; ci < cols.size(); ++ci) {
                                std::string cn = cols[ci];
                                for (char& c : cn) c = static_cast<char>(std::tolower((unsigned char)c));
                                if (cn == "timestamp" || cn == "time" || cn == "date" || cn == "ts") {
                                    resolved_ts_idx = ci;
                                    std::cerr << "  auto-detected ts_col=" << cols[ci] << " in " << name << "\n";
                                    break;
                                }
                            }
                        }
                    } else {
                        resolved_ts_idx = ts_col_idx;
                    }

                    // Resolve value columns
                    if (value_cols_str.empty()) {
                        for (u32 ci = 0; ci < cols.size(); ++ci) {
                            if (ci != resolved_ts_idx)
                                value_indices.push_back(ci);
                        }
                    } else {
                        std::istringstream vss(value_cols_str);
                        std::string vc;
                        while (std::getline(vss, vc, ',')) {
                            for (u32 ci = 0; ci < cols.size(); ++ci) {
                                if (cols[ci] == vc) {
                                    value_indices.push_back(ci);
                                    break;
                                }
                            }
                        }
                    }
                    col_stats_acc.resize(value_indices.size());
                    continue;
                }

                // Non-header first row when has_header=false
                if (!has_header && col_names.empty()) {
                    fm.col_count = static_cast<u32>(cols.size());
                    resolved_ts_idx = ts_col_by_name ? 0 : ts_col_idx;
                    if (value_cols_str.empty()) {
                        for (u32 ci = 0; ci < cols.size(); ++ci) {
                            if (ci != resolved_ts_idx)
                                value_indices.push_back(ci);
                        }
                    }
                    col_stats_acc.resize(value_indices.size());
                    for (u32 ci = 0; ci < cols.size(); ++ci) {
                        col_names.push_back("col" + std::to_string(ci));
                    }
                }

                if (resolved_ts_idx == UINT32_MAX || resolved_ts_idx >= cols.size()) {
                    ++fm.row_count;
                    continue;
                }

                // Parse timestamp
                i64 ts_ms = parse_timestamp(cols[resolved_ts_idx], fmt_enum, custom_fmt);
                if (ts_ms != INT64_MIN) {
                    u64 ts_u = static_cast<u64>(ts_ms);
                    if (ts_u < fm.ts_min)
                        fm.ts_min = ts_u;
                    if (ts_u > fm.ts_max)
                        fm.ts_max = ts_u;
                }

                // Parse value columns
                for (size_t vi = 0; vi < value_indices.size(); ++vi) {
                    u32 ci = value_indices[vi];
                    if (ci < cols.size()) {
                        try {
                            double v = std::stod(cols[ci]);
                            col_stats_acc[vi].update(v);
                        } catch (...) {
                        }
                    }
                }
                ++fm.row_count;
            }

            if (fm.ts_min == UINT64_MAX)
                fm.ts_min = 0;

            fm.value_col_count = static_cast<u32>(col_stats_acc.size());
            fm.ts_indexed = (resolved_ts_idx != UINT32_MAX) ? 1 : 0;
            fm.col_names_str_offset = 0;  // filled below

            // Build per-file ColStats
            u32 names_offset = static_cast<u32>(col_name_strtab.size());
            fm.col_names_str_offset = names_offset;

            for (size_t vi = 0; vi < col_stats_acc.size(); ++vi) {
                const RunningStats& rs = col_stats_acc[vi];
                u32 ci = value_indices[vi];
                std::string cname = ci < col_names.size() ? col_names[ci] : "col" + std::to_string(ci);

                ColStats cs{};
                cs.mean = rs.mean;
                cs.stddev = rs.stddev();
                cs.vmin = (rs.count > 0) ? rs.vmin : 0.0;
                cs.vmax = (rs.count > 0) ? rs.vmax : 0.0;
                cs.count = rs.count;
                cs.col_name_str_offset = add_col_name(cname);
                all_stats.push_back(cs);
            }

            meta_table[fi] = fm;
            std::cerr << "  [" << fi << "] " << name << ": " << fm.row_count << " rows\n";
        }

        // Sec 1: TS_PARAMS
        TsParams tp{};
        tp.file_count = static_cast<u32>(file_count);
        tp.ts_col_idx = ts_col_by_name ? 0xFFFFFFFF : ts_col_idx;
        tp.ts_format_enum = static_cast<u8>(fmt_enum);
        tp.delim = static_cast<u8>(delim_char);
        tp.has_header = has_header ? 1 : 0;
        tp.skip_rows = skip_rows;
        if (ts_col_by_name) {
            std::strncpy(tp.ts_col_name, ts_col_str.c_str(), sizeof(tp.ts_col_name) - 1);
        }
        if (fmt_enum == TsFormatEnum::Custom) {
            std::strncpy(tp.ts_format_str, custom_fmt.c_str(), sizeof(tp.ts_format_str) - 1);
        }
        {
            std::vector<u8> sec(sizeof(tp));
            std::memcpy(sec.data(), &tp, sizeof(tp));
            writer.add_section(SEC_TS_PARAMS, sec);
        }

        // Sec 2: TS_METADATA_TABLE
        {
            std::vector<u8> sec(file_count * sizeof(TsFileMetadata));
            std::memcpy(sec.data(), meta_table.data(), sec.size());
            writer.add_section(SEC_TS_METADATA, sec);
        }

        // Sec 3: TS_COL_STATS
        {
            u32 total = static_cast<u32>(all_stats.size());
            u32 strtab_offset = 2 * sizeof(u32) + total * sizeof(ColStats);
            size_t sec_bytes = strtab_offset + col_name_strtab.size();
            std::vector<u8> sec(sec_bytes);
            u8* wp = sec.data();
            std::memcpy(wp, &total, 4);
            wp += 4;
            std::memcpy(wp, &strtab_offset, 4);
            wp += 4;
            std::memcpy(wp, all_stats.data(), total * sizeof(ColStats));
            wp += total * sizeof(ColStats);
            std::memcpy(wp, col_name_strtab.data(), col_name_strtab.size());
            writer.add_section(SEC_TS_COLSTATS, sec);
        }

        std::cerr << "Timeseries index complete: " << file_count << " files\n";
    }

    void show_help() const override {
        std::cout << R"(Time series index build options (--with key=value):
  ts_col=NAME|INDEX   Timestamp column name or 0-based index [REQUIRED]
                      Use --with ts_col=auto to enable best-effort autodetection (not recommended)
  ts_format=FORMAT    Timestamp format [REQUIRED]
                      Supported: iso8601, epoch_s, epoch_ms, epoch_us, auto, or strptime string
  delim=CHAR          Delimiter character (default: ','; use '\t' for TSV)
  has_header=true|false  Whether the first row is a header (default: true)
  value_cols=A,B,C    Comma-separated column names/indices to compute stats for (default: all)
  skip_rows=N         Rows to skip before header/data (default: 0)

Search options (--with key=value):
  since=TS            Earliest timestamp (epoch ms, epoch s, or ISO 8601)
  until=TS            Latest timestamp
  col=NAME            Filter to files that have a column matching NAME (partial)
  zscore=N            Files where any column's max deviates > N sigma from its mean
  min=V, max=V        Value range filter applied to all numeric columns
  topk=N              Maximum results (default: 10)
  format=X            text, json, filenames (default: text)

Examples:
  mar index -i sensors.mar --type timeseries \
    --with ts_col=timestamp --with ts_format=iso8601 --with value_cols=temperature,humidity

  mar search -i sensors.mar --index sensors.timeseries.mai \
    --with since=2024-01-01 --with until=2024-01-31 --with format=json

  mar search -i sensors.mar --index sensors.timeseries.mai --with zscore=3.0
)";
    }
};

// ============================================================================
// TimeSeriesSearcher
// ============================================================================

class TimeSeriesSearcher : public Searcher {
public:
    bool supports_type(MAIIndexType type) const override { return type == MAIIndexType::TimeSeries; }

    std::vector<SearchResult> search(const MarReader& archive, const MAIReader& index, const std::string& /*query*/,
                                     const IndexOptions& opts) override {
        // Load params
        size_t psz = 0;
        const u8* pp = index.get_section_ptr(SEC_TS_PARAMS, &psz);
        if (!pp || psz < sizeof(TsParams)) {
            throw std::runtime_error("Timeseries index: PARAMS section missing");
        }
        TsParams tp;
        std::memcpy(&tp, pp, sizeof(tp));

        // Load metadata table
        size_t msz = 0;
        const u8* mp = index.get_section_ptr(SEC_TS_METADATA, &msz);
        if (!mp)
            throw std::runtime_error("Timeseries index: METADATA section missing");
        const TsFileMetadata* meta = reinterpret_cast<const TsFileMetadata*>(mp);
        u32 file_count = tp.file_count;

        // Load col stats
        size_t csz = 0;
        const u8* cp = index.get_section_ptr(SEC_TS_COLSTATS, &csz);
        const ColStats* col_stats_arr = nullptr;
        const char* col_strtab = nullptr;
        u32 total_stats = 0, strtab_off = 0;
        if (cp && csz >= 8) {
            std::memcpy(&total_stats, cp, 4);
            std::memcpy(&strtab_off, cp + 4, 4);
            col_stats_arr = reinterpret_cast<const ColStats*>(cp + 8);
            col_strtab = reinterpret_cast<const char*>(cp + strtab_off);
        }

        // Parse filter parameters
        const std::string& since_str = opts.get("since");
        const std::string& until_str = opts.get("until");
        const std::string& col_filt = opts.get("col");
        const std::string& zscore_s = opts.get("zscore");
        const std::string& minv_s = opts.get("min");
        const std::string& maxv_s = opts.get("max");

        auto parse_ts_ms = [&](const std::string& s) -> u64 {
            if (s.empty())
                return 0;
            i64 v = parse_timestamp(s, TsFormatEnum::Auto, "");
            return v == INT64_MIN ? 0 : static_cast<u64>(v);
        };

        u64 since_ms = parse_ts_ms(since_str);
        u64 until_ms = parse_ts_ms(until_str);
        double zscore_thresh = zscore_s.empty() ? 0.0 : std::stod(zscore_s);
        double minv = minv_s.empty() ? -1e300 : std::stod(minv_s);
        double maxv = maxv_s.empty() ? 1e300 : std::stod(maxv_s);

        std::vector<SearchResult> results;

        for (u32 fi = 0; fi < file_count; ++fi) {
            const TsFileMetadata& m = meta[fi];
            if (!m.ts_indexed)
                continue;

            // Time range overlap filter
            if (since_ms != 0 && m.ts_max != 0 && m.ts_max < since_ms)
                continue;
            if (until_ms != 0 && m.ts_min != 0 && m.ts_min > until_ms)
                continue;

            // Column name filter
            bool col_match = col_filt.empty();
            if (!col_filt.empty() && col_stats_arr && col_strtab) {
                for (u32 si = m.col_stats_start; si < m.col_stats_start + m.value_col_count; ++si) {
                    if (si >= total_stats)
                        break;
                    std::string cname(col_strtab + col_stats_arr[si].col_name_str_offset);
                    if (cname.find(col_filt) != std::string::npos) {
                        col_match = true;
                        break;
                    }
                }
            }
            if (!col_match)
                continue;

            // Value range / zscore filter
            double best_zscore = 0.0;
            std::string zscore_col;
            if ((zscore_thresh > 0.0 || minv > -1e299 || maxv < 1e299) && col_stats_arr) {
                bool passes = true;
                for (u32 si = m.col_stats_start; si < m.col_stats_start + m.value_col_count; ++si) {
                    if (si >= total_stats)
                        break;
                    const ColStats& cs = col_stats_arr[si];
                    if (cs.vmax < minv || cs.vmin > maxv) {
                        passes = false;
                        break;
                    }
                    if (zscore_thresh > 0.0 && cs.stddev > 1e-9) {
                        double z = std::abs(cs.vmax - cs.mean) / cs.stddev;
                        if (z > best_zscore) {
                            best_zscore = z;
                            zscore_col = std::string(col_strtab + cs.col_name_str_offset);
                        }
                    }
                }
                if (!passes)
                    continue;
                if (zscore_thresh > 0.0 && best_zscore < zscore_thresh)
                    continue;
            }

            SearchResult r;
            r.file_id = fi;
            auto n = archive.get_name(fi);
            r.filename = n ? *n : "(unknown)";
            r.score = (zscore_thresh > 0.0) ? best_zscore : 1.0;

            r.metadata["ts_min"] = std::to_string(m.ts_min);
            r.metadata["ts_max"] = std::to_string(m.ts_max);
            r.metadata["row_count"] = std::to_string(m.row_count);
            if (!zscore_col.empty())
                r.metadata["zscore_col"] = zscore_col;
            if (!col_filt.empty())
                r.metadata["matched_col"] = col_filt;

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
};

// ============================================================================
// Registration
// ============================================================================

static struct RegisterTimeSeries {
    RegisterTimeSeries() {
        IndexRegistry::instance().register_indexer(std::make_unique<TimeSeriesIndexer>());
        IndexRegistry::instance().register_searcher(std::make_unique<TimeSeriesSearcher>());
    }
} g_register_timeseries;

}  // namespace mar
