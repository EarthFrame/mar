// Email index: MIME header parsing, inverted index, thread reconstruction.
//
// Sections:
//   1  EMAIL_PARAMS         32-byte fixed header
//   2  EMAIL_HEADERS_TABLE  EmailHeaderEntry * message_count
//   3  EMAIL_STRING_TABLE   u32 count + u32 offsets[count] + NUL-terminated strings
//   4  EMAIL_INVERTED_INDEX TokenDir[] + postings area

#include "mar/index_format.hpp"
#include "mar/index_registry.hpp"

#include <algorithm>
#include <cctype>
#include <cstring>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <regex>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace mar {

// ============================================================================
// Section codes
// ============================================================================

constexpr u32 SEC_EMAIL_PARAMS = 1;
constexpr u32 SEC_EMAIL_HEADERS = 2;
constexpr u32 SEC_EMAIL_STRINGS = 3;
constexpr u32 SEC_EMAIL_INVERTED = 4;

// ============================================================================
// On-disk structs
// ============================================================================

#pragma pack(push, 1)

struct EmailParams {  // 32 bytes
    u32 file_count;
    u32 message_count;
    u32 token_count;
    u32 thread_count;
    u8 version;
    u8 reserved[15];
};

struct EmailHeaderEntry {  // 48 bytes
    u32 file_id;
    u32 message_idx;
    u32 from_str;  // index into string offset array
    u32 to_str;
    u32 subject_str;
    u32 msgid_str;
    u64 date_epoch;      // Unix epoch seconds; 0 if unknown
    u32 thread_root_id;  // message index of thread root
    u8 reserved[12];
};

struct TokenDir {
    u32 token_hash;
    u32 postings_offset;  // byte offset within postings blob
    u32 postings_count;   // number of delta-encoded message IDs
};

#pragma pack(pop)

static_assert(sizeof(EmailParams) == 32, "EmailParams must be 32 bytes");
static_assert(sizeof(EmailHeaderEntry) == 48, "EmailHeaderEntry must be 48 bytes");

// ============================================================================
// String table
// ============================================================================

class EmailStringTable {
public:
    u32 add(const std::string& s) {
        auto it = cache_.find(s);
        if (it != cache_.end())
            return it->second;
        u32 idx = static_cast<u32>(strings_.size());
        cache_[s] = idx;
        strings_.push_back(s);
        return idx;
    }

    // Serialise: [u32 count][u32 offsets[count]][NUL-terminated strings...]
    std::vector<u8> serialise() const {
        // Compute offsets
        std::vector<u32> offsets;
        offsets.reserve(strings_.size());
        u32 off = 0;
        for (auto& s : strings_) {
            offsets.push_back(off);
            off += static_cast<u32>(s.size()) + 1;
        }

        u32 count = static_cast<u32>(strings_.size());
        size_t total = sizeof(u32)            // count
                       + count * sizeof(u32)  // offsets
                       + off;                 // string bytes
        std::vector<u8> out(total);
        u8* p = out.data();
        std::memcpy(p, &count, 4);
        p += 4;
        std::memcpy(p, offsets.data(), count * 4);
        p += count * 4;
        for (auto& s : strings_) {
            std::memcpy(p, s.data(), s.size());
            p += s.size();
            *p++ = '\0';
        }
        return out;
    }

    const std::string& at(u32 idx) const { return strings_.at(idx); }
    size_t size() const { return strings_.size(); }

    // Deserialise helpers (for searching)
    static std::string read_string(const u8* sec, u32 str_idx) {
        const u8* p = sec;
        u32 count;
        std::memcpy(&count, p, 4);
        p += 4;
        if (str_idx >= count)
            return {};
        const u32* offsets = reinterpret_cast<const u32*>(p);
        u32 off = offsets[str_idx];
        const char* s = reinterpret_cast<const char*>(p + count * 4 + off);
        return std::string(s);
    }

private:
    std::vector<std::string> strings_;
    std::map<std::string, u32> cache_;
};

// ============================================================================
// Murmur-inspired 32-bit token hash (stable)
// ============================================================================

static u32 token_hash32(const std::string& s) {
    u32 h = 0x811c9dc5u;
    for (unsigned char c : s) {
        h ^= static_cast<u32>(c);
        h *= 0x01000193u;
    }
    return h;
}

// ============================================================================
// Simple RFC 2822 date parser (best-effort)
// ============================================================================

static const char* MONTH_NAMES[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun",  "Jul",
                                    "Aug", "Sep", "Oct", "Nov", "Dec", nullptr};

static u64 parse_rfc2822_date(const std::string& val) {
    if (val.empty())
        return 0;
    // Very simplified: look for "DD Mon YYYY HH:MM:SS"
    int day = 0, year = 0, hour = 0, min = 0, sec = 0;
    int mon = 0;
    char mon_str[8] = {};

    std::istringstream ss(val);
    std::string tok;
    // Skip optional weekday
    std::getline(ss, tok, ',');
    if (tok.size() == 3 && std::isalpha(tok[0])) {
        // Had a weekday -- tok consumed. Now parse remainder.
    } else {
        // No weekday -- reparse
        ss.str(val);
        ss.clear();
        tok = "";
    }

    ss >> day >> mon_str >> year >> tok;  // tok = HH:MM:SS
    if (day == 0 || year < 1970)
        return 0;
    for (int m = 0; MONTH_NAMES[m]; ++m) {
        if (strncmp(mon_str, MONTH_NAMES[m], 3) == 0) {
            mon = m + 1;
            break;
        }
    }
    if (mon == 0)
        return 0;
    sscanf(tok.c_str(), "%d:%d:%d", &hour, &min, &sec);

    struct tm t{};
    t.tm_year = year - 1900;
    t.tm_mon = mon - 1;
    t.tm_mday = day;
    t.tm_hour = hour;
    t.tm_min = min;
    t.tm_sec = sec;
    time_t epoch = mktime(&t);
    return epoch < 0 ? 0 : static_cast<u64>(epoch);
}

// ============================================================================
// MIME header parser
// ============================================================================

struct MimeHeaders {
    std::string from, to, cc, subject, message_id, references, in_reply_to, date;
};

static std::string& header_ref(MimeHeaders& h, const std::string& field) {
    if (field == "from")
        return h.from;
    if (field == "to")
        return h.to;
    if (field == "cc")
        return h.cc;
    if (field == "subject")
        return h.subject;
    if (field == "message-id")
        return h.message_id;
    if (field == "references")
        return h.references;
    if (field == "in-reply-to")
        return h.in_reply_to;
    if (field == "date")
        return h.date;
    static std::string discard;
    discard.clear();
    return discard;
}

static MimeHeaders parse_mime_headers(const std::string& text) {
    MimeHeaders h;
    std::istringstream ss(text);
    std::string line, last_field;
    while (std::getline(ss, line)) {
        if (!line.empty() && line.back() == '\r')
            line.pop_back();
        if (line.empty())
            break;  // end of headers

        if (std::isspace((unsigned char)line[0]) && !last_field.empty()) {
            auto& target = header_ref(h, last_field);
            target += ' ';
            target += line.substr(1);
            continue;
        }

        auto colon = line.find(':');
        if (colon == std::string::npos)
            continue;
        std::string field = line.substr(0, colon);
        std::string value = (colon + 1 < line.size()) ? line.substr(colon + 2) : "";
        while (!value.empty() && std::isspace((unsigned char)value[0])) value.erase(value.begin());

        std::string fl = field;
        for (char& c : fl) c = static_cast<char>(std::tolower((unsigned char)c));

        auto& target = header_ref(h, fl);
        if (!target.empty()) {
            target += ' ';
            target += value;
        } else
            target = value;
        last_field = fl;
    }
    return h;
}

// ============================================================================
// Tokenizer
// ============================================================================

static const std::set<std::string> STOPWORDS = {"a",    "an",  "the", "and",  "or",   "of",  "in",  "to",
                                                "is",   "it",  "for", "with", "on",   "at",  "be",  "this",
                                                "that", "are", "was", "by",   "from", "as",  "but", "not",
                                                "have", "he",  "she", "we",   "they", "you", "i",   "do",
                                                "will", "can", "has", "had",  "his",  "her"};

static std::vector<std::string> tokenise(const std::string& text) {
    std::vector<std::string> tokens;
    std::string tok;
    for (unsigned char c : text) {
        if (std::isalnum(c)) {
            tok += static_cast<char>(std::tolower(c));
        } else {
            if (!tok.empty()) {
                if (tok.size() >= 2 && STOPWORDS.find(tok) == STOPWORDS.end()) {
                    tokens.push_back(tok);
                }
                tok.clear();
            }
        }
    }
    if (!tok.empty() && tok.size() >= 2 && STOPWORDS.find(tok) == STOPWORDS.end()) {
        tokens.push_back(tok);
    }
    return tokens;
}

// ============================================================================
// Message parsed from an .eml or .mbox file
// ============================================================================

struct ParsedMessage {
    u32 file_id;
    u32 message_idx;
    MimeHeaders headers;
    std::string body_preview;  // first 4 KB
};

static std::vector<ParsedMessage> parse_file(u32 file_id, const std::vector<u8>& data, const std::string& name) {
    std::vector<ParsedMessage> msgs;
    std::string text(reinterpret_cast<const char*>(data.data()), data.size());

    bool is_mbox = (name.size() >= 5 && name.substr(name.size() - 5) == ".mbox") ||
                   (!text.empty() && text.substr(0, 5) == "From ");

    if (!is_mbox) {
        ParsedMessage m;
        m.file_id = file_id;
        m.message_idx = 0;
        m.headers = parse_mime_headers(text);
        // Find body (after blank line)
        auto blank = text.find("\n\n");
        if (blank != std::string::npos) {
            size_t body_start = blank + 2;
            size_t body_end = std::min(body_start + 4096, text.size());
            m.body_preview = text.substr(body_start, body_end - body_start);
        }
        msgs.push_back(std::move(m));
        return msgs;
    }

    // mbox: split on "From " lines.
    size_t pos = 0;
    u32 idx = 0;
    while (pos < text.size()) {
        size_t next_from = text.find("\nFrom ", pos + 1);
        size_t msg_end = (next_from == std::string::npos) ? text.size() : next_from;

        // Skip the "From " line itself.
        size_t msg_start = text.find('\n', pos);
        if (msg_start == std::string::npos)
            break;
        ++msg_start;

        std::string msg_text = text.substr(msg_start, msg_end - msg_start);
        ParsedMessage m;
        m.file_id = file_id;
        m.message_idx = idx++;
        m.headers = parse_mime_headers(msg_text);
        auto blank = msg_text.find("\n\n");
        if (blank != std::string::npos) {
            size_t bs = blank + 2;
            size_t be = std::min(bs + 4096, msg_text.size());
            m.body_preview = msg_text.substr(bs, be - bs);
        }
        msgs.push_back(std::move(m));
        pos = (next_from == std::string::npos) ? text.size() : next_from + 1;
    }
    return msgs;
}

// ============================================================================
// EmailIndexer
// ============================================================================

class EmailIndexer : public Indexer {
public:
    const char* type_name() const override { return "email"; }
    MAIIndexType index_type() const override { return MAIIndexType::Email; }

    void build(const MarReader& reader, MAIWriter& writer, const IndexOptions&) override {
        const size_t file_count = reader.file_count();

        EmailStringTable strtab;
        std::vector<EmailHeaderEntry> header_table;
        // token_hash -> sorted message IDs (deduplicated)
        std::map<u32, std::vector<u32>> postings;
        // thread reconstruction: message-id -> root message index
        std::map<std::string, u32> msgid_to_idx;
        std::map<u32, u32> thread_root;  // msg_idx -> root msg_idx (union-find)

        u32 msg_global_idx = 0;

        std::cerr << "Building email index...\n";

        for (u32 fi = 0; fi < file_count; ++fi) {
            auto entry_opt = reader.get_file_entry(fi);
            if (!entry_opt || entry_opt->entry_type != EntryType::RegularFile)
                continue;

            auto name_opt = reader.get_name(fi);
            std::string name = name_opt ? *name_opt : "";

            // Only index .eml and .mbox files.
            bool is_email = false;
            if (name.size() >= 4 && name.substr(name.size() - 4) == ".eml")
                is_email = true;
            if (name.size() >= 5 && name.substr(name.size() - 5) == ".mbox")
                is_email = true;
            if (!is_email)
                continue;

            auto data = const_cast<MarReader&>(reader).read_file(fi);
            auto msgs = parse_file(fi, data, name);
            std::cerr << "  " << name << ": " << msgs.size() << " message(s)\n";

            for (auto& m : msgs) {
                u32 msg_idx = msg_global_idx++;

                // Register message-id.
                if (!m.headers.message_id.empty()) {
                    msgid_to_idx[m.headers.message_id] = msg_idx;
                }
                thread_root[msg_idx] = msg_idx;  // default: own root

                // Tokenise subject + body preview.
                std::string content = m.headers.subject + " " + m.body_preview;
                auto tokens = tokenise(content);
                for (auto& tok : tokens) {
                    u32 h = token_hash32(tok);
                    postings[h].push_back(msg_idx);
                }

                EmailHeaderEntry hdr{};
                hdr.file_id = fi;
                hdr.message_idx = m.message_idx;
                hdr.from_str = strtab.add(m.headers.from);
                hdr.to_str = strtab.add(m.headers.to);
                hdr.subject_str = strtab.add(m.headers.subject);
                hdr.msgid_str = strtab.add(m.headers.message_id);
                hdr.date_epoch = parse_rfc2822_date(m.headers.date);
                hdr.thread_root_id = msg_idx;  // updated below
                header_table.push_back(hdr);
            }
        }

        // Thread reconstruction: resolve In-Reply-To and References.
        for (u32 msg_idx = 0; msg_idx < header_table.size(); ++msg_idx) {
            auto& hdr = header_table[msg_idx];
            u32 fi = hdr.file_id;
            auto data = const_cast<MarReader&>(reader).read_file(fi);
            auto msgs = parse_file(fi, data, reader.get_name(fi).value_or(""));
            if (hdr.message_idx < msgs.size()) {
                const auto& m = msgs[hdr.message_idx];
                std::string parent_id = m.headers.in_reply_to;
                if (parent_id.empty() && !m.headers.references.empty()) {
                    // Last token in References is the direct parent.
                    auto sp = m.headers.references.rfind(' ');
                    parent_id = (sp == std::string::npos) ? m.headers.references : m.headers.references.substr(sp + 1);
                }
                auto it = msgid_to_idx.find(parent_id);
                if (it != msgid_to_idx.end()) {
                    // Walk up to root.
                    u32 root = it->second;
                    while (thread_root[root] != root) root = thread_root[root];
                    thread_root[msg_idx] = root;
                    hdr.thread_root_id = root;
                }
            }
        }

        // Deduplicate postings lists.
        for (auto& [h, ids] : postings) {
            std::sort(ids.begin(), ids.end());
            ids.erase(std::unique(ids.begin(), ids.end()), ids.end());
        }

        u32 message_count = static_cast<u32>(header_table.size());
        u32 token_count = static_cast<u32>(postings.size());

        // Count unique thread roots.
        std::set<u32> roots;
        for (auto& [k, v] : thread_root) roots.insert(v);
        u32 thread_count = static_cast<u32>(roots.size());

        std::cerr << "Email index: " << message_count << " messages, " << token_count << " tokens, " << thread_count
                  << " threads\n";

        // Sec 1: EMAIL_PARAMS
        EmailParams ep{};
        ep.file_count = static_cast<u32>(file_count);
        ep.message_count = message_count;
        ep.token_count = token_count;
        ep.thread_count = thread_count;
        ep.version = 1;
        {
            std::vector<u8> sec(sizeof(ep));
            std::memcpy(sec.data(), &ep, sizeof(ep));
            writer.add_section(SEC_EMAIL_PARAMS, sec);
        }

        // Sec 2: EMAIL_HEADERS_TABLE
        {
            std::vector<u8> sec(message_count * sizeof(EmailHeaderEntry));
            std::memcpy(sec.data(), header_table.data(), sec.size());
            writer.add_section(SEC_EMAIL_HEADERS, sec);
        }

        // Sec 3: EMAIL_STRING_TABLE
        writer.add_section(SEC_EMAIL_STRINGS, strtab.serialise());

        // Sec 4: EMAIL_INVERTED_INDEX
        // Layout: TokenDir[token_count] + delta-encoded postings blob
        {
            // Build sorted directory + postings blob.
            std::vector<TokenDir> dir;
            dir.reserve(token_count);
            std::vector<u8> postings_blob;

            for (auto& [h, ids] : postings) {
                TokenDir td;
                td.token_hash = h;
                td.postings_offset = static_cast<u32>(postings_blob.size());
                td.postings_count = static_cast<u32>(ids.size());
                dir.push_back(td);

                // Delta-encode and store as u32 (simplified; no varint).
                u32 prev = 0;
                for (u32 id : ids) {
                    u32 delta = id - prev;
                    prev = id;
                    u8 buf[4];
                    std::memcpy(buf, &delta, 4);
                    postings_blob.insert(postings_blob.end(), buf, buf + 4);
                }
            }

            // Sort directory by token_hash for binary search.
            std::sort(dir.begin(), dir.end(),
                      [](const TokenDir& a, const TokenDir& b) { return a.token_hash < b.token_hash; });

            size_t sec_bytes = dir.size() * sizeof(TokenDir) + postings_blob.size();
            std::vector<u8> sec(sec_bytes);
            u8* wp = sec.data();
            for (auto& td : dir) {
                std::memcpy(wp, &td, sizeof(td));
                wp += sizeof(td);
            }
            std::memcpy(wp, postings_blob.data(), postings_blob.size());
            writer.add_section(SEC_EMAIL_INVERTED, sec);
        }
    }

    void show_help() const override {
        std::cout << R"(Email index build options:
  (no build-time parameters required)

Search options (--with key=value):
  from=ADDR      Filter by From: address (substring match)
  to=ADDR        Filter by To: address (substring match)
  subject=TEXT   Filter by Subject: (substring match)
  since=DATE     Earliest date (ISO 8601 or epoch seconds)
  until=DATE     Latest date (ISO 8601 or epoch seconds)
  thread=MSGID   Return all messages in the same thread
  topk=N         Maximum results (default: 10)
  format=X       text, json, filenames (default: text)

Positional query: keyword full-text search (space-separated tokens are ORed)

Examples:
  mar index -i mail.mar --type email
  mar search -i mail.mar --index mail.email.mai "project deadline" \
    --with since=2024-01-01 --with format=json
  mar search -i mail.mar --index mail.email.mai \
    --with thread="<abc@mail.example.com>"
)";
    }
};

// ============================================================================
// EmailSearcher
// ============================================================================

static u64 parse_date_filter(const std::string& s) {
    if (s.empty())
        return 0;
    // Try epoch seconds first.
    try {
        u64 v = std::stoull(s);
        if (v > 1000000000ULL)
            return v;  // looks like epoch
    } catch (...) {
    }
    // ISO 8601: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS
    struct tm t{};
    if (sscanf(s.c_str(), "%d-%d-%dT%d:%d:%d", &t.tm_year, &t.tm_mon, &t.tm_mday, &t.tm_hour, &t.tm_min, &t.tm_sec) >=
            3 ||
        sscanf(s.c_str(), "%d-%d-%d", &t.tm_year, &t.tm_mon, &t.tm_mday) == 3) {
        t.tm_year -= 1900;
        t.tm_mon -= 1;
        time_t ep = mktime(&t);
        return ep < 0 ? 0 : static_cast<u64>(ep);
    }
    return 0;
}

class EmailSearcher : public Searcher {
public:
    bool supports_type(MAIIndexType type) const override { return type == MAIIndexType::Email; }

    std::vector<SearchResult> search(const MarReader& archive, const MAIReader& index, const std::string& query,
                                     const IndexOptions& opts) override {
        // Load params
        size_t psz = 0;
        const u8* pp = index.get_section_ptr(SEC_EMAIL_PARAMS, &psz);
        if (!pp || psz < sizeof(EmailParams)) {
            throw std::runtime_error("Email index: PARAMS section missing");
        }
        EmailParams ep;
        std::memcpy(&ep, pp, sizeof(ep));

        // Load header table
        size_t hsz = 0;
        const u8* hp = index.get_section_ptr(SEC_EMAIL_HEADERS, &hsz);
        if (!hp)
            throw std::runtime_error("Email index: HEADERS section missing");
        const EmailHeaderEntry* hdrs = reinterpret_cast<const EmailHeaderEntry*>(hp);

        // Load string table
        size_t ssz = 0;
        const u8* sp = index.get_section_ptr(SEC_EMAIL_STRINGS, &ssz);
        if (!sp)
            throw std::runtime_error("Email index: STRING_TABLE section missing");

        // Load inverted index
        size_t isz = 0;
        const u8* ip = index.get_section_ptr(SEC_EMAIL_INVERTED, &isz);

        u32 msg_count = ep.message_count;
        u32 tok_count = ep.token_count;

        // --- Step 1: keyword query via inverted index ---
        std::set<u32> keyword_hits;
        bool has_keyword_query = !query.empty();

        if (has_keyword_query && ip && tok_count > 0) {
            auto tokens = tokenise(query);
            const TokenDir* dir = reinterpret_cast<const TokenDir*>(ip);
            size_t postings_start = tok_count * sizeof(TokenDir);
            const u8* postings_blob = ip + postings_start;

            for (auto& tok : tokens) {
                u32 h = token_hash32(tok);
                // Binary search in sorted directory.
                size_t lo = 0, hi = tok_count;
                while (lo < hi) {
                    size_t mid = (lo + hi) / 2;
                    if (dir[mid].token_hash < h)
                        lo = mid + 1;
                    else
                        hi = mid;
                }
                if (lo < tok_count && dir[lo].token_hash == h) {
                    const u8* pp2 = postings_blob + dir[lo].postings_offset;
                    u32 prev = 0;
                    for (u32 j = 0; j < dir[lo].postings_count; ++j) {
                        u32 delta;
                        std::memcpy(&delta, pp2, 4);
                        pp2 += 4;
                        prev += delta;
                        keyword_hits.insert(prev);
                    }
                }
            }
        }

        // --- Step 2: header filters ---
        const std::string filter_from = opts.get("from");
        const std::string filter_to = opts.get("to");
        const std::string filter_subject = opts.get("subject");
        const std::string filter_thread = opts.get("thread");
        const u64 since = parse_date_filter(opts.get("since"));
        const u64 until = parse_date_filter(opts.get("until"));

        auto to_lower = [](std::string s) {
            for (char& c : s) c = static_cast<char>(std::tolower((unsigned char)c));
            return s;
        };

        std::vector<SearchResult> results;
        for (u32 mi = 0; mi < msg_count; ++mi) {
            // Apply keyword filter
            if (has_keyword_query && keyword_hits.find(mi) == keyword_hits.end())
                continue;

            const EmailHeaderEntry& hdr = hdrs[mi];

            // Apply header filters
            if (since != 0 && hdr.date_epoch != 0 && hdr.date_epoch < since)
                continue;
            if (until != 0 && hdr.date_epoch != 0 && hdr.date_epoch > until)
                continue;

            if (!filter_from.empty()) {
                std::string from = to_lower(EmailStringTable::read_string(sp, hdr.from_str));
                if (from.find(to_lower(filter_from)) == std::string::npos)
                    continue;
            }
            if (!filter_to.empty()) {
                std::string to = to_lower(EmailStringTable::read_string(sp, hdr.to_str));
                if (to.find(to_lower(filter_to)) == std::string::npos)
                    continue;
            }
            if (!filter_subject.empty()) {
                std::string sub = to_lower(EmailStringTable::read_string(sp, hdr.subject_str));
                if (sub.find(to_lower(filter_subject)) == std::string::npos)
                    continue;
            }
            if (!filter_thread.empty()) {
                // Find messages that share the same thread root as the given message-id.
                u32 target_root = UINT32_MAX;
                for (u32 j = 0; j < msg_count; ++j) {
                    std::string mid = EmailStringTable::read_string(sp, hdrs[j].msgid_str);
                    if (mid == filter_thread) {
                        target_root = hdrs[j].thread_root_id;
                        break;
                    }
                }
                if (target_root == UINT32_MAX || hdr.thread_root_id != target_root)
                    continue;
            }

            // Build result
            SearchResult r;
            r.file_id = hdr.file_id;
            auto n = archive.get_name(hdr.file_id);
            r.filename = n ? *n : "(unknown)";
            r.score = 1.0;

            r.metadata["from"] = EmailStringTable::read_string(sp, hdr.from_str);
            r.metadata["subject"] = EmailStringTable::read_string(sp, hdr.subject_str);
            r.metadata["message_id"] = EmailStringTable::read_string(sp, hdr.msgid_str);
            if (hdr.date_epoch != 0) {
                r.metadata["date"] = std::to_string(hdr.date_epoch);
            }
            r.metadata["thread_root_id"] = std::to_string(hdr.thread_root_id);

            results.push_back(std::move(r));
        }

        // Sort by date descending (newest first).
        std::sort(results.begin(), results.end(), [&](const SearchResult& a, const SearchResult& b) {
            u64 da = (a.metadata.count("date") ? std::stoull(a.metadata.at("date")) : 0);
            u64 db = (b.metadata.count("date") ? std::stoull(b.metadata.at("date")) : 0);
            return da > db;
        });

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

static struct RegisterEmail {
    RegisterEmail() {
        IndexRegistry::instance().register_indexer(std::make_unique<EmailIndexer>());
        IndexRegistry::instance().register_searcher(std::make_unique<EmailSearcher>());
    }
} g_register_email;

}  // namespace mar
