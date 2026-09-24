#include "mar/okf/yaml.hpp"

#include <cctype>
#include <sstream>

namespace mar::okf {
namespace {

bool is_blank_or_comment(const std::string& line) {
    for (char c : line) {
        if (c == ' ') continue;
        if (c == '\t') continue;
        if (c == '#') return true;
        return false;
    }
    return true;
}

std::optional<size_t> indent_of(const std::string& line) {
    size_t n = 0;
    for (char c : line) {
        if (c == ' ') {
            ++n;
        } else if (c == '\t') {
            return std::nullopt;
        } else {
            break;
        }
    }
    return n;
}

std::string trim(const std::string& s) {
    size_t start = 0;
    while (start < s.size() && std::isspace(static_cast<unsigned char>(s[start]))) ++start;
    size_t end = s.size();
    while (end > start && std::isspace(static_cast<unsigned char>(s[end - 1]))) --end;
    return s.substr(start, end - start);
}

std::string trim_right(const std::string& s) {
    size_t end = s.size();
    while (end > 0 && std::isspace(static_cast<unsigned char>(s[end - 1]))) --end;
    return s.substr(0, end);
}

Value parse_scalar(const std::string& raw) {
    const std::string t = trim(raw);
    if (t.empty() || t == "~" || t == "null" || t == "Null" || t == "NULL") {
        return Value::null();
    }
    if (t == "true" || t == "True" || t == "TRUE") return Value::boolean(true);
    if (t == "false" || t == "False" || t == "FALSE") return Value::boolean(false);
    if (!t.empty() && (t.front() == '"' || t.front() == '\'')) {
        if (t.size() >= 2 && t.front() == t.back()) {
            return Value::string(t.substr(1, t.size() - 2));
        }
    }
    return Value::string(t);
}

class Parser {
public:
    explicit Parser(std::vector<std::string> lines) : lines_(std::move(lines)) {}

    Value parse_document(YamlError& err) {
        skip_blank();
        if (pos_ >= lines_.size()) return Value::null();
        const auto indent = current_indent(err);
        if (!indent) return Value::null();
        auto node = parse_node(*indent, err);
        if (!node) return Value::null();
        skip_blank();
        if (pos_ < lines_.size()) {
            err = {pos_ + 1, "unexpected trailing content"};
            return Value::null();
        }
        return *node;
    }

private:
    std::vector<std::string> lines_;
    size_t pos_ = 0;

    void skip_blank() {
        while (pos_ < lines_.size() && is_blank_or_comment(lines_[pos_])) ++pos_;
    }

    std::optional<size_t> current_indent(YamlError& err) const {
        if (pos_ >= lines_.size()) return 0;
        auto ind = indent_of(lines_[pos_]);
        if (!ind) {
            err = {pos_ + 1, "tab character in indentation"};
        }
        return ind;
    }

    std::optional<Value> parse_node(size_t indent, YamlError& err) {
        if (pos_ >= lines_.size()) return std::nullopt;
        const std::string trimmed = trim(lines_[pos_]);
        if (trimmed.empty()) return std::nullopt;

        if (trimmed[0] == '-') {
            return parse_block_sequence(indent, err);
        }
        if (trimmed.find(':') != std::string::npos && trimmed[0] != '{' && trimmed[0] != '[') {
            return parse_block_mapping(indent, err);
        }
        if (trimmed[0] == '{') {
            return parse_flow_mapping(trimmed, err);
        }
        if (trimmed[0] == '[') {
            return parse_flow_sequence(trimmed, err);
        }
        err = {pos_ + 1, "expected mapping or sequence"};
        return std::nullopt;
    }

    std::optional<Value> parse_block_mapping(size_t indent, YamlError& err) {
        Mapping map;
        while (pos_ < lines_.size()) {
            skip_blank();
            if (pos_ >= lines_.size()) break;
            const auto line_indent = indent_of(lines_[pos_]);
            if (!line_indent) {
                err = {pos_ + 1, "tab character in indentation"};
                return std::nullopt;
            }
            if (*line_indent < indent) break;
            if (*line_indent > indent) {
                err = {pos_ + 1, "unexpected indentation"};
                return std::nullopt;
            }

            std::string line = trim_right(lines_[pos_]);
            ++pos_;
            const size_t colon = line.find(':');
            if (colon == std::string::npos) {
                err = {pos_, "expected key: value"};
                return std::nullopt;
            }
            std::string key = trim(line.substr(0, colon));
            std::string rest = trim(line.substr(colon + 1));

            if (rest.empty()) {
                skip_blank();
                if (pos_ >= lines_.size()) {
                    map.push(key, Value::null());
                    continue;
                }
                const auto next_indent = indent_of(lines_[pos_]);
                if (!next_indent) {
                    err = {pos_ + 1, "tab character in indentation"};
                    return std::nullopt;
                }
                if (*next_indent <= indent) {
                    map.push(key, Value::null());
                    continue;
                }
                auto child = parse_node(*next_indent, err);
                if (!child) return std::nullopt;
                map.push(std::move(key), std::move(*child));
            } else if (rest[0] == '{') {
                auto child = parse_flow_mapping(rest, err);
                if (!child) return std::nullopt;
                map.push(std::move(key), std::move(*child));
            } else if (rest[0] == '[') {
                auto child = parse_flow_sequence(rest, err);
                if (!child) return std::nullopt;
                map.push(std::move(key), std::move(*child));
            } else {
                map.push(std::move(key), parse_scalar(rest));
            }
        }
        return Value::mapping(std::move(map));
    }

    std::optional<Value> parse_block_sequence(size_t indent, YamlError& err) {
        std::vector<Value> items;
        while (pos_ < lines_.size()) {
            skip_blank();
            if (pos_ >= lines_.size()) break;
            const auto line_indent = indent_of(lines_[pos_]);
            if (!line_indent) {
                err = {pos_ + 1, "tab character in indentation"};
                return std::nullopt;
            }
            if (*line_indent < indent) break;
            if (*line_indent > indent) {
                err = {pos_ + 1, "unexpected indentation"};
                return std::nullopt;
            }

            std::string line = trim(lines_[pos_]);
            ++pos_;
            if (line.empty() || line[0] != '-') {
                err = {pos_, "expected sequence item"};
                return std::nullopt;
            }
            std::string rest = trim(line.substr(1));
            if (rest.empty()) {
                skip_blank();
                if (pos_ >= lines_.size()) {
                    items.push_back(Value::null());
                    continue;
                }
                const auto next_indent = indent_of(lines_[pos_]);
                if (!next_indent || *next_indent <= indent) {
                    items.push_back(Value::null());
                    continue;
                }
                auto child = parse_node(*next_indent, err);
                if (!child) return std::nullopt;
                items.push_back(std::move(*child));
            } else if (rest.find(':') != std::string::npos && rest[0] != '{' && rest[0] != '[') {
                --pos_;
                auto child = parse_block_mapping(indent + 2, err);
                if (!child) return std::nullopt;
                items.push_back(std::move(*child));
            } else if (rest[0] == '{') {
                auto child = parse_flow_mapping(rest, err);
                if (!child) return std::nullopt;
                items.push_back(std::move(*child));
            } else {
                items.push_back(parse_scalar(rest));
            }
        }
        return Value::sequence(std::move(items));
    }

    std::optional<Value> parse_flow_mapping(const std::string& text, YamlError& err) {
        std::string t = trim(text);
        if (t.empty() || t[0] != '{' || t.back() != '}') {
            err = {pos_, "invalid flow mapping"};
            return std::nullopt;
        }
        t = trim(t.substr(1, t.size() - 2));
        Mapping map;
        if (t.empty()) return Value::mapping(std::move(map));

        size_t i = 0;
        while (i < t.size()) {
            while (i < t.size() && (t[i] == ' ' || t[i] == ',')) ++i;
            if (i >= t.size()) break;

            size_t key_start = i;
            if (t[i] == '"' || t[i] == '\'') {
                const char q = t[i++];
                while (i < t.size() && t[i] != q) ++i;
                if (i >= t.size()) {
                    err = {pos_, "unterminated quoted key"};
                    return std::nullopt;
                }
                ++i;
            } else {
                while (i < t.size() && t[i] != ':' && t[i] != ',') ++i;
            }
            std::string key = trim(t.substr(key_start, i - key_start));
            while (i < t.size() && t[i] != ':') ++i;
            if (i >= t.size()) {
                err = {pos_, "expected ':' in flow mapping"};
                return std::nullopt;
            }
            ++i;
            while (i < t.size() && t[i] == ' ') ++i;

            size_t val_start = i;
            if (t[i] == '{' || t[i] == '[') {
                const char open = t[i];
                const char close = (open == '{') ? '}' : ']';
                int depth = 1;
                ++i;
                while (i < t.size() && depth > 0) {
                    if (t[i] == open) ++depth;
                    if (t[i] == close) --depth;
                    ++i;
                }
            } else if (t[i] == '"' || t[i] == '\'') {
                const char q = t[i++];
                while (i < t.size() && t[i] != q) ++i;
                if (i < t.size()) ++i;
            } else {
                while (i < t.size() && t[i] != ',') ++i;
            }
            std::string val_text = trim(t.substr(val_start, i - val_start));
            Value val = Value::null();
            if (!val_text.empty() && val_text[0] == '{') {
                auto child = parse_flow_mapping(val_text, err);
                if (!child) return std::nullopt;
                val = std::move(*child);
            } else if (!val_text.empty() && val_text[0] == '[') {
                auto child = parse_flow_sequence(val_text, err);
                if (!child) return std::nullopt;
                val = std::move(*child);
            } else {
                val = parse_scalar(val_text);
            }
            map.push(std::move(key), std::move(val));
        }
        return Value::mapping(std::move(map));
    }

    std::optional<Value> parse_flow_sequence(const std::string& text, YamlError& err) {
        std::string t = trim(text);
        if (t.empty() || t[0] != '[' || t.back() != ']') {
            err = {pos_, "invalid flow sequence"};
            return std::nullopt;
        }
        t = trim(t.substr(1, t.size() - 2));
        std::vector<Value> items;
        if (t.empty()) return Value::sequence(std::move(items));

        size_t i = 0;
        while (i < t.size()) {
            while (i < t.size() && (t[i] == ' ' || t[i] == ',')) ++i;
            if (i >= t.size()) break;
            size_t start = i;
            if (t[i] == '{' || t[i] == '[') {
                const char open = t[i];
                const char close = (open == '{') ? '}' : ']';
                int depth = 1;
                ++i;
                while (i < t.size() && depth > 0) {
                    if (t[i] == open) ++depth;
                    if (t[i] == close) --depth;
                    ++i;
                }
            } else if (t[i] == '"' || t[i] == '\'') {
                const char q = t[i++];
                while (i < t.size() && t[i] != q) ++i;
                if (i < t.size()) ++i;
            } else {
                while (i < t.size() && t[i] != ',') ++i;
            }
            std::string item_text = trim(t.substr(start, i - start));
            if (!item_text.empty() && item_text[0] == '{') {
                auto child = parse_flow_mapping(item_text, err);
                if (!child) return std::nullopt;
                items.push_back(std::move(*child));
            } else if (!item_text.empty() && item_text[0] == '[') {
                auto child = parse_flow_sequence(item_text, err);
                if (!child) return std::nullopt;
                items.push_back(std::move(*child));
            } else {
                items.push_back(parse_scalar(item_text));
            }
        }
        return Value::sequence(std::move(items));
    }
};

}  // namespace

const Value* Mapping::get(const std::string& key) const {
    for (const auto& [k, v] : entries_) {
        if (k == key) return &v;
    }
    return nullptr;
}

void Mapping::insert(std::string key, Value value) {
    for (auto& [k, v] : entries_) {
        if (k == key) {
            v = std::move(value);
            return;
        }
    }
    entries_.emplace_back(std::move(key), std::move(value));
}

void Mapping::push(std::string key, Value value) { insert(std::move(key), std::move(value)); }

Value Value::null() { return Value(Kind::Null); }
Value Value::boolean(bool v) {
    Value x(Kind::Bool);
    x.bool_ = v;
    return x;
}
Value Value::string(std::string s) {
    Value x(Kind::String);
    x.string_ = std::move(s);
    return x;
}
Value Value::mapping(Mapping m) {
    Value x(Kind::Mapping);
    x.mapping_ = std::move(m);
    return x;
}
Value Value::sequence(std::vector<Value> items) {
    Value x(Kind::Sequence);
    x.sequence_ = std::move(items);
    return x;
}

const std::string* Value::as_string() const {
    return kind_ == Kind::String ? &string_ : nullptr;
}
std::optional<bool> Value::as_bool() const {
    if (kind_ == Kind::Bool) return bool_;
    return std::nullopt;
}
const Mapping* Value::as_mapping() const {
    return kind_ == Kind::Mapping ? &mapping_ : nullptr;
}
const std::vector<Value>* Value::as_sequence() const {
    return kind_ == Kind::Sequence ? &sequence_ : nullptr;
}

std::optional<std::string> Value::scalar_string() const {
    if (const auto* s = as_string()) return *s;
    if (kind_ == Kind::Bool) return bool_ ? "true" : "false";
    return std::nullopt;
}

Value Value::parse(const std::string& text, YamlError* err) {
    std::vector<std::string> lines;
    std::istringstream in(text);
    std::string line;
    while (std::getline(in, line)) {
        if (!line.empty() && line.back() == '\r') line.pop_back();
        lines.push_back(line);
    }
    YamlError local;
    Parser p(std::move(lines));
    Value result = p.parse_document(local);
    if (err) *err = local;
    if (!local.message.empty()) return Value::null();
    return result;
}

std::string Value::to_yaml(int indent) const {
    const std::string pad(indent, ' ');
    switch (kind_) {
        case Kind::Null:
            return "null";
        case Kind::Bool:
            return bool_ ? "true" : "false";
        case Kind::String: {
            bool needs_quote = string_.empty() || string_.find(':') != std::string::npos ||
                               string_.find('#') != std::string::npos || string_.find('\n') != std::string::npos;
            if (!needs_quote) return string_;
            return '"' + string_ + '"';
        }
        case Kind::Sequence: {
            if (sequence_.empty()) return "[]";
            std::ostringstream out;
            for (const auto& item : sequence_) {
                out << pad << "- " << item.to_yaml(indent + 2) << '\n';
            }
            return out.str();
        }
        case Kind::Mapping: {
            if (mapping_.empty()) return "{}";
            std::ostringstream out;
            for (const auto& [k, v] : mapping_.entries()) {
                const std::string val = v.to_yaml(indent + 2);
                if (val.find('\n') != std::string::npos) {
                    out << pad << k << ":\n" << val;
                    if (!val.empty() && val.back() != '\n') out << '\n';
                } else {
                    out << pad << k << ": " << val << '\n';
                }
            }
            return out.str();
        }
        default:
            return string_;
    }
}

}  // namespace mar::okf
