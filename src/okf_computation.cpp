#include "mar/okf/computation.hpp"

#include <sstream>

namespace mar::okf {
namespace {

std::optional<std::pair<size_t, std::string>> parse_heading(const std::string& line) {
    const size_t start = line.find_first_not_of(" \t");
    if (start == std::string::npos || line[start] != '#') return std::nullopt;
    size_t hashes = 0;
    while (start + hashes < line.size() && line[start + hashes] == '#') ++hashes;
    if (hashes == 0 || hashes > 6) return std::nullopt;
    std::string title = line.substr(start + hashes);
    while (!title.empty() && (title[0] == ' ' || title[0] == '\t')) title.erase(0, 1);
    while (!title.empty() && title.back() == '#') title.pop_back();
    while (!title.empty() && (title.back() == ' ' || title.back() == '\t')) title.pop_back();
    return std::make_pair(hashes, title);
}

std::vector<std::string> computation_section_lines(const std::string& body) {
    std::istringstream in(body);
    std::string line;
    size_t level = 0;
    bool in_section = false;
    std::vector<std::string> section;
    while (std::getline(in, line)) {
        if (!in_section) {
            if (auto heading = parse_heading(line)) {
                if (heading->second == "Computation") {
                    level = heading->first;
                    in_section = true;
                }
            }
            continue;
        }
        if (auto heading = parse_heading(line)) {
            if (heading->first <= level) break;
        }
        section.push_back(line);
    }
    return section;
}

std::string dedent_line(const std::string& line, size_t n) {
    if (!line.empty() && line[0] == '\t') return line.substr(1);
    size_t strip = 0;
    while (strip < line.size() && line[strip] == ' ') ++strip;
    return line.substr(std::min(strip, n));
}

std::optional<InlineComputation> indented_block(const std::vector<std::string>& section) {
    std::vector<std::string> code;
    bool started = false;
    for (const auto& line : section) {
        const bool is_code = line.rfind("    ", 0) == 0 || (!line.empty() && line[0] == '\t');
        if (is_code) {
            started = true;
            code.push_back(dedent_line(line, 4));
        } else if (line.find_first_not_of(" \t") == std::string::npos) {
            if (started) code.emplace_back();
        } else if (started) {
            break;
        }
    }
    while (!code.empty() && code.front().find_first_not_of(" \t") == std::string::npos) code.erase(code.begin());
    while (!code.empty() && code.back().find_first_not_of(" \t") == std::string::npos) code.pop_back();
    if (code.empty()) return std::nullopt;
    std::ostringstream out;
    for (size_t i = 0; i < code.size(); ++i) {
        if (i) out << '\n';
        out << code[i];
    }
    InlineComputation result;
    result.code = out.str();
    result.fenced = false;
    return result;
}

std::optional<InlineComputation> fenced_block(const std::vector<std::string>& section) {
    for (size_t i = 0; i < section.size(); ++i) {
        const std::string trimmed = section[i];
        size_t start = trimmed.find_first_not_of(" \t");
        if (start == std::string::npos) continue;
        std::string t = trimmed.substr(start);
        std::string marker;
        if (t.rfind("```", 0) == 0) marker = "```";
        else if (t.rfind("~~~", 0) == 0) marker = "~~~";
        else continue;
        std::string info = t.substr(3);
        while (!info.empty() && (info[0] == ' ' || info[0] == '\t')) info.erase(0, 1);
        InlineComputation result;
        result.fenced = true;
        if (!info.empty()) result.language = info;
        std::vector<std::string> code;
        for (size_t j = i + 1; j < section.size(); ++j) {
            const std::string line = section[j];
            size_t ls = line.find_first_not_of(" \t");
            if (ls != std::string::npos && line.substr(ls).rfind(marker, 0) == 0) break;
            code.push_back(line);
        }
        while (!code.empty() && code.front().find_first_not_of(" \t") == std::string::npos) code.erase(code.begin());
        while (!code.empty() && code.back().find_first_not_of(" \t") == std::string::npos) code.pop_back();
        if (code.empty()) return std::nullopt;
        std::ostringstream out;
        for (size_t k = 0; k < code.size(); ++k) {
            if (k) out << '\n';
            out << code[k];
        }
        result.code = out.str();
        return result;
    }
    return std::nullopt;
}

Parameter parse_parameter(const Value& value) {
    Parameter p;
    if (const auto* map = value.as_mapping()) {
        if (const auto* name = map->get("name")) p.name = name->scalar_string();
        if (const auto* type = map->get("type")) p.type = type->scalar_string();
        if (const auto* req = map->get("required")) {
            if (auto b = req->as_bool()) p.required = *b;
        }
    }
    return p;
}

std::vector<Parameter> parse_parameters(const Value* value) {
    std::vector<Parameter> out;
    if (!value) return out;
    if (const auto* seq = value->as_sequence()) {
        for (const auto& item : *seq) out.push_back(parse_parameter(item));
    } else if (value->as_mapping()) {
        out.push_back(parse_parameter(*value));
    }
    return out;
}

Executor parse_executor(const Value* value) {
    Executor ex;
    if (!value) return ex;
    const auto* map = value->as_mapping();
    if (!map) return ex;
    if (const auto* resource = map->get("resource")) ex.resource = resource->scalar_string();
    if (const auto* receipt = map->get("receipt")) {
        if (const auto* seq = receipt->as_sequence()) {
            for (const auto& item : *seq) {
                if (auto s = item.scalar_string()) ex.receipt.push_back(*s);
            }
        } else if (auto s = receipt->scalar_string()) {
            ex.receipt.push_back(*s);
        }
    }
    return ex;
}

Attester parse_attester(const Value* value) {
    Attester at;
    if (!value) return at;
    if (const auto* map = value->as_mapping()) {
        if (const auto* resource = map->get("resource")) at.resource = resource->scalar_string();
    }
    return at;
}

}  // namespace

bool is_attested_computation_type(const Frontmatter& fm) {
    return fm.type() == ATTESTED_COMPUTATION_TYPE;
}

std::optional<InlineComputation> extract_inline_computation(const std::string& body) {
    const auto section = computation_section_lines(body);
    if (section.empty()) return std::nullopt;
    if (auto fenced = fenced_block(section)) return fenced;
    return indented_block(section);
}

AttestedComputation parse_attested_computation(const Frontmatter& fm, const std::string& body) {
    AttestedComputation contract;
    if (const auto* runtime = fm.mapping().get("runtime")) contract.runtime = runtime->scalar_string();
    contract.parameters = parse_parameters(fm.mapping().get("parameters"));
    if (const auto* executor = fm.mapping().get("executor")) contract.executor = parse_executor(executor);
    if (const auto* attester = fm.mapping().get("attester")) contract.attester = parse_attester(attester);

    std::optional<std::string> path;
    if (const auto* computation = fm.mapping().get("computation")) {
        path = computation->scalar_string();
        if (path && path->empty()) path.reset();
    }
    auto inline_code = extract_inline_computation(body);
    if (path && inline_code) {
        contract.computation.kind = ComputationSourceKind::File;
        contract.computation.file_path = *path;
        contract.has_redundant_inline = true;
    } else if (path) {
        contract.computation.kind = ComputationSourceKind::File;
        contract.computation.file_path = *path;
    } else if (inline_code) {
        contract.computation.kind = ComputationSourceKind::Inline;
        contract.computation.inline_code = *inline_code;
    }
    return contract;
}

}  // namespace mar::okf
