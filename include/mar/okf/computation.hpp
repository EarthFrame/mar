#pragma once

#include "mar/okf/document.hpp"

#include <optional>
#include <string>
#include <vector>

namespace mar::okf {

constexpr const char* ATTESTED_COMPUTATION_TYPE = "Attested Computation";

struct Parameter {
    std::optional<std::string> name;
    std::optional<std::string> type;
    std::optional<bool> required;
    [[nodiscard]] bool is_required() const { return required.value_or(false); }
};

struct Executor {
    std::optional<std::string> resource;
    std::vector<std::string> receipt;
};

struct Attester {
    std::optional<std::string> resource;
};

struct InlineComputation {
    std::string code;
    std::optional<std::string> language;
    bool fenced = false;
};

enum class ComputationSourceKind { Inline, File, Missing };

struct ComputationSource {
    ComputationSourceKind kind = ComputationSourceKind::Missing;
    InlineComputation inline_code;
    std::string file_path;
    [[nodiscard]] bool is_missing() const { return kind == ComputationSourceKind::Missing; }
};

struct AttestedComputation {
    std::optional<std::string> runtime;
    std::vector<Parameter> parameters;
    ComputationSource computation;
    std::optional<Executor> executor;
    std::optional<Attester> attester;
    bool has_redundant_inline = false;
};

[[nodiscard]] bool is_attested_computation_type(const Frontmatter& fm);
[[nodiscard]] AttestedComputation parse_attested_computation(const Frontmatter& fm, const std::string& body);
[[nodiscard]] std::optional<InlineComputation> extract_inline_computation(const std::string& body);

}  // namespace mar::okf
