#pragma once

#include "mar/okf/bundle.hpp"

#include <optional>
#include <string>
#include <vector>

namespace mar::okf {

enum class Severity { Error, Warning, Info };

struct Diagnostic {
    Severity severity = Severity::Info;
    std::string path;
    std::string concept_id;
    std::string message;
};

class Report {
public:
    [[nodiscard]] bool is_conformant() const;
    [[nodiscard]] size_t error_count() const;
    [[nodiscard]] size_t warning_count() const;
    [[nodiscard]] const std::vector<Diagnostic>& diagnostics() const { return diagnostics_; }
    void add(Diagnostic d) { diagnostics_.push_back(std::move(d)); }

private:
    std::vector<Diagnostic> diagnostics_;
};

Report validate_bundle(const Bundle& bundle, const std::optional<std::string>& today = std::nullopt);

}  // namespace mar::okf
