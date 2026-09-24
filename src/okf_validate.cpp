#include "mar/okf/validate.hpp"

namespace mar::okf {

bool Report::is_conformant() const {
    for (const auto& d : diagnostics_) {
        if (d.severity == Severity::Error) return false;
    }
    return true;
}

size_t Report::error_count() const {
    size_t n = 0;
    for (const auto& d : diagnostics_) {
        if (d.severity == Severity::Error) ++n;
    }
    return n;
}

size_t Report::warning_count() const {
    size_t n = 0;
    for (const auto& d : diagnostics_) {
        if (d.severity == Severity::Warning) ++n;
    }
    return n;
}

Report validate_bundle(const Bundle& bundle, const std::optional<std::string>& today) {
    Report report;

    for (const auto& [path, err] : bundle.parse_errors()) {
        Diagnostic d;
        d.severity = Severity::Error;
        d.path = path;
        d.message = "unparseable concept document: " + err.message;
        report.add(std::move(d));
    }

    for (const auto& rec : bundle.concepts()) {
        if (!rec.document.has_nonempty_type()) {
            Diagnostic d;
            d.severity = Severity::Error;
            d.path = rec.path;
            d.concept_id = rec.id;
            d.message = "missing required frontmatter field `type`";
            report.add(std::move(d));
        }

        if (!rec.document.frontmatter.title()) {
            Diagnostic d;
            d.severity = Severity::Warning;
            d.path = rec.path;
            d.concept_id = rec.id;
            d.message = "missing recommended field `title`";
            report.add(std::move(d));
        }
        if (!rec.document.frontmatter.description()) {
            Diagnostic d;
            d.severity = Severity::Warning;
            d.path = rec.path;
            d.concept_id = rec.id;
            d.message = "missing recommended field `description`";
            report.add(std::move(d));
        }

        for (const auto& link : rec.outbound_links) {
            if (!link.exists) {
                Diagnostic d;
                d.severity = Severity::Info;
                d.path = rec.path;
                d.concept_id = rec.id;
                d.message = "link target does not resolve to a concept in the bundle";
                report.add(std::move(d));
            }
        }

        if (today && rec.document.frontmatter.stale_after()) {
            if (*rec.document.frontmatter.stale_after() <= *today) {
                Diagnostic d;
                d.severity = Severity::Info;
                d.path = rec.path;
                d.concept_id = rec.id;
                d.message = "stale since " + *rec.document.frontmatter.stale_after();
                report.add(std::move(d));
            }
        }
    }

    return report;
}

}  // namespace mar::okf
