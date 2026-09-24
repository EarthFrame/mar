#include "mar/okf/lint.hpp"

#include "mar/okf/util.hpp"

#include <filesystem>
#include <fstream>
#include <map>
#include <set>

namespace mar::okf {
namespace {

void add_lint(Report& report, Severity severity, const std::string& code, const ConceptRecord& rec,
              const std::string& message, const std::set<std::string>& ignore) {
    if (ignore.count(code)) return;
    Diagnostic d;
    d.severity = severity;
    d.path = rec.path;
    d.concept_id = rec.id;
    d.message = "[" + code + "] " + message;
    report.add(std::move(d));
}

bool is_concept_link_target(const std::string& raw) {
    const std::string t = raw;
    if (t.empty() || t[0] == '#') return false;
    if (str_starts_with(t, "//") || t.find("://") != std::string::npos) return false;
    const auto hash = t.find('#');
    const std::string before = hash == std::string::npos ? t : t.substr(0, hash);
    const auto slash = before.find_last_of('/');
    const std::string base = slash == std::string::npos ? before : before.substr(slash + 1);
    return str_ends_with(base, ".md") || base.find('.') == std::string::npos;
}

std::string parent_dir(const std::string& id) {
    const auto pos = id.find_last_of('/');
    if (pos == std::string::npos) return "";
    return id.substr(0, pos);
}

std::set<std::string> indexed_concepts(const Bundle& bundle) {
    std::set<std::string> out;
    const auto ids = bundle.concept_ids();
    for (const auto& index_path : bundle.index_files()) {
        std::string text;
        if (const auto it = bundle.index_contents().find(index_path); it != bundle.index_contents().end()) {
            text = it->second;
        } else if (bundle.root_path()) {
            std::ifstream in(std::filesystem::path(*bundle.root_path()) / index_path);
            if (!in) continue;
            text.assign((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
        } else {
            continue;
        }
        DocumentError err;
        auto doc = Document::parse(text, &err);
        if (!doc) continue;
        const std::string source_id = parent_dir(concept_id_from_path(index_path));
        for (const auto& link : extract_links(doc->body)) {
            if (!is_concept_link_target(link.target)) continue;
            if (auto resolved = resolve_link_target(link.target, source_id, ids)) out.insert(*resolved);
        }
    }
    return out;
}

}  // namespace

Report lint_bundle(const Bundle& bundle, const std::optional<std::string>& today,
                   const std::set<std::string>& ignore) {
    Report report;
    std::map<std::string, size_t> title_counts;
    for (const auto& rec : bundle.concepts()) {
        if (auto title = rec.document.frontmatter.title()) {
            title_counts[*title]++;
        }
    }
    const auto indexed = indexed_concepts(bundle);

    for (const auto& rec : bundle.concepts()) {
        const auto& fm = rec.document.frontmatter;
        if (!fm.title()) {
            add_lint(report, Severity::Warning, "L1", rec, "missing `title`", ignore);
        }
        if (!fm.description()) {
            add_lint(report, Severity::Warning, "L2", rec, "missing `description`", ignore);
        }
        if (!fm.has_key("generated") && !fm.timestamp()) {
            add_lint(report, Severity::Warning, "L3", rec, "missing `generated`", ignore);
        }
        if (!fm.has_key("verified")) {
            add_lint(report, Severity::Info, "L4", rec, "no `verified` events; trust tier is `unverified`", ignore);
        }
        if (fm.timestamp()) {
            add_lint(report, Severity::Warning, "L5", rec, "`timestamp` is a v0.1 key superseded by `generated`", ignore);
        }
        if (rec.document.has_legacy_citations()) {
            add_lint(report, Severity::Warning, "L6", rec, "legacy body `# Citations` list present", ignore);
        }
        if (rec.document.body.find_first_not_of(" \t\n\r") == std::string::npos) {
            add_lint(report, Severity::Warning, "L7", rec, "body is empty", ignore);
        } else if (!rec.document.has_top_level_heading()) {
            add_lint(report, Severity::Warning, "L8", rec, "body has no top-level `#` heading", ignore);
        }
        if (auto gen = fm.generated_at()) {
            if (auto ver = fm.latest_verified_at()) {
                if (*ver < *gen) {
                    add_lint(report, Severity::Warning, "L9", rec,
                             "latest verification predates `generated.at`", ignore);
                }
            }
        }
        for (const auto& link : rec.outbound_links) {
            if (!link.exists) continue;
            if (const auto* target = bundle.find(link.target)) {
                if (target->status == "deprecated") {
                    add_lint(report, Severity::Warning, "L10", rec,
                             "links to deprecated concept `" + link.target + "`", ignore);
                    break;
                }
            }
        }
        if (today && fm.stale_after() && *fm.stale_after() <= *today) {
            add_lint(report, Severity::Warning, "L11", rec, "stale since " + *fm.stale_after(), ignore);
        }
        if (fm.status() == "draft") {
            add_lint(report, Severity::Info, "L12", rec, "`status: draft`", ignore);
        }
        for (const auto& link : rec.outbound_links) {
            if (link.exists && link.target == rec.id) {
                add_lint(report, Severity::Info, "L13", rec, "self-link", ignore);
                break;
            }
        }
        if (auto title = fm.title()) {
            if (title_counts[*title] > 1) {
                add_lint(report, Severity::Warning, "L14", rec, "duplicate `title`", ignore);
            }
        }
        if (rec.backlinks.empty() && !indexed.count(rec.id)) {
            add_lint(report, Severity::Warning, "L15", rec,
                     "orphan concept: no backlinks and not listed in any `index.md`", ignore);
        }
    }

    if (bundle.root_path()) {
        for (const auto& index_path : bundle.index_files()) {
            const std::string dir = parent_dir(index_path);
            std::set<std::string> actual;
            for (const auto& rec : bundle.concepts()) {
                if (parent_dir(rec.id) == dir) actual.insert(rec.id);
            }
            std::set<std::string> listed;
            std::string source_id = concept_id_from_path(index_path);
            std::ifstream in(std::filesystem::path(*bundle.root_path()) / index_path);
            if (!in) continue;
            std::string text((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
            DocumentError err;
            auto doc = Document::parse(text, &err);
            if (!doc) continue;
            for (const auto& link : extract_links(doc->body)) {
                if (!is_concept_link_target(link.target)) continue;
                if (auto resolved = resolve_link_target(link.target, source_id, bundle.concept_ids())) {
                    if (parent_dir(*resolved) == dir) listed.insert(*resolved);
                }
            }
            std::vector<std::string> missing;
            std::vector<std::string> extra;
            for (const auto& id : actual) {
                if (!listed.count(id)) missing.push_back(id);
            }
            for (const auto& id : listed) {
                if (!actual.count(id)) extra.push_back(id);
            }
            if (!missing.empty() || !extra.empty()) {
                if (ignore.count("L16")) continue;
                Diagnostic d;
                d.severity = Severity::Warning;
                d.path = index_path;
                d.message = "[L16] index.md is out of sync with its directory";
                report.add(std::move(d));
            }
        }
    }

    return report;
}

}  // namespace mar::okf
