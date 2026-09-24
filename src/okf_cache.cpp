#include "mar/okf/cache.hpp"

#include <chrono>
#include <filesystem>
#include <iomanip>
#include <nlohmann/json.hpp>
#include <sstream>

namespace mar::okf {

bool source_has_okf_cache(const std::string& root_path) {
    return std::filesystem::exists(std::filesystem::path(root_path) / ".okf" / "manifest.json");
}

CacheFiles build_cache(const Bundle& bundle, const Report& report, const std::string& actor) {
    CacheFiles files;

    nlohmann::json manifest;
    manifest["okf_version"] = bundle.okf_version().value_or(OKF_VERSION);
    manifest["spec_version"] = OKF_VERSION;
    manifest["concept_count"] = bundle.concepts().size();
    manifest["generator"] = actor.empty() ? "mar/okf" : actor;
    const auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::ostringstream ts;
    ts << std::put_time(std::gmtime(&now), "%Y-%m-%dT%H:%M:%SZ");
    manifest["packed_at"] = ts.str();
    files.manifest_json = manifest.dump(2) + "\n";

    std::ostringstream concepts;
    for (const auto& rec : bundle.concepts()) {
        nlohmann::json row;
        row["id"] = rec.id;
        row["path"] = rec.path;
        row["type"] = rec.document.frontmatter.type().value_or("");
        row["title"] = rec.document.frontmatter.title().value_or(rec.id);
        row["description"] = rec.document.frontmatter.description().value_or("");
        row["status"] = rec.status;
        row["trust_tier"] = trust_tier_name(rec.trust_tier);
        row["tags"] = rec.document.frontmatter.tags();
        if (auto stale = rec.document.frontmatter.stale_after()) {
            row["stale_after"] = *stale;
        }
        concepts << row.dump() << '\n';
    }
    files.concepts_jsonl = concepts.str();

    nlohmann::json graph;
    graph["concepts"] = nlohmann::json::array();
    for (const auto& rec : bundle.concepts()) {
        nlohmann::json node;
        node["id"] = rec.id;
        node["links"] = nlohmann::json::array();
        for (const auto& link : rec.outbound_links) {
            nlohmann::json edge;
            edge["target"] = link.target;
            edge["exists"] = link.exists;
            edge["text"] = link.text;
            edge["raw"] = link.raw;
            node["links"].push_back(std::move(edge));
        }
        node["backlinks"] = rec.backlinks;
        graph["concepts"].push_back(std::move(node));
    }
    files.graph_json = graph.dump(2) + "\n";

    nlohmann::json tags = nlohmann::json::object();
    for (const auto& [tag, ids] : bundle.tags()) {
        tags[tag] = ids;
    }
    files.tags_json = tags.dump(2) + "\n";

    nlohmann::json report_json;
    report_json["diagnostics"] = nlohmann::json::array();
    for (const auto& d : report.diagnostics()) {
        if (d.severity == Severity::Info && d.message.rfind("stale since", 0) == 0) continue;
        nlohmann::json item;
        item["severity"] = d.severity == Severity::Error   ? "error"
                           : d.severity == Severity::Warning ? "warning"
                                                             : "info";
        if (!d.path.empty()) item["path"] = d.path;
        if (!d.concept_id.empty()) item["concept_id"] = d.concept_id;
        item["message"] = d.message;
        report_json["diagnostics"].push_back(std::move(item));
    }
    files.report_json = report_json.dump(2) + "\n";

    return files;
}

}  // namespace mar::okf
