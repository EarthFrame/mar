#include "mar/okf/cli.hpp"

#include "mar/okf/bundle.hpp"
#include "mar/okf/cache.hpp"
#include "mar/okf/computation.hpp"
#include "mar/okf/diff.hpp"
#include "mar/okf/document.hpp"
#include "mar/okf/index_gen.hpp"
#include "mar/okf/lint.hpp"
#include "mar/okf/util.hpp"
#include "mar/okf/validate.hpp"
#include "mar/reader.hpp"
#include "mar/writer.hpp"

#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <sstream>

namespace mar::okf {
namespace {

constexpr int EXIT_OK = 0;
constexpr int EXIT_USAGE = 2;
constexpr int EXIT_ERROR = 3;
constexpr int EXIT_INTEGRITY = 65;

void okf_error(const std::string& msg) { std::cerr << "mar: error: " << msg << '\n'; }

struct OkfOptions {
    bool no_cache = false;
    bool include_cache = false;
    bool strict = false;
    bool force = false;
    std::string today;
    std::string output;
    std::string format = "text";
    std::string type_filter;
    std::string concept_id;
    bool frontmatter_only = false;
    std::string actor;
    bool write_file = false;
    std::string ignore_rules;
};

bool parse_common_flags(int& i, int argc, char* argv[], OkfOptions& opts) {
    const std::string arg = argv[i];
    if (arg == "--no-cache") {
        opts.no_cache = true;
        return true;
    }
    if (arg == "--today" && i + 1 < argc) {
        opts.today = argv[++i];
        return true;
    }
    if ((arg == "-f" || arg == "-o" || arg == "--output") && i + 1 < argc) {
        opts.output = argv[++i];
        return true;
    }
    if (arg == "--format" && i + 1 < argc) {
        opts.format = argv[++i];
        return true;
    }
    if (arg == "--type" && i + 1 < argc) {
        opts.type_filter = argv[++i];
        return true;
    }
    if (arg == "--actor" && i + 1 < argc) {
        opts.actor = argv[++i];
        return true;
    }
    if (arg == "--strict") {
        opts.strict = true;
        return true;
    }
    if (arg == "--force") {
        opts.force = true;
        return true;
    }
    if (arg == "--include-cache") {
        opts.include_cache = true;
        return true;
    }
    if (arg == "--frontmatter") {
        opts.frontmatter_only = true;
        return true;
    }
    if (arg == "-w" || arg == "--write") {
        opts.write_file = true;
        return true;
    }
    if (arg == "--ignore" && i + 1 < argc) {
        opts.ignore_rules = argv[++i];
        return true;
    }
    return false;
}

std::vector<std::string> collect_positionals(int argc, char* argv[]) {
    std::vector<std::string> out;
    for (int i = 0; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "-h" || arg == "--help") continue;
        if (str_starts_with(arg, "-")) {
            if (arg == "--today" || arg == "-f" || arg == "-o" || arg == "--output" || arg == "--format" || arg == "--type" ||
                arg == "--actor" || arg == "--ignore") {
                ++i;
            }
            continue;
        }
        out.push_back(arg);
    }
    return out;
}

Bundle load_input(const std::string& path, const OkfOptions& opts) {
    auto source = open_bundle_source(path);
    BundleLoadOptions load_opts;
    load_opts.use_cache = !opts.no_cache;
    return Bundle::load(*source, load_opts);
}

int cmd_pack(int argc, char* argv[]) {
    OkfOptions opts;
    for (int i = 0; i < argc; ++i) {
        if (!parse_common_flags(i, argc, argv, opts) && (argv[i] == std::string("-h") || argv[i] == std::string("--help"))) {
            print_okf_usage();
            return EXIT_OK;
        }
    }
    const auto args = collect_positionals(argc, argv);
    if (args.empty() || opts.output.empty()) {
        okf_error("usage: mar okf pack <bundle-dir|archive> -f <output.mar>");
        return EXIT_USAGE;
    }

    const std::string& input = args[0];
    if (std::filesystem::is_directory(input) && source_has_okf_cache(input) && !opts.force) {
        okf_error("source contains .okf/; use --force to rebuild");
        return EXIT_ERROR;
    }

    auto source = open_bundle_source(input);
    BundleLoadOptions load_opts;
    load_opts.use_cache = false;
    Bundle bundle = Bundle::load(*source, load_opts);
    Report report = validate_bundle(bundle, opts.today.empty() ? std::nullopt : std::optional<std::string>(opts.today));
    if (opts.strict && !report.is_conformant()) {
        okf_error("bundle is not conformant (--strict)");
        return EXIT_INTEGRITY;
    }

    if (const auto parent = std::filesystem::path(opts.output).parent_path(); !parent.empty()) {
        std::filesystem::create_directories(parent);
    }

    MarWriter writer(opts.output);
    for (const auto& path : source->list_paths()) {
        if (str_starts_with(path, CACHE_PREFIX)) continue;
        if (auto data = source->read(path)) writer.add_memory(path, *data);
    }

    if (!opts.no_cache) {
        CacheFiles cache = build_cache(bundle, report, opts.actor);
        writer.add_memory(std::string(CACHE_PREFIX) + "manifest.json",
                          std::vector<u8>(cache.manifest_json.begin(), cache.manifest_json.end()));
        writer.add_memory(std::string(CACHE_PREFIX) + "concepts.jsonl",
                          std::vector<u8>(cache.concepts_jsonl.begin(), cache.concepts_jsonl.end()));
        writer.add_memory(std::string(CACHE_PREFIX) + "graph.json",
                          std::vector<u8>(cache.graph_json.begin(), cache.graph_json.end()));
        writer.add_memory(std::string(CACHE_PREFIX) + "tags.json",
                          std::vector<u8>(cache.tags_json.begin(), cache.tags_json.end()));
        writer.add_memory(std::string(CACHE_PREFIX) + "report.json",
                          std::vector<u8>(cache.report_json.begin(), cache.report_json.end()));
    }
    writer.finish();
    std::cout << "packed " << opts.output << " (" << bundle.concepts().size() << " concepts)\n";
    return EXIT_OK;
}

int cmd_unpack(int argc, char* argv[]) {
    OkfOptions opts;
    for (int i = 0; i < argc; ++i) parse_common_flags(i, argc, argv, opts);
    const auto args = collect_positionals(argc, argv);
    if (args.empty() || opts.output.empty()) {
        okf_error("usage: mar okf unpack <archive> -o <dir>");
        return EXIT_USAGE;
    }
    MarReader reader(args[0]);
    std::filesystem::create_directories(opts.output);
    for (size_t i = 0; i < reader.file_count(); ++i) {
        const auto name = reader.get_name(i);
        if (!name) continue;
        if (!opts.include_cache && str_starts_with(*name, CACHE_PREFIX)) continue;
        const auto data = reader.read_file(i);
        const auto out_path = std::filesystem::path(opts.output) / *name;
        std::filesystem::create_directories(out_path.parent_path());
        std::ofstream out(out_path, std::ios::binary);
        out.write(reinterpret_cast<const char*>(data.data()), static_cast<std::streamsize>(data.size()));
    }
    std::cout << "unpacked to " << opts.output << '\n';
    return EXIT_OK;
}

int cmd_info(int argc, char* argv[]) {
    OkfOptions opts;
    for (int i = 0; i < argc; ++i) parse_common_flags(i, argc, argv, opts);
    const auto args = collect_positionals(argc, argv);
    if (args.empty()) {
        okf_error("usage: mar okf info <bundle-dir|archive>");
        return EXIT_USAGE;
    }
    const Bundle bundle = load_input(args[0], opts);
    std::map<std::string, size_t> types;
    std::map<std::string, size_t> tiers;
    for (const auto& rec : bundle.concepts()) {
        types[rec.document.frontmatter.type().value_or("(none)")]++;
        tiers[trust_tier_name(rec.trust_tier)]++;
    }
    std::cout << "concepts: " << bundle.concepts().size() << '\n';
    std::cout << "okf_version: " << bundle.okf_version().value_or("(undeclared)") << '\n';
    std::cout << "cache: " << (bundle.has_cache() ? "yes" : "no") << '\n';
    std::cout << "types:\n";
    for (const auto& [type, count] : types) std::cout << "  " << type << ": " << count << '\n';
    std::cout << "trust:\n";
    for (const auto& [tier, count] : tiers) std::cout << "  " << tier << ": " << count << '\n';
    return EXIT_OK;
}

int cmd_ls(int argc, char* argv[]) {
    OkfOptions opts;
    for (int i = 0; i < argc; ++i) parse_common_flags(i, argc, argv, opts);
    const auto args = collect_positionals(argc, argv);
    if (args.empty()) {
        okf_error("usage: mar okf ls <bundle-dir|archive>");
        return EXIT_USAGE;
    }
    const Bundle bundle = load_input(args[0], opts);
    bool first_json = true;
    if (opts.format == "json") std::cout << "[\n";
    for (const auto& rec : bundle.concepts()) {
        if (!opts.type_filter.empty() && rec.document.frontmatter.type() != opts.type_filter) continue;
        if (opts.format == "json") {
            if (!first_json) std::cout << ",\n";
            first_json = false;
            std::cout << "  {\"id\": \"" << rec.id << "\", \"type\": \""
                      << rec.document.frontmatter.type().value_or("") << "\", \"title\": \""
                      << rec.document.frontmatter.title().value_or(rec.id) << "\"}";
        } else {
            std::cout << rec.id << '\t' << rec.document.frontmatter.type().value_or("") << '\t'
                      << rec.document.frontmatter.title().value_or(rec.id) << '\n';
        }
    }
    if (opts.format == "json") std::cout << "\n]\n";
    return EXIT_OK;
}

int cmd_cat(int argc, char* argv[]) {
    OkfOptions opts;
    for (int i = 0; i < argc; ++i) parse_common_flags(i, argc, argv, opts);
    const auto args = collect_positionals(argc, argv);
    if (args.size() < 2) {
        okf_error("usage: mar okf cat <bundle-dir|archive> <concept-id>");
        return EXIT_USAGE;
    }
    const Bundle bundle = load_input(args[0], opts);
    std::string id = args[1];
    if (str_ends_with(id, ".md")) id = concept_id_from_path(id);
    const ConceptRecord* rec = bundle.find(id);
    if (!rec) {
        okf_error("concept not found: " + id);
        return EXIT_ERROR;
    }
    if (opts.frontmatter_only) {
        std::cout << Value::mapping(rec->document.frontmatter.mapping()).to_yaml();
    } else {
        std::cout << rec->document.serialize();
    }
    return EXIT_OK;
}

int cmd_validate(int argc, char* argv[]) {
    OkfOptions opts;
    for (int i = 0; i < argc; ++i) parse_common_flags(i, argc, argv, opts);
    const auto args = collect_positionals(argc, argv);
    if (args.empty()) {
        okf_error("usage: mar okf validate <bundle-dir|archive>");
        return EXIT_USAGE;
    }
    BundleLoadOptions load_opts;
    load_opts.use_cache = !opts.no_cache;
    auto source = open_bundle_source(args[0]);
    const Bundle bundle = Bundle::load(*source, load_opts);
    const Report report =
        validate_bundle(bundle, opts.today.empty() ? std::nullopt : std::optional<std::string>(opts.today));
    for (const auto& d : report.diagnostics()) {
        std::cout << '[';
        switch (d.severity) {
            case Severity::Error:
                std::cout << "error";
                break;
            case Severity::Warning:
                std::cout << "warning";
                break;
            case Severity::Info:
                std::cout << "info";
                break;
        }
        std::cout << "] ";
        if (!d.path.empty()) std::cout << d.path << ": ";
        std::cout << d.message << '\n';
    }
    std::cout << bundle.concepts().size() << " concept(s); " << report.error_count() << " error(s), "
              << report.warning_count() << " warning(s).\n";
    if (report.is_conformant()) {
        std::cout << "conformant with OKF v" << OKF_VERSION << '\n';
        return EXIT_OK;
    }
    std::cout << "not conformant with OKF v" << OKF_VERSION << '\n';
    return EXIT_INTEGRITY;
}

int cmd_graph(int argc, char* argv[]) {
    OkfOptions opts;
    for (int i = 0; i < argc; ++i) parse_common_flags(i, argc, argv, opts);
    const auto args = collect_positionals(argc, argv);
    if (args.empty()) {
        okf_error("usage: mar okf graph <bundle-dir|archive>");
        return EXIT_USAGE;
    }
    const Bundle bundle = load_input(args[0], opts);
    if (opts.format == "json") {
        std::cout << "{\n  \"concepts\": [\n";
        bool first = true;
        for (const auto& rec : bundle.concepts()) {
            if (!first) std::cout << ",\n";
            first = false;
            std::cout << "    {\"id\": \"" << rec.id << "\", \"links\": [";
            bool first_link = true;
            for (const auto& link : rec.outbound_links) {
                if (!first_link) std::cout << ", ";
                first_link = false;
                std::cout << "{\"target\": \"" << link.target << "\", \"exists\": " << (link.exists ? "true" : "false")
                          << ", \"text\": \"" << link.text << "\", \"raw\": \"" << link.raw << "\"}";
            }
            std::cout << "]}";
        }
        std::cout << "\n  ]\n}\n";
        return EXIT_OK;
    }
    for (const auto& rec : bundle.concepts()) {
        std::cout << rec.id << ":\n";
        for (const auto& link : rec.outbound_links) {
            std::cout << "  " << (link.exists ? "->" : "-x") << ' ' << link.target;
            if (!link.text.empty()) std::cout << " (\"" << link.text << "\")";
            std::cout << '\n';
        }
    }
    return EXIT_OK;
}

int cmd_trust(int argc, char* argv[]) {
    OkfOptions opts;
    for (int i = 0; i < argc; ++i) parse_common_flags(i, argc, argv, opts);
    const auto args = collect_positionals(argc, argv);
    if (args.empty()) {
        okf_error("usage: mar okf trust <bundle-dir|archive>");
        return EXIT_USAGE;
    }
    const Bundle bundle = load_input(args[0], opts);
    for (const auto& rec : bundle.concepts()) {
        std::cout << rec.id << " [" << rec.status << "] " << trust_tier_name(rec.trust_tier);
        if (!opts.today.empty() && rec.document.frontmatter.stale_after() &&
            *rec.document.frontmatter.stale_after() <= opts.today) {
            std::cout << " STALE";
        }
        std::cout << '\n';
    }
    return EXIT_OK;
}

std::set<std::string> parse_ignore_rules(const std::string& csv) {
    std::set<std::string> out;
    std::string token;
    for (char c : csv) {
        if (c == ',') {
            if (!token.empty()) out.insert(token);
            token.clear();
        } else {
            token.push_back(c);
        }
    }
    if (!token.empty()) out.insert(token);
    return out;
}

int cmd_lint(int argc, char* argv[]) {
    OkfOptions opts;
    for (int i = 0; i < argc; ++i) parse_common_flags(i, argc, argv, opts);
    const auto args = collect_positionals(argc, argv);
    if (args.empty()) {
        okf_error("usage: mar okf lint <bundle-dir|archive>");
        return EXIT_USAGE;
    }
    BundleLoadOptions load_opts;
    load_opts.use_cache = !opts.no_cache;
    auto source = open_bundle_source(args[0]);
    const Bundle bundle = Bundle::load(*source, load_opts);
    const Report report = lint_bundle(
        bundle, opts.today.empty() ? std::nullopt : std::optional<std::string>(opts.today),
        parse_ignore_rules(opts.ignore_rules));
    size_t warnings = 0;
    for (const auto& d : report.diagnostics()) {
        std::cout << '[';
        switch (d.severity) {
            case Severity::Error:
                std::cout << "error";
                break;
            case Severity::Warning:
                std::cout << "warning";
                ++warnings;
                break;
            case Severity::Info:
                std::cout << "info";
                break;
        }
        std::cout << "] ";
        if (!d.path.empty()) std::cout << d.path << ": ";
        std::cout << d.message << '\n';
    }
    if (warnings == 0) {
        std::cout << "clean lint\n";
        return EXIT_OK;
    }
    std::cout << warnings << " lint warning(s)\n";
    return EXIT_INTEGRITY;
}

int cmd_diff(int argc, char* argv[]) {
    OkfOptions opts;
    for (int i = 0; i < argc; ++i) parse_common_flags(i, argc, argv, opts);
    const auto args = collect_positionals(argc, argv);
    if (args.size() < 2) {
        okf_error("usage: mar okf diff <a> <b>");
        return EXIT_USAGE;
    }
    BundleLoadOptions load_opts;
    load_opts.use_cache = false;
    auto a_source = open_bundle_source(args[0]);
    auto b_source = open_bundle_source(args[1]);
    const Bundle a = Bundle::load(*a_source, load_opts);
    const Bundle b = Bundle::load(*b_source, load_opts);
    const BundleDiff diff = bundle_diff(a, b);
    std::cout << args[0] << " -> " << args[1] << '\n';
    std::cout << diff.format();
    size_t changes = diff.added.size() + diff.removed.size() + diff.renamed.size() + diff.content.size() +
                     diff.frontmatter.size() + diff.trust.size() + diff.added_links.size() + diff.removed_links.size() +
                     diff.mended_links.size() + diff.broken_links.size();
    std::cout << changes << " change(s).\n";
    return EXIT_OK;
}

int cmd_fmt(int argc, char* argv[]) {
    OkfOptions opts;
    for (int i = 0; i < argc; ++i) parse_common_flags(i, argc, argv, opts);
    const auto args = collect_positionals(argc, argv);
    if (args.empty()) {
        okf_error("usage: mar okf fmt <file> [-w]");
        return EXIT_USAGE;
    }
    std::ifstream in(args[0]);
    if (!in) {
        okf_error("cannot read file: " + args[0]);
        return EXIT_ERROR;
    }
    std::string text((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    DocumentError err;
    auto doc = Document::parse(text, &err);
    if (!doc) {
        okf_error(err.message);
        return EXIT_INTEGRITY;
    }
    const std::string formatted = doc->serialize();
    if (opts.write_file) {
        std::ofstream out(args[0]);
        out << formatted;
        std::cout << "formatted " << args[0] << '\n';
    } else {
        std::cout << formatted;
    }
    return EXIT_OK;
}

int cmd_index(int argc, char* argv[]) {
    for (int i = 0; i < argc; ++i) {
        if (argv[i] == std::string("-h") || argv[i] == std::string("--help")) {
            print_okf_usage();
            return EXIT_OK;
        }
    }
    const auto args = collect_positionals(argc, argv);
    if (args.empty()) {
        okf_error("usage: mar okf index <bundle-dir>");
        return EXIT_USAGE;
    }
    if (!std::filesystem::is_directory(args[0])) {
        okf_error("index requires a bundle directory");
        return EXIT_ERROR;
    }
    const auto written = regenerate_indexes(args[0]);
    if (written.empty()) {
        std::cout << "no index files written (empty bundle?)\n";
        return EXIT_OK;
    }
    for (const auto& path : written) std::cout << "wrote " << path << '\n';
    return EXIT_OK;
}

int cmd_computations(int argc, char* argv[]) {
    OkfOptions opts;
    for (int i = 0; i < argc; ++i) parse_common_flags(i, argc, argv, opts);
    const auto args = collect_positionals(argc, argv);
    if (args.empty()) {
        okf_error("usage: mar okf computations <bundle-dir|archive>");
        return EXIT_USAGE;
    }
    const Bundle bundle = load_input(args[0], opts);
    bool any = false;
    for (const auto& rec : bundle.concepts()) {
        if (!is_attested_computation_type(rec.document.frontmatter)) continue;
        any = true;
        const auto contract = parse_attested_computation(rec.document.frontmatter, rec.document.body);
        std::cout << rec.id;
        if (auto title = rec.document.frontmatter.title()) std::cout << " — " << *title;
        std::cout << '\n';
        std::cout << "  runtime: " << contract.runtime.value_or("(missing)") << '\n';
        if (contract.computation.kind == ComputationSourceKind::File) {
            std::cout << "  computation: file " << contract.computation.file_path << '\n';
        } else if (contract.computation.kind == ComputationSourceKind::Inline) {
            std::cout << "  computation: inline (" << contract.computation.inline_code.code.size() << " bytes)\n";
        } else {
            std::cout << "  computation: (missing)\n";
        }
        if (!contract.parameters.empty()) {
            std::cout << "  parameters:\n";
            for (const auto& p : contract.parameters) {
                std::cout << "    - " << p.name.value_or("(unnamed)");
                if (p.type) std::cout << ": " << *p.type;
                if (p.is_required()) std::cout << " (required)";
                std::cout << '\n';
            }
        }
        if (!rec.backlinks.empty()) {
            std::cout << "  used by:";
            for (const auto& back : rec.backlinks) std::cout << ' ' << back;
            std::cout << '\n';
        }
        std::cout << '\n';
    }
    if (!any) std::cout << "no Attested Computation concepts found\n";
    return EXIT_OK;
}

}  // namespace

void print_okf_usage() {
    std::cout << R"(Usage: mar okf <subcommand> [options] <arguments>

Subcommands:
  pack      Pack a bundle directory or archive into a .mar file
  unpack    Extract an OKF archive to a directory
  info      Summarize concepts, types, and trust tiers
  ls        List concepts
  cat       Show one concept document
  validate  Check OKF conformance
  lint      Opinionated bundle hygiene checks (L1–L16)
  diff      OKF-semantics diff between two bundles
  fmt       Normalize a concept document
  index     Regenerate index.md files (directory only)
  computations  List Attested Computation contracts
  graph     Show the cross-link graph
  trust     Show trust tier and status per concept

Common options:
  --no-cache         Ignore or skip the .okf/ cache
  --today DATE       Pin staleness checks (YYYY-MM-DD)
  -f, --output PATH  Output path for pack
  -o, --output PATH  Output directory for unpack
  --format text|json Output format (ls, graph, validate)
  --type TYPE        Filter by concept type (ls)
  --strict           Fail pack if validate errors exist
  --force            Allow pack when source contains .okf/
  --include-cache    Include .okf/ when unpacking
  --frontmatter      Show only frontmatter (cat)
  --actor ID         Record packer in manifest (pack)
  --ignore RULES     Comma-separated lint codes to ignore (lint)
  --write, -w        Write formatted output back to file (fmt)

Examples:
  mar okf pack ./bundle -f bundle.mar
  mar okf info bundle.mar
  mar okf validate bundle.mar
  mar okf lint bundle.mar --today 2026-07-01
  mar okf diff ./bundle-v1 ./bundle-v2
  mar okf index ./bundle
  mar okf cat bundle.mar metrics/revenue
)";
}

int cmd_okf(int argc, char* argv[]) {
    if (argc == 0) {
        print_okf_usage();
        return EXIT_USAGE;
    }
    const std::string sub = argv[0];
    if (sub == "-h" || sub == "--help" || sub == "help") {
        print_okf_usage();
        return EXIT_OK;
    }
    if (sub == "pack") return cmd_pack(argc - 1, argv + 1);
    if (sub == "unpack") return cmd_unpack(argc - 1, argv + 1);
    if (sub == "info") return cmd_info(argc - 1, argv + 1);
    if (sub == "ls") return cmd_ls(argc - 1, argv + 1);
    if (sub == "cat") return cmd_cat(argc - 1, argv + 1);
    if (sub == "validate") return cmd_validate(argc - 1, argv + 1);
    if (sub == "lint") return cmd_lint(argc - 1, argv + 1);
    if (sub == "diff") return cmd_diff(argc - 1, argv + 1);
    if (sub == "fmt") return cmd_fmt(argc - 1, argv + 1);
    if (sub == "index") return cmd_index(argc - 1, argv + 1);
    if (sub == "computations") return cmd_computations(argc - 1, argv + 1);
    if (sub == "graph") return cmd_graph(argc - 1, argv + 1);
    if (sub == "trust") return cmd_trust(argc - 1, argv + 1);
    okf_error("unknown okf subcommand: " + sub);
    print_okf_usage();
    return EXIT_USAGE;
}

}  // namespace mar::okf
