#include "mar/diff.hpp"
#include "mar/checksum.hpp"
#include <iostream>
#include <iomanip>
#include <sstream>

namespace mar {

namespace {
    std::string format_bytes(u64 bytes) {
        const char* units[] = {"B", "KB", "MB", "GB", "TB"};
        int unit = 0;
        double size = static_cast<double>(bytes);
        while (size >= 1024 && unit < 4) {
            size /= 1024;
            unit++;
        }
        std::stringstream ss;
        ss << std::fixed << std::setprecision(2) << size << " " << units[unit];
        return ss.str();
    }
}

DiffStatistics ArchiveDiffer::compare(const MarReader& source, const MarReader& target) {
    stats_ = DiffStatistics();
    
    // Compare file-level information
    auto source_names = source.get_names();
    auto target_names = target.get_names();
    std::unordered_set<std::string> source_names_set(source_names.begin(), source_names.end());
    std::unordered_set<std::string> target_names_set(target_names.begin(), target_names.end());

    for (const auto& name : source_names) {
        auto s_entry_opt = source.find_file(name);
        if (!s_entry_opt) continue;
        
        if (target_names_set.count(name)) {
            auto t_entry_opt = target.find_file(name);
            if (t_entry_opt) {
                const auto& s_entry = s_entry_opt->second;
                const auto& t_entry = t_entry_opt->second;
                
                bool modified = (s_entry.logical_size != t_entry.logical_size);
                if (!modified && source.has_hashes() && target.has_hashes()) {
                    auto s_hash = source.get_hash(s_entry_opt->first);
                    auto t_hash = target.get_hash(t_entry_opt->first);
                    if (s_hash && t_hash && *s_hash != *t_hash) {
                        modified = true;
                    }
                }
                
                if (modified) {
                    stats_.files_modified++;
                    stats_.bytes_modified += s_entry.logical_size;
                } else {
                    stats_.files_unchanged++;
                    stats_.bytes_unchanged += s_entry.logical_size;
                }
            }
        } else {
            stats_.files_deleted++;
            stats_.bytes_deleted += s_entry_opt->second.logical_size;
        }
    }

    for (const auto& name : target_names) {
        if (!source_names_set.count(name)) {
            auto t_entry_opt = target.find_file(name);
            if (t_entry_opt) {
                stats_.files_added++;
                stats_.bytes_added += t_entry_opt->second.logical_size;
            }
        }
    }

    return stats_;
}

void ArchiveDiffer::print_summary(std::ostream& out, const std::string& source_name, const std::string& target_name) {
    out << "\nDiff Summary: " << source_name << " -> " << target_name << "\n";
    out << std::string(70, '=') << "\n\n";
    
    out << "Files:\n";
    out << "  Added:     " << std::setw(6) << stats_.files_added << "\n";
    out << "  Deleted:   " << std::setw(6) << stats_.files_deleted << "\n";
    out << "  Modified:  " << std::setw(6) << stats_.files_modified << "\n";
    out << "  Unchanged: " << std::setw(6) << stats_.files_unchanged << "\n";
    out << "  Total:     " << std::setw(6) << (stats_.files_added + stats_.files_deleted + 
                                                stats_.files_modified + stats_.files_unchanged) << "\n";
    
    out << "\nSize Changes:\n";
    out << "  Added:     " << format_bytes(stats_.bytes_added) << "\n";
    out << "  Deleted:   " << format_bytes(stats_.bytes_deleted) << "\n";
    out << "  Modified:  " << format_bytes(stats_.bytes_modified) << "\n";
    out << "  Unchanged: " << format_bytes(stats_.bytes_unchanged) << "\n";
}

std::vector<FileDiff> ArchiveDiffer::get_file_diffs(const MarReader& source, const MarReader& target) {
    std::vector<FileDiff> diffs;
    
    auto source_names = source.get_names();
    auto target_names = target.get_names();
    std::unordered_set<std::string> source_names_set(source_names.begin(), source_names.end());
    std::unordered_set<std::string> target_names_set(target_names.begin(), target_names.end());

    // Check deleted and modified files
    for (const auto& name : source_names) {
        FileDiff diff;
        diff.path = name;
        
        auto s_entry_opt = source.find_file(name);
        if (!s_entry_opt) continue;
        
        diff.old_size = s_entry_opt->second.logical_size;
        if (source.has_hashes()) {
            auto hash = source.get_hash(s_entry_opt->first);
            if (hash) {
                diff.old_hash = hash_to_hex(hash->data(), hash->size());
            }
        }
        
        if (target_names_set.count(name)) {
            auto t_entry_opt = target.find_file(name);
            if (t_entry_opt) {
                const auto& t_entry = t_entry_opt->second;
                diff.new_size = t_entry.logical_size;
                if (target.has_hashes()) {
                    auto hash = target.get_hash(t_entry_opt->first);
                    if (hash) {
                        diff.new_hash = hash_to_hex(hash->data(), hash->size());
                    }
                }
                
                // Determine if modified
                bool modified = (diff.old_size != diff.new_size);
                if (!modified && source.has_hashes() && target.has_hashes()) {
                    modified = (diff.old_hash != diff.new_hash);
                }
                
                diff.type = modified ? FileDiff::MODIFIED : FileDiff::UNCHANGED;
                if (modified) {
                    stats_.files_modified++;
                    stats_.bytes_modified += diff.old_size;
                } else {
                    stats_.files_unchanged++;
                    stats_.bytes_unchanged += diff.old_size;
                }
            }
        } else {
            diff.type = FileDiff::DELETED;
            stats_.files_deleted++;
            stats_.bytes_deleted += diff.old_size;
        }
        
        diffs.push_back(diff);
    }

    // Check added files
    for (const auto& name : target_names) {
        if (!source_names_set.count(name)) {
            FileDiff diff;
            diff.path = name;
            diff.type = FileDiff::ADDED;
            
            auto t_entry_opt = target.find_file(name);
            if (t_entry_opt) {
                diff.new_size = t_entry_opt->second.logical_size;
                if (target.has_hashes()) {
                    auto hash = target.get_hash(t_entry_opt->first);
                    if (hash) {
                        diff.new_hash = hash_to_hex(hash->data(), hash->size());
                    }
                }
                stats_.files_added++;
                stats_.bytes_added += diff.new_size;
            }
            
            diffs.push_back(diff);
        }
    }

    return diffs;
}

void ArchiveDiffer::print_file_diffs(std::ostream& out, const std::vector<FileDiff>& diffs) {
    // Git-diff style output
    for (const auto& diff : diffs) {
        switch (diff.type) {
            case FileDiff::ADDED:
                out << "A  " << diff.path;
                if (diff.new_size > 0) out << " (" << format_bytes(diff.new_size) << ")";
                if (!diff.new_hash.empty()) out << " [" << diff.new_hash.substr(0, 8) << "]";
                out << "\n";
                break;
            case FileDiff::DELETED:
                out << "D  " << diff.path;
                if (diff.old_size > 0) out << " (" << format_bytes(diff.old_size) << ")";
                if (!diff.old_hash.empty()) out << " [" << diff.old_hash.substr(0, 8) << "]";
                out << "\n";
                break;
            case FileDiff::MODIFIED:
                out << "M  " << diff.path;
                if (diff.old_size != diff.new_size) {
                    out << " (" << format_bytes(diff.old_size) << " → " << format_bytes(diff.new_size) << ")";
                }
                if (!diff.old_hash.empty() && !diff.new_hash.empty() && diff.old_hash != diff.new_hash) {
                    out << " [" << diff.old_hash.substr(0, 8) << " → " << diff.new_hash.substr(0, 8) << "]";
                }
                out << "\n";
                break;
            case FileDiff::UNCHANGED:
                // Don't show unchanged files in diff view (like git)
                break;
        }
    }
}

} // namespace mar
