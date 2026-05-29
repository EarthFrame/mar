#ifdef MAR_HAS_FUSE

#if MAR_HAS_FUSE == 3
#define FUSE_USE_VERSION 31
#include <fuse3/fuse.h>
#else
#define FUSE_USE_VERSION 26
#include <fuse.h>
#endif

#include "mar/fuse.hpp"
#include "mar/errors.hpp"
#include <sys/stat.h>
#include <cstring>
#include <iostream>
#include <vector>
#include <string>
#include <algorithm>
#include <map>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <cstring>
#include <iostream>
#include <vector>
#include <string>
#include <algorithm>
#include <map>

#if defined(__APPLE__) && MAR_HAS_FUSE == 3
#define MAR_FUSE_STAT struct fuse_darwin_attr
#define MAR_FUSE_FILL_DIR fuse_darwin_fill_dir_t
#else
#define MAR_FUSE_STAT struct stat
#define MAR_FUSE_FILL_DIR fuse_fill_dir_t
#endif

namespace mar {

namespace {

// Helper to get MarFuse instance from FUSE context
MarFuse* get_fuse_instance() {
    return static_cast<MarFuse*>(fuse_get_context()->private_data);
}

// Map EntryType to mode_t
mode_t entry_type_to_mode(EntryType type) {
    switch (type) {
        case EntryType::RegularFile: return S_IFREG;
        case EntryType::Directory:   return S_IFDIR;
        case EntryType::Symlink:     return S_IFLNK;
        case EntryType::CharDevice:  return S_IFCHR;
        case EntryType::BlockDevice: return S_IFBLK;
        case EntryType::Fifo:        return S_IFIFO;
        case EntryType::Socket:      return S_IFSOCK;
        default:                     return 0;
    }
}

// FUSE callbacks
int mar_getattr(const char* path, MAR_FUSE_STAT* stbuf, struct fuse_file_info* fi) {
    (void)fi;
    auto* fuse = get_fuse_instance();
    auto reader = fuse->reader();
    
    std::memset(stbuf, 0, sizeof(MAR_FUSE_STAT));

    // The path here is relative to the mount point.
    // We need to map it back to the archive path using the prefix.
    std::string internal_path = fuse->prefix();
    if (!internal_path.empty() && internal_path.back() != '/') internal_path += '/';
    
    if (std::strcmp(path, "/") == 0) {
        // If we are at the root of the mount, we might be looking at a directory in the archive
        if (internal_path.empty()) {
#if defined(__APPLE__) && MAR_HAS_FUSE == 3
            stbuf->mode = S_IFDIR | 0755;
            stbuf->nlink = 2;
#else
            stbuf->st_mode = S_IFDIR | 0755;
            stbuf->st_nlink = 2;
#endif
            return 0;
        }
        // Remove trailing slash for lookup
        std::string lookup = internal_path;
        if (!lookup.empty() && lookup.back() == '/') lookup.pop_back();
        auto found = reader->find_file(lookup);
        if (!found) {
            // Try with slash
            found = reader->find_file(internal_path);
        }
        if (found) {
            const auto& entry = found->second;
#if defined(__APPLE__) && MAR_HAS_FUSE == 3
            stbuf->mode = entry_type_to_mode(entry.entry_type);
            stbuf->size = entry.logical_size;
            stbuf->nlink = (entry.entry_type == EntryType::Directory) ? 2 : 1;
#else
            stbuf->st_mode = entry_type_to_mode(entry.entry_type);
            stbuf->st_size = entry.logical_size;
            stbuf->st_nlink = (entry.entry_type == EntryType::Directory) ? 2 : 1;
#endif
            auto posix = reader->get_posix_meta(found->first);
            if (posix) {
#if defined(__APPLE__) && MAR_HAS_FUSE == 3
                stbuf->mode |= (posix->mode & 0777);
                stbuf->uid = posix->uid;
                stbuf->gid = posix->gid;
                stbuf->atimespec.tv_sec = posix->atime;
                stbuf->mtimespec.tv_sec = posix->mtime;
                stbuf->ctimespec.tv_sec = posix->ctime;
#else
                stbuf->st_mode |= (posix->mode & 0777);
                stbuf->st_uid = posix->uid;
                stbuf->st_gid = posix->gid;
                stbuf->st_atime = posix->atime;
                stbuf->st_mtime = posix->mtime;
                stbuf->st_ctime = posix->ctime;
#endif
            } else {
#if defined(__APPLE__) && MAR_HAS_FUSE == 3
                stbuf->mode |= 0755;
#else
                stbuf->st_mode |= 0755;
#endif
            }
            return 0;
        }
        // Root of mount that doesn't exist in archive? Should not happen if we planned well.
#if defined(__APPLE__) && MAR_HAS_FUSE == 3
        stbuf->mode = S_IFDIR | 0755;
        stbuf->nlink = 2;
#else
        stbuf->st_mode = S_IFDIR | 0755;
        stbuf->st_nlink = 2;
#endif
        return 0;
    }

    internal_path += (path + 1);

    auto found = reader->find_file(internal_path);
    if (!found) {
        // Try as directory
        std::string dir_path = internal_path;
        if (!dir_path.empty() && dir_path.back() != '/') dir_path += '/';
        found = reader->find_file(dir_path);
    }

    if (!found) return -ENOENT;

    const auto& entry = found->second;
#if defined(__APPLE__) && MAR_HAS_FUSE == 3
    stbuf->mode = entry_type_to_mode(entry.entry_type);
    stbuf->size = entry.logical_size;
    stbuf->nlink = (entry.entry_type == EntryType::Directory) ? 2 : 1;
#else
    stbuf->st_mode = entry_type_to_mode(entry.entry_type);
    stbuf->st_size = entry.logical_size;
    stbuf->st_nlink = (entry.entry_type == EntryType::Directory) ? 2 : 1;
#endif

    auto posix = reader->get_posix_meta(found->first);
    if (posix) {
#if defined(__APPLE__) && MAR_HAS_FUSE == 3
        stbuf->mode |= (posix->mode & 0777);
        stbuf->uid = posix->uid;
        stbuf->gid = posix->gid;
        stbuf->atimespec.tv_sec = posix->atime;
        stbuf->mtimespec.tv_sec = posix->mtime;
        stbuf->ctimespec.tv_sec = posix->ctime;
#else
        stbuf->st_mode |= (posix->mode & 0777);
        stbuf->st_uid = posix->uid;
        stbuf->st_gid = posix->gid;
        stbuf->st_atime = posix->atime;
        stbuf->st_mtime = posix->mtime;
        stbuf->st_ctime = posix->ctime;
#endif
    } else {
#if defined(__APPLE__) && MAR_HAS_FUSE == 3
        stbuf->mode |= 0644;
        if (entry.entry_type == EntryType::Directory) stbuf->mode |= 0111;
#else
        stbuf->st_mode |= 0644;
        if (entry.entry_type == EntryType::Directory) stbuf->st_mode |= 0111;
#endif
    }

    return 0;
}

int mar_readdir(const char* path, void* buf, MAR_FUSE_FILL_DIR filler, off_t offset, struct fuse_file_info* fi, enum fuse_readdir_flags flags) {
    (void)offset; (void)fi; (void)flags;
    auto* fuse = get_fuse_instance();
    auto reader = fuse->reader();

    filler(buf, ".", NULL, 0, (fuse_fill_dir_flags)0);
    filler(buf, "..", NULL, 0, (fuse_fill_dir_flags)0);

    std::string dir_path = fuse->prefix();
    if (!dir_path.empty() && dir_path.back() != '/') dir_path += '/';
    if (std::strcmp(path, "/") != 0) {
        dir_path += (path + 1);
    }
    if (!dir_path.empty() && dir_path.back() != '/') dir_path += '/';

    const auto& names = reader->get_names();
    const auto& entries = reader->get_file_entries();

    std::map<std::string, bool> seen;

    for (size_t i = 0; i < entries.size(); ++i) {
        const std::string& name = names[entries[i].name_id];
        if (name.size() > dir_path.size() && name.compare(0, dir_path.size(), dir_path) == 0) {
            std::string sub = name.substr(dir_path.size());
            size_t slash = sub.find('/');
            if (slash != std::string::npos) {
                sub = sub.substr(0, slash);
            }
            if (!sub.empty() && !seen[sub]) {
                filler(buf, sub.c_str(), NULL, 0, (fuse_fill_dir_flags)0);
                seen[sub] = true;
            }
        }
    }

    return 0;
}

int mar_open(const char* path, struct fuse_file_info* fi) {
    auto* fuse = get_fuse_instance();
    auto reader = fuse->reader();
    std::string internal_path = fuse->prefix();
    if (!internal_path.empty() && internal_path.back() != '/') internal_path += '/';
    internal_path += (path + 1);

    auto found = reader->find_file(internal_path);
    if (!found) return -ENOENT;
    if (found->second.entry_type != EntryType::RegularFile) return -EISDIR;
    
    if ((fi->flags & O_ACCMODE) != O_RDONLY) return -EACCES;
    
    return 0;
}

int mar_read(const char* path, char* buf, size_t size, off_t offset, struct fuse_file_info* fi) {
    (void)fi;
    auto* fuse = get_fuse_instance();
    auto reader = fuse->reader();
    std::string internal_path = fuse->prefix();
    if (!internal_path.empty() && internal_path.back() != '/') internal_path += '/';
    internal_path += (path + 1);

    auto found = reader->find_file(internal_path);
    if (!found) return -ENOENT;

    return (int)reader->read_file_range(found->first, (u64)offset, (u64)size, (u8*)buf);
}

int mar_readlink(const char* path, char* buf, size_t size) {
    auto* fuse = get_fuse_instance();
    auto reader = fuse->reader();
    std::string internal_path = fuse->prefix();
    if (!internal_path.empty() && internal_path.back() != '/') internal_path += '/';
    internal_path += (path + 1);

    auto found = reader->find_file(internal_path);
    if (!found) return -ENOENT;
    if (found->second.entry_type != EntryType::Symlink) return -EINVAL;

    auto target = reader->get_symlink_target(found->first);
    if (!target) return -EIO;

    std::strncpy(buf, target->c_str(), size - 1);
    buf[size - 1] = '\0';
    return 0;
}

static const struct fuse_operations mar_oper = {
    .getattr    = mar_getattr,
    .readlink   = mar_readlink,
    .open       = mar_open,
    .read       = mar_read,
    .readdir    = mar_readdir,
};

} // anonymous namespace

MarFuse::MarFuse(std::shared_ptr<MarReader> reader, const std::string& prefix)
    : reader_(std::move(reader)), prefix_(prefix) {}

MarFuse::~MarFuse() = default;

int MarFuse::mount(const std::string& mountpoint, bool foreground) {
    std::vector<const char*> args;
    args.push_back("mar");
    if (foreground) args.push_back("-f");
    
    // Performance and behavior options
    args.push_back("-o"); args.push_back("ro");
    args.push_back("-o"); args.push_back("kernel_cache");
    args.push_back("-o"); args.push_back("attr_timeout=3600");
    args.push_back("-o"); args.push_back("entry_timeout=3600");
#ifndef __APPLE__
    args.push_back("-o"); args.push_back("nonempty");
    args.push_back("-o"); args.push_back("auto_unmount");
#endif

    args.push_back(mountpoint.c_str());

    struct fuse_args f_args = FUSE_ARGS_INIT((int)args.size(), const_cast<char**>(args.data()));
    return fuse_main(f_args.argc, f_args.argv, &mar_oper, this);
}

bool MarFuse::unmount(const std::string& mountpoint) {
#ifdef __APPLE__
    std::string cmd = "umount " + mountpoint + " 2>/dev/null";
#else
    std::string cmd = "fusermount -u " + mountpoint + " 2>/dev/null";
#endif
    return std::system(cmd.c_str()) == 0;
}

} // namespace mar

#endif // MAR_HAS_FUSE
