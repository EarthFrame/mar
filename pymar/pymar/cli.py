import sys
import os
import argparse
from typing import List, Optional
import pymar
from pymar.core import slice_archive, create_archive, MarArchive
from pymar.tools import mar_list, mar_get, mar_extract, mar_validate, mar_header, mar_version

def print_usage():
    print("""Usage: pymar <command> [options] <arguments>

Commands:
  slice    Extract a subset of files from a local or remote archive into a new archive
  create   Create an archive from files or directories
  list     List contents of an archive
  get      Extract a specific file to stdout
  extract  Extract files from an archive to a directory
  validate Validate archive integrity
  header   Display archive header information
  version  Display tool version

Run 'pymar <command> --help' for command-specific options.""")

def cmd_slice(argv: List[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="pymar slice",
        description="Extract a subset of files from a local or remote (S3/HTTP) archive into a new archive."
    )
    parser.add_argument("archive", help="Path or URI (e.g. s3://bucket/path.mar) to source archive")
    parser.add_argument("patterns", nargs="*", help="Positional glob patterns or filenames to include")
    parser.add_argument("-o", "--output", required=True, help="Path to destination archive")
    parser.add_argument("-i", "--include", action="append", default=[], help="Include pattern (repeatable)")
    parser.add_argument("-x", "--exclude", action="append", default=[], help="Exclude pattern (repeatable)")
    parser.add_argument("-T", "--files-from", help="Read include files/patterns from file (- for stdin)")
    parser.add_argument("--exclude-from", help="Read exclude files/patterns from file")
    parser.add_argument("-c", "--compression", default="zstd", choices=["zstd", "lz4", "gzip", "bzip2", "none"], help="Output compression algorithm")
    parser.add_argument("-f", "--force", action="store_true", default=False, help="Overwrite existing output archive")
    parser.add_argument("-j", "--threads", type=int, default=0, help="Parallel worker threads")
    parser.add_argument("-v", "--verbose", action="count", default=0, help="Verbose output")

    args = parser.parse_args(argv)

    try:
        out = slice_archive(
            archive_path_or_url=args.archive,
            output_path=args.output,
            files=args.patterns or None,
            includes=args.include or None,
            excludes=args.exclude or None,
            files_from=args.files_from,
            exclude_from=args.exclude_from,
            compression=args.compression,
            force=args.force,
            threads=args.threads
        )
        if args.verbose:
            print(f"mar: successfully sliced {args.archive} -> {out}")
        return 0
    except Exception as e:
        sys.stderr.write(f"mar: error: {e}\n")
        return 1

def cmd_create(argv: List[str]) -> int:
    parser = argparse.ArgumentParser(prog="pymar create", description="Create a new MAR archive.")
    parser.add_argument("archive", help="Output archive path")
    parser.add_argument("files", nargs="+", help="Files or directories to pack")
    parser.add_argument("-c", "--compression", default="zstd", choices=["zstd", "lz4", "gzip", "bzip2", "none"])
    args = parser.parse_args(argv)
    try:
        create_archive(args.archive, args.files, compression=args.compression)
        return 0
    except Exception as e:
        sys.stderr.write(f"mar: error: {e}\n")
        return 1

def cmd_list(argv: List[str]) -> int:
    parser = argparse.ArgumentParser(prog="pymar list", description="List archive contents.")
    parser.add_argument("archive", help="Path or URI to archive")
    args = parser.parse_args(argv)
    try:
        arc = pymar.open(args.archive)
        for name in arc.list_files():
            print(name)
        return 0
    except Exception as e:
        sys.stderr.write(f"mar: error: {e}\n")
        return 1

def cmd_get(argv: List[str]) -> int:
    parser = argparse.ArgumentParser(prog="pymar get", description="Get a file from an archive.")
    parser.add_argument("archive", help="Path or URI to archive")
    parser.add_argument("filename", help="File name in archive")
    args = parser.parse_args(argv)
    try:
        arc = pymar.open(args.archive)
        content = arc.read_file(args.filename)
        sys.stdout.buffer.write(content)
        return 0
    except Exception as e:
        sys.stderr.write(f"mar: error: {e}\n")
        return 1

def cmd_extract(argv: List[str]) -> int:
    parser = argparse.ArgumentParser(prog="pymar extract", description="Extract archive contents.")
    parser.add_argument("archive", help="Path to archive")
    parser.add_argument("-o", "--output", default=".", help="Output directory")
    args = parser.parse_args(argv)
    try:
        mar_extract(args.archive, args.output)
        return 0
    except Exception as e:
        sys.stderr.write(f"mar: error: {e}\n")
        return 1

def cmd_validate(argv: List[str]) -> int:
    parser = argparse.ArgumentParser(prog="pymar validate", description="Validate archive integrity.")
    parser.add_argument("archive", help="Path to archive")
    args = parser.parse_args(argv)
    try:
        ok = mar_validate(args.archive)
        if ok:
            print("Archive integrity: OK")
            return 0
        else:
            sys.stderr.write("Archive integrity: FAILED\n")
            return 1
    except Exception as e:
        sys.stderr.write(f"mar: error: {e}\n")
        return 1

def main(argv: Optional[List[str]] = None) -> int:
    if argv is None:
        argv = sys.argv[1:]

    if not argv or argv[0] in ("-h", "--help"):
        print_usage()
        return 0

    if argv[0] == "--version":
        print(mar_version())
        return 0

    cmd = argv[0]
    cmd_args = argv[1:]

    if cmd == "slice":
        return cmd_slice(cmd_args)
    elif cmd == "create":
        return cmd_create(cmd_args)
    elif cmd == "list":
        return cmd_list(cmd_args)
    elif cmd == "get":
        return cmd_get(cmd_args)
    elif cmd == "extract":
        return cmd_extract(cmd_args)
    elif cmd == "validate":
        return cmd_validate(cmd_args)
    elif cmd == "version":
        print(mar_version())
        return 0
    else:
        sys.stderr.write(f"Unknown command: {cmd}\n")
        print_usage()
        return 2

if __name__ == "__main__":
    sys.exit(main())
