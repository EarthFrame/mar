#!/usr/bin/env python3
"""
Metadata parity check: TAR vs MAR.

This script:
- creates a small synthetic dataset with known metadata (modes, mtimes, symlink, empty file)
- archives it with both tar and mar
- compares what each archive *stores* (using tarfile for TAR, `mar list --json` for MAR)
- compares what each tool *restores on extraction* (non-root: we don't expect ownership restoration)

Outputs markdown to stdout, and optionally writes docs/METADATA_PARITY.md.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import stat
import subprocess
import tarfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MAR = REPO_ROOT / "mar"


@dataclass(frozen=True)
class Entry:
    name: str
    type: str  # file|dir|symlink
    size: int
    mode: int | None
    uid: int | None
    gid: int | None
    mtime: int | None
    atime: int | None
    ctime: int | None
    symlink_target: str | None


def run(cmd: list[str], cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, cwd=str(cwd) if cwd else None, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def write_file(p: Path, data: bytes) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "wb") as f:
        f.write(data)


def set_mtime(p: Path, mtime: int) -> None:
    os.utime(p, (mtime, mtime), follow_symlinks=False)


def build_dataset(root: Path) -> None:
    if root.exists():
        shutil.rmtree(root)
    (root / "subdir").mkdir(parents=True)

    write_file(root / "a.txt", b"hello\n")
    write_file(root / "empty.txt", b"")
    write_file(root / "subdir" / "exec.sh", b"#!/bin/sh\necho hi\n")
    os.chmod(root / "subdir" / "exec.sh", 0o755)

    # Stable timestamps (seconds since epoch)
    fixed = 1700000000
    set_mtime(root / "a.txt", fixed)
    set_mtime(root / "empty.txt", fixed + 1)
    set_mtime(root / "subdir" / "exec.sh", fixed + 2)

    # Symlink
    os.symlink("a.txt", root / "link_to_a.txt")
    set_mtime(root / "link_to_a.txt", fixed + 3)

    # Set directory mtimes last (creating children updates directory mtime).
    set_mtime(root / "subdir", fixed - 9)
    set_mtime(root, fixed - 10)


def lstat_entry(root: Path, rel: str) -> Entry:
    p = root / rel
    st = os.lstat(p)
    mode = stat.S_IMODE(st.st_mode)
    if stat.S_ISLNK(st.st_mode):
        t = os.readlink(p)
        et = "symlink"
        sz = 0
    elif stat.S_ISDIR(st.st_mode):
        t = None
        et = "dir"
        sz = 0
    else:
        t = None
        et = "file"
        sz = st.st_size
    return Entry(
        name=rel,
        type=et,
        size=sz,
        mode=mode,
        uid=st.st_uid,
        gid=st.st_gid,
        mtime=int(st.st_mtime),
        atime=int(st.st_atime),
        ctime=int(st.st_ctime),
        symlink_target=t,
    )


def read_tar_entries(tar_path: Path) -> dict[str, Entry]:
    out: dict[str, Entry] = {}
    with tarfile.open(tar_path, "r:*") as tf:
        for ti in tf.getmembers():
            # tar stores names without leading "./" usually
            name = ti.name
            if name.endswith("/") and ti.isdir():
                name = name[:-1]
            et = "dir" if ti.isdir() else ("symlink" if ti.issym() else "file")
            out[name] = Entry(
                name=name,
                type=et,
                size=ti.size if ti.isfile() else 0,
                mode=(ti.mode if ti.mode is not None else None),
                uid=ti.uid,
                gid=ti.gid,
                mtime=int(ti.mtime) if ti.mtime is not None else None,
                atime=None,
                ctime=None,
                symlink_target=(ti.linkname if ti.issym() else None),
            )
    return out


def read_mar_entries(mar_bin: Path, mar_path: Path) -> dict[str, Entry]:
    r = run([str(mar_bin), "list", "--json", str(mar_path)])
    data: list[dict[str, Any]] = json.loads(r.stdout)
    out: dict[str, Entry] = {}
    for obj in data:
        if "_metadata" in obj:
            continue
        name = obj["name"]
        mode_val = int(obj["mode"]) if "mode" in obj else None
        # MAR stores full st_mode (type + perms). TAR typically stores perms only.
        # For parity checks, compare permission bits.
        if mode_val is not None:
            mode_val = mode_val & 0o777

        out[name] = Entry(
            name=name,
            type=obj.get("type", "other"),
            size=int(obj.get("size", 0)),
            mode=mode_val,
            uid=(int(obj["uid"]) if "uid" in obj else None),
            gid=(int(obj["gid"]) if "gid" in obj else None),
            mtime=(int(obj["mtime"]) if "mtime" in obj else None),
            atime=(int(obj["atime"]) if "atime" in obj else None),
            ctime=(int(obj["ctime"]) if "ctime" in obj else None),
            symlink_target=obj.get("symlink_target"),
        )
    return out


def compare_field(a: Entry | None, b: Entry | None, field: str) -> str:
    if a is None or b is None:
        return "N/A"
    # Symlink permissions and timestamps are not consistently meaningful across tools/platforms.
    # Treat them as informational only for parity tables.
    if a.type == "symlink" and field in {"mode", "mtime", "atime", "ctime"}:
        return "N/A"
    av = getattr(a, field)
    bv = getattr(b, field)
    return "✓" if av == bv else "✗"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mar", default=str(DEFAULT_MAR), help="Path to mar binary (default: ./mar)")
    ap.add_argument("--write-doc", action="store_true", help="Write docs/METADATA_PARITY.md")
    args = ap.parse_args()

    mar_bin = Path(args.mar).resolve()
    if not mar_bin.exists():
        raise SystemExit(f"mar binary not found: {mar_bin}")

    scratch = REPO_ROOT / "benchmarks" / "scratch_parity"
    dataset = scratch / "dataset"
    build_dataset(dataset)

    tar_path = scratch / "parity.tar"
    mar_path = scratch / "parity.mar"
    tar_out = scratch / "out_tar"
    mar_out = scratch / "out_mar"
    for p in (tar_out, mar_out):
        if p.exists():
            shutil.rmtree(p)
        p.mkdir(parents=True, exist_ok=True)

    # Create archives (store a top-level folder name for stable paths)
    if tar_path.exists():
        tar_path.unlink()
    run(["tar", "cf", str(tar_path), "-C", str(scratch), "dataset"])

    if mar_path.exists():
        mar_path.unlink()
    run([str(mar_bin), "create", "--force", "-c", "none", str(mar_path), str(dataset)])

    # Read "stored" metadata
    tar_entries = read_tar_entries(tar_path)
    mar_entries = read_mar_entries(mar_bin, mar_path)

    # Extract and read "restored" metadata (non-root)
    run(["tar", "xf", str(tar_path), "-C", str(tar_out), "-p"])
    run([str(mar_bin), "extract", "-o", str(mar_out), str(mar_path)])

    # Choose a few representative entries (including empty file + exec + symlink)
    # TAR names are "dataset/<rel>", MAR names are "dataset/<rel>" for directories.
    keys = [
        "dataset",
        "dataset/a.txt",
        "dataset/empty.txt",
        "dataset/subdir",
        "dataset/subdir/exec.sh",
        "dataset/link_to_a.txt",
    ]

    # Build original/extracted stat maps
    orig_stat = {k: lstat_entry(scratch, k) for k in keys if (scratch / k).exists() or (scratch / k).is_symlink()}
    tar_stat = {k: lstat_entry(tar_out, k) for k in keys if (tar_out / k).exists() or (tar_out / k).is_symlink()}
    mar_stat = {k: lstat_entry(mar_out, k) for k in keys if (mar_out / k).exists() or (mar_out / k).is_symlink()}

    # Markdown output
    lines: list[str] = []
    lines.append("# TAR vs MAR metadata parity")
    lines.append("")
    lines.append("This is an empirical check on a small synthetic dataset created by `benchmarks/scripts/metadata_parity.py`.")
    lines.append("")
    lines.append("## TL;DR parity summary")
    lines.append("")
    lines.append("- **Stored in archive**:")
    lines.append("  - **Matches TAR (common case)**: path names, type (file/dir/symlink), logical size, permission bits, mtime.")
    lines.append("  - **Differs from TAR today**: MAR currently stores **uid/gid as 0** (portable), so those won’t match TAR’s numeric owner fields.")
    lines.append("- **Restored on extraction (non-root)**:")
    lines.append("  - **TAR** restores **mode + mtime** (with `tar -p`).")
    lines.append("  - **MAR** restores **mode only**; it does **not apply timestamps or ownership** on extraction yet.")
    lines.append("")
    lines.append("## What the archive stores (header metadata)")
    lines.append("")
    lines.append("Notes:")
    lines.append("- For **mode**, we compare **permission bits** only (MAR stores full `st_mode` including type bits).")
    lines.append("- MAR currently stores **uid/gid as 0** (see `src/writer.cpp`), so uid/gid will not match TAR.")
    lines.append("")
    lines.append("| Entry | Field | TAR stores | MAR stores | Match |")
    lines.append("|---|---:|---:|---:|:---:|")
    for k in keys:
        t = tar_entries.get(k)
        m = mar_entries.get(k)
        for field in ["type", "size", "mode", "uid", "gid", "mtime", "symlink_target"]:
            lines.append(
                f"| `{k}` | `{field}` | "
                f"{'yes' if t and getattr(t, field) is not None else 'no'} | "
                f"{'yes' if m and getattr(m, field) is not None else 'no'} | "
                f"{compare_field(t, m, field)} |"
            )

    lines.append("")
    lines.append("## What extraction restores (non-root)")
    lines.append("")
    lines.append("| Entry | Field | TAR matches original | MAR matches original |")
    lines.append("|---|---:|:---:|:---:|")
    for k in keys:
        o = orig_stat.get(k)
        t = tar_stat.get(k)
        m = mar_stat.get(k)
        for field in ["type", "size", "mode", "mtime", "symlink_target"]:
            t_ok = compare_field(o, t, field)
            m_ok = compare_field(o, m, field)
            lines.append(f"| `{k}` | `{field}` | {t_ok} | {m_ok} |")

    lines.append("")
    lines.append("## Summary (current behavior)")
    lines.append("")
    lines.append("- **Stored metadata parity**:")
    lines.append("  - MAR stores **uid/gid/mode/mtime/atime/ctime** per entry (`PosixEntry`) and symlink targets.")
    lines.append("  - TAR stores **uid/gid/mode/mtime** per entry by default; atime/ctime and xattrs typically require PAX extensions.")
    lines.append("- **Extraction parity (non-root)**:")
    lines.append("  - TAR generally restores **mode + mtime** when extracting with `-p` (ownership may require privileges).")
    lines.append("  - MAR currently restores **mode only**; it **does not apply timestamps or ownership** on extraction yet.")
    lines.append("")

    out_md = "\n".join(lines) + "\n"
    print(out_md, end="")

    if args.write_doc:
        doc_path = REPO_ROOT / "docs" / "METADATA_PARITY.md"
        doc_path.parent.mkdir(parents=True, exist_ok=True)
        doc_path.write_text(out_md, encoding="utf-8")


if __name__ == "__main__":
    main()

