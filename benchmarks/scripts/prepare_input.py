#!/usr/bin/env python3
import sys
import os
import subprocess
from pathlib import Path

def get_total_size(path):
    """Get the total size of a file or directory. 
    If it's a compressed tarball, try to get the decompressed size."""
    path = Path(path)
    if not path.exists():
        return 0
    
    if path.is_file():
        suffixes = path.suffixes
        if '.gz' in suffixes or '.tgz' in suffixes:
            try:
                # gzip -l works for single files, for tar.gz it shows the size of the tar
                result = subprocess.run(['gzip', '-l', str(path)], capture_output=True, text=True)
                lines = result.stdout.strip().split('\n')
                if len(lines) > 1:
                    return int(lines[1].split()[1])
            except:
                pass
        elif '.xz' in suffixes:
            try:
                result = subprocess.run(['xz', '-l', str(path)], capture_output=True, text=True)
                for line in result.stdout.split('\n'):
                    if 'uncompressed' in line.lower() or 'totals' in line:
                        # Totals: 1 file, 1.4 MiB (1,480,000 bytes), 2.1 GiB (2,250,000,000 bytes)
                        import re
                        match = re.search(r'\(([\d,]+) bytes\)', line)
                        if match:
                            return int(match.group(1).replace(',', ''))
            except:
                pass
        return path.stat().st_size
    else:
        # Directory - sum of all files
        total = 0
        for f in path.glob('**/*'):
            if f.is_file():
                total += f.stat().st_size
        return total

def get_blake3(path):
    """Calculate the BLAKE3 checksum of a file or directory content."""
    if not Path(path).exists():
        return "N/A"
    
    try:
        if os.path.isfile(path):
            result = subprocess.run(['b3sum', '--no-names', str(path)], capture_output=True, text=True, check=True)
            return result.stdout.strip()
        else:
            # For a directory, we b3sum all files in a deterministic order
            # and then b3sum the resulting list of hashes.
            cmd = f"find {path} -type f -print0 | sort -z | xargs -0 b3sum --no-names | b3sum --no-names"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=True)
            return result.stdout.strip()
    except subprocess.CalledProcessError:
        return "Error calculating checksum"

def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <input_path>")
        sys.exit(1)
    
    input_path = sys.argv[1]
    if not os.path.exists(input_path):
        print(f"Error: {input_path} does not exist.")
        sys.exit(1)
    
    size = get_total_size(input_path)
    checksum = get_blake3(input_path)
    
    print(f"Path: {input_path}")
    print(f"Decompressed Size: {size} bytes ({size / (1024*1024):.2f} MiB)")
    print(f"BLAKE3 Checksum: {checksum}")

if __name__ == "__main__":
    main()
