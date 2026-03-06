#!/usr/bin/env python3
"""
MAR Benchmark Framework
Orchestrates benchmark execution, timing, and result collection.
"""

import yaml
import subprocess
import os
import sys
import csv
import shutil
import argparse
import tempfile
import uuid
import re
from pathlib import Path
from datetime import datetime

def run_command(cmd, capture_output=True):
    """Run a command and return its output."""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=capture_output, text=True, check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        if capture_output:
            print(f"Error running command '{cmd}':")
            print(f"  Stderr: {e.stderr}")
        return None

def get_dir_size(path):
    """Get total size of a directory or file in bytes."""
    path = Path(path)
    if not path.exists():
        return 0
    if path.is_file():
        return path.stat().st_size
    return sum(f.stat().st_size for f in path.glob('**/*') if f.is_file())

def main():
    parser = argparse.ArgumentParser(description="MAR Benchmark Framework")
    parser.add_argument("--config", type=str, help="Path to benchmark config YAML")
    parser.add_argument("--purge", action="store_true", help="Purge system cache before each benchmark")
    parser.add_argument("--no-plots", action="store_true", help="Skip plot generation")
    parser.add_argument("--keep-scratch", action="store_true", help="Keep scratch directory for debugging (default: cleanup)")
    args = parser.parse_args()

    print("=" * 70)
    print("MAR BENCHMARK FRAMEWORK")
    print("=" * 70)
    print()

    # Detect timeout command
    timeout_bin = ""
    for cmd in ["timeout", "gtimeout"]:
        if shutil.which(cmd):
            timeout_bin = cmd
            break

    # Detect time command (GNU time vs BSD time)
    # GNU time supports -p -o, BSD time does not
    time_bin = ""
    time_format = ""  # How to format the command
    
    # Try to use GNU time first (gtime on macOS if installed, or time with -p support)
    if shutil.which("gtime"):
        time_bin = "gtime"
        time_format = "gnu"  # Uses: gtime -p -o file cmd
    elif sys.platform == "darwin":
        # macOS has BSD time, need to use a different approach
        time_bin = "time"
        time_format = "bsd"  # Uses: (time cmd) > file 2>&1
    else:
        # Linux typically has GNU time
        time_bin = "time"
        time_format = "gnu"  # Uses: time -p -o file cmd

    # Setup paths
    script_dir = Path(__file__).parent.resolve()
    repo_root = script_dir.parent.parent
    
    # Resolve config path (handle relative paths from current working directory)
    if args.config:
        config_path = Path(args.config).resolve()  # Convert to absolute path from cwd
    else:
        config_path = repo_root / "benchmarks/configs/default/default_mar_benchmarks.yaml"
    
    if not config_path.exists():
        print(f"✗ Error: Config not found at {config_path}")
        sys.exit(1)
    
    os.chdir(repo_root)
    print(f"Repository root: {repo_root}")
    print(f"Config file: {config_path}")
    print()

    # Load config
    print("Loading configuration...")
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    print("✓ Configuration loaded")

    global_config = config.get('global', {})
    print(f"Cache purging: {'Enabled (requires sudo)' if args.purge else 'Disabled'}")
    print(f"Timeout command: {timeout_bin or 'Not available'}")
    print(f"Time command: {time_bin} ({time_format})")
    print()

    global_seed = global_config.get('seed', 42)
    profiling_enabled = global_config.get('profiling', False)
    global_timeout = global_config.get('timeout', 600)
    
    benchmarks = config.get('benchmarks', [])

    # Create unique scratch directory for this benchmark run
    run_id = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    scratch_dir = repo_root / "benchmarks" / f"scratch_{run_id}"
    scratch_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Scratch directory: {scratch_dir}")
    print(f"Run ID: {run_id}")
    print()

    # Validate inputs
    print(f"Validating {len(benchmarks)} benchmarks...")
    skipped = 0
    for bench in benchmarks:
        bench_id = bench['id']
        command_type = bench['command']
        input_path = bench.get('input', '')
        
        # Check inputs exist (except those in profile_output which are generated)
        if input_path and not input_path.startswith("profile_output/"):
            if not os.path.exists(input_path):
                print(f"  ✗ Benchmark '{bench_id}' input '{input_path}' not found. Failing fast.")
                sys.exit(1)
    
    print(f"✓ All {len(benchmarks)} benchmarks validated")
    print()
    
    # Generate shell script
    shell_script_path = Path("benchmarks/scripts/run_internal.sh")
    print(f"Generating shell script: {shell_script_path}")
    with open(shell_script_path, 'w') as sh:
        sh.write("#!/bin/bash\n")
        sh.write("# MAR Benchmark Execution Script\n")
        sh.write(f"# Generated with {len(benchmarks)} benchmarks\n")
        sh.write(f"# Run ID: {run_id}\n")
        sh.write(f"# Scratch dir: {scratch_dir}\n")
        sh.write("set -e\n\n")
        sh.write(f"SCRATCH_DIR='{scratch_dir}'\n")
        sh.write("mkdir -p $SCRATCH_DIR\n\n")
        sh.write("TOTAL_BENCHMARKS={}\n".format(len(benchmarks)))
        sh.write(f"TIME_CMD='{time_bin}'\n")
        sh.write(f"TIME_FORMAT='{time_format}'\n")
        sh.write("\n")
        
        for i, bench in enumerate(benchmarks, 1):
            bench_id = bench['id']
            full_cmd = bench['command']  # Full command line is now directly in config
            timeout_val = bench.get('timeout', global_timeout)
            output_file = bench.get('output_file', '')  # Optional: file to measure size
            cleanup = bench.get('cleanup', '')  # Optional: path to cleanup
            
            sh.write(f"echo \"[{i}/{len(benchmarks)}] Running {bench_id}...\"\n")
            
            # Purge cache if requested
            if args.purge:
                if sys.platform == "darwin":
                    sh.write("echo '  [Cache] Purging system cache...'\n")
                    sh.write("sudo purge > /dev/null 2>&1\n")
                elif sys.platform.startswith("linux"):
                    sh.write("echo '  [Cache] Dropping page cache...'\n")
                    sh.write("sync && echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null 2>&1\n")

            time_log = f"$SCRATCH_DIR/{bench_id}.time"
            
            # Expand tilde in command
            full_cmd_expanded = full_cmd.replace('~/', f"{os.path.expanduser('~')}/")
            
            # Check if multiline command
            is_multiline = '\n' in full_cmd_expanded
            
            # Execute with timing
            if is_multiline:
                # Multiline: wrap in a subshell with timing
                sh.write(f"# Multiline command block\n")
                sh.write(f"{{ time {{\n")
                sh.write(f"{full_cmd_expanded}\n")
                sh.write(f"}}; }} 2> {time_log}\n")
            else:
                # Single line: wrap with timing
                sh.write(f"{{ time {full_cmd_expanded}; }} 2> {time_log}\n")
            
            sh.write(f"EXIT_CODE=$?\n")
            sh.write(f"if [ $EXIT_CODE -ne 0 ]; then\n")
            sh.write(f"  echo '  ✗ Benchmark failed or timed out (>{timeout_val}s)'\n")
            sh.write(f"  exit 1\n")
            sh.write(f"fi\n")

            # Collect output size before cleanup (for compression metrics)
            if output_file:
                size_log = f"$SCRATCH_DIR/{bench_id}.size"
                sh.write(f"# Save output size for metrics\n")
                # Use stat for bytes (works on both macOS and Linux)
                sh.write(f"(stat -f%z {output_file} 2>/dev/null || stat -c%s {output_file} 2>/dev/null) > {size_log}\n")

            # Clean up if requested
            if cleanup:
                sh.write(f"rm -rf {cleanup}\n")

            sh.write("\n")

    shell_script_path.chmod(0o755)

    # Log what we're running
    print(f"✓ Shell script generated with {len(benchmarks)} commands\n")
    print("=" * 70)
    print("BENCHMARK EXECUTION ORDER")
    print("=" * 70)

    print()
    for i, bench in enumerate(benchmarks, 1):
        cmd = bench['command']
        workload = bench.get('workload', 'N/A')
        print(f"  {i:2d}. [{cmd:7s}] {bench['id']:30s} ({workload})")
    print()
    print("=" * 70)
    print(f"Total: {len(benchmarks)} benchmarks")
    print("=" * 70)
    print()

    # Run the shell script
    print("Starting benchmark execution...")
    print()
    try:
        subprocess.run(["bash", str(shell_script_path)], check=True)
    except subprocess.CalledProcessError:
        print("\n✗ Benchmarks failed or were interrupted.")
        sys.exit(1)

    print()
    print("=" * 70)
    print("✓ BENCHMARKS COMPLETED SUCCESSFULLY")
    print("=" * 70)
    print()

    # Collect results
    print("Collecting results...")
    output_csv = "benchmarks/results.csv"
    with open(output_csv, 'w', newline='') as csvfile:
        fieldnames = ['benchmark_id', 'workload', 'operation', 'variant', 'metric', 'value', 'units']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for bench in benchmarks:
            bench_id = bench['id']
            workload = bench.get('workload', 'unknown')
            operation = bench.get('operation', 'unknown')
            variant = bench.get('variant', 'unknown')
            
            # Parse timing from scratch directory
            time_log = scratch_dir / f"{bench_id}.time"
            if time_log.exists():
                with open(time_log, 'r') as tf:
                    content = tf.read()
                    # Try to parse both GNU time format (lines with "real/user/sys metric") 
                    # and BSD time format (all output on stderr)
                    
                    timing_found = False
                    
                    # GNU time format: "real 1.23" on separate lines
                    for line in content.split('\n'):
                        if line.strip():
                            parts = line.split()
                            if len(parts) >= 2:
                                metric = parts[0]
                                value_str = parts[1]
                                
                                # Try parsing as decimal (GNU format)
                                try:
                                    value = float(value_str)
                                    if metric in ['real', 'user', 'sys']:
                                        writer.writerow({
                                            'benchmark_id': bench_id,
                                            'workload': workload,
                                            'operation': operation,
                                            'variant': variant,
                                            'metric': f"time_{metric}",
                                            'value': value,
                                            'units': 'seconds'
                                        })
                                        timing_found = True
                                except ValueError:
                                    pass
                    
                    # If GNU format didn't work, try BSD format
                    if not timing_found and 'real' in content:
                        # BSD time output format: "real\t0m11.221s" (keyword then Mm SS.MILLIs format)
                        # Pattern: keyword, then optional whitespace/tabs, then Mm SS.MILLIs
                        real_match = re.search(r'real[\s\t]+(\d+)m([\d.]+)s', content)
                        user_match = re.search(r'user[\s\t]+(\d+)m([\d.]+)s', content)
                        sys_match = re.search(r'sys[\s\t]+(\d+)m([\d.]+)s', content)
                        
                        def convert_time(match):
                            """Convert M m SS.MILLIs to total seconds"""
                            if not match:
                                return None
                            minutes = int(match.group(1))
                            seconds = float(match.group(2))
                            return minutes * 60 + seconds
                        
                        if real_match:
                            writer.writerow({
                                'benchmark_id': bench_id,
                                'workload': workload,
                                'operation': operation,
                                'variant': variant,
                                'metric': 'time_real',
                                'value': convert_time(real_match),
                                'units': 'seconds'
                            })
                        if user_match:
                            writer.writerow({
                                'benchmark_id': bench_id,
                                'workload': workload,
                                'operation': operation,
                                'variant': variant,
                                'metric': 'time_user',
                                'value': convert_time(user_match),
                                'units': 'seconds'
                            })
                        if sys_match:
                            writer.writerow({
                                'benchmark_id': bench_id,
                                'workload': workload,
                                'operation': operation,
                                'variant': variant,
                                'metric': 'time_sys',
                                'value': convert_time(sys_match),
                                'units': 'seconds'
                            })

            # Collect size/compression metrics for create operations
            if operation == 'create' and output_file:
                # Try to read output size from size file (saved during execution)
                size_log = scratch_dir / f"{bench_id}.size"
                if size_log.exists():
                    try:
                        with open(size_log, 'r') as sf:
                            out_size = int(sf.read().strip())
                        
                        # For numpy workload, we know the input
                        # Extract input path from command (after last argument)
                        cmd_parts = full_cmd.split()
                        input_dir = None
                        for part in reversed(cmd_parts):
                            if 'benchmarks/data/' in part:
                                input_dir = part.rstrip('/')
                                break
                        
                        if input_dir and os.path.exists(input_dir):
                            in_size = get_dir_size(input_dir)
                            if in_size > 0 and out_size > 0:
                                writer.writerow({
                                    'benchmark_id': bench_id,
                                    'workload': workload,
                                    'operation': operation,
                                    'variant': variant,
                                    'metric': 'compression_ratio',
                                    'value': float(in_size) / float(out_size),
                                    'units': 'ratio'
                                })
                                writer.writerow({
                                    'benchmark_id': bench_id,
                                    'workload': workload,
                                    'operation': operation,
                                    'variant': variant,
                                    'metric': 'output_size_bytes',
                                    'value': float(out_size),
                                    'units': 'bytes'
                                })
                    except (ValueError, IOError):
                        pass

    print(f"✓ Results written to {output_csv}")
    print(f"  - Total rows: {sum(1 for _ in open(output_csv)) - 1}")  # -1 for header
    print()
    
    # Generate plots
    if not args.no_plots:
        r_plot_script = repo_root / "benchmarks/scripts/plot_results.R"
        if r_plot_script.exists():
            print("Generating plots...")
            print(f"  Running: Rscript {r_plot_script}")
            result = subprocess.run(["Rscript", str(r_plot_script), output_csv, "benchmarks/visuals"], 
                                  capture_output=True, text=True)
            if result.returncode == 0:
                # Count generated plots
                import glob
                plots = glob.glob("benchmarks/visuals/*.png")
                print(f"✓ Plots generated: {len(plots)} files in benchmarks/visuals/")
                if plots:
                    for plot in sorted(plots)[:5]:  # Show first 5
                        print(f"    - {Path(plot).name}")
                    if len(plots) > 5:
                        print(f"    ... and {len(plots) - 5} more")
            else:
                print(f"⚠ Plot generation failed (R error)")
                if result.stderr:
                    print(f"  Error: {result.stderr[:200]}")
        else:
            print("⚠ Plot script not found at {r_plot_script}")
    print()

    # Final summary
    print("=" * 70)
    print("BENCHMARK SUMMARY")
    print("=" * 70)
    print(f"Results CSV: benchmarks/results.csv")
    print(f"Plots dir:   benchmarks/visuals/")
    print(f"Script log:  benchmarks/scripts/run_internal.sh")
    print(f"Run ID: {run_id}")
    print(f"Scratch dir: {scratch_dir}")
    print("=" * 70)
    print()

    # Cleanup scratch directory
    if not args.keep_scratch:
        print("Cleaning up scratch directory...")
        try:
            shutil.rmtree(scratch_dir)
            print(f"✓ Scratch directory removed: {scratch_dir}")
        except Exception as e:
            print(f"⚠ Warning: Could not remove scratch directory: {e}")
    else:
        print(f"ℹ Scratch directory preserved (--keep-scratch): {scratch_dir}")
    print()

if __name__ == "__main__":
    main()
