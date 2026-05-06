#!/usr/bin/env python3
"""
Lint Output Filter and Reporter
Filters clang-tidy output to remove system header errors and generates
a structured, LLM-friendly report organized by file and issue type.

Usage:
  make lint 2>&1 | python3 scripts/lint_filter.py [--format json|md|text]
"""

import sys
import json
import re
from collections import defaultdict
from pathlib import Path


def parse_lint_line(line):
    """
    Parse a clang-tidy output line.
    Format: /path/to/file.cpp:123:45: error: message [check-name]
    Returns: (file, line, col, severity, message, check) or None
    """
    # Skip empty lines
    if not line.strip():
        return None

    # Match clang-tidy output format
    match = re.match(r'^([^:]+):(\d+):(\d+): (\w+): (.+?) \[([^\]]+)\]', line)
    if match:
        file_path, line_num, col, severity, message, check_name = match.groups()
        return {
            'file': file_path,
            'line': int(line_num),
            'col': int(col),
            'severity': severity,
            'message': message,
            'check': check_name,
        }
    return None


def is_system_header(file_path):
    """Check if a file is a system header (not in src/ or include/mar/)."""
    # System headers typically come from /opt, /usr, or Xcode paths
    if any(path in file_path for path in ['/opt/', '/usr/', 'Xcode', '/Library/', '.cache']):
        return True
    # Skip third-party dependencies that are bundled
    if 'xxhash3.h' in file_path:
        return True
    # Also check if it's NOT in src/ or include/mar/
    if 'src/' not in file_path and 'include/mar/' not in file_path:
        return True
    return False


def get_package_from_file(file_path):
    """Extract package/component name from file path."""
    # Example: src/index_registry.cpp -> index_registry
    # Example: include/mar/index.h -> index
    path = Path(file_path)
    stem = path.stem
    
    # Map common stems to packages
    packages = {
        'index': 'Indexing',
        'archive': 'Archive',
        'compression': 'Compression',
        'crypto': 'Cryptography',
        'util': 'Utilities',
        'cli': 'CLI',
        'format': 'Format',
        'span': 'Span Management',
        'block': 'Block Management',
    }
    
    for key, label in packages.items():
        if key in stem.lower():
            return label
    
    return 'Core' if 'main' in stem else 'General'


def group_issues(issues):
    """Group issues by file and check type."""
    by_file = defaultdict(list)
    by_check = defaultdict(list)
    by_package = defaultdict(lambda: defaultdict(list))
    
    for issue in issues:
        by_file[issue['file']].append(issue)
        by_check[issue['check']].append(issue)
        package = get_package_from_file(issue['file'])
        by_package[package][issue['check']].append(issue)
    
    return by_file, by_check, by_package


def format_text(issues):
    """Format as plain text with ASCII art."""
    if not issues:
        return "✓ No linting issues found!\n"
    
    by_file, by_check, by_package = group_issues(issues)
    
    output = []
    output.append("=" * 80)
    output.append("LINTING ISSUES REPORT")
    output.append("=" * 80)
    output.append(f"\nTotal Issues: {len(issues)}")
    output.append(f"Files Affected: {len(by_file)}")
    output.append(f"Check Types: {len(by_check)}\n")
    
    # Summary by package
    output.append("SUMMARY BY PACKAGE")
    output.append("-" * 80)
    for package in sorted(by_package.keys()):
        checks = by_package[package]
        count = sum(len(v) for v in checks.values())
        output.append(f"{package:.<40} {count:>5} issues")
        for check, check_issues in sorted(checks.items()):
            output.append(f"  • {check:.<35} {len(check_issues):>3}")
    
    # Detailed breakdown by file
    output.append("\n" + "=" * 80)
    output.append("DETAILED BREAKDOWN BY FILE")
    output.append("=" * 80)
    
    for file_path in sorted(by_file.keys()):
        file_issues = by_file[file_path]
        package = get_package_from_file(file_path)
        output.append(f"\n📄 {file_path}")
        output.append(f"   Package: {package} | Issues: {len(file_issues)}")
        output.append("   " + "-" * 76)
        
        for issue in sorted(file_issues, key=lambda x: (x['line'], x['col'])):
            output.append(f"   Line {issue['line']:>4}, Col {issue['col']:>2}: {issue['severity'].upper()}")
            output.append(f"   → {issue['check']}")
            output.append(f"      {issue['message']}\n")
    
    return "\n".join(output)


def format_json(issues):
    """Format as JSON."""
    by_file, by_check, by_package = group_issues(issues)
    
    return json.dumps({
        'summary': {
            'total_issues': len(issues),
            'files_affected': len(by_file),
            'check_types': len(by_check),
            'packages': len(by_package),
        },
        'by_package': {
            package: {
                check: [
                    {
                        'file': issue['file'],
                        'line': issue['line'],
                        'col': issue['col'],
                        'severity': issue['severity'],
                        'message': issue['message'],
                    }
                    for issue in issues_list
                ]
                for check, issues_list in sorted(checks.items())
            }
            for package, checks in sorted(by_package.items())
        },
        'by_file': {
            file_path: [
                {
                    'line': issue['line'],
                    'col': issue['col'],
                    'severity': issue['severity'],
                    'check': issue['check'],
                    'message': issue['message'],
                }
                for issue in sorted(issues_list, key=lambda x: (x['line'], x['col']))
            ]
            for file_path, issues_list in sorted(by_file.items())
        },
    }, indent=2)


def format_md(issues):
    """Format as Markdown."""
    if not issues:
        return "## Linting Report\n\n✓ **No issues found!**\n"
    
    by_file, by_check, by_package = group_issues(issues)
    
    output = []
    output.append("# Linting Issues Report")
    output.append(f"\n**Summary:** {len(issues)} issues across {len(by_file)} files")
    output.append(f"\n## Issues by Package\n")
    
    for package in sorted(by_package.keys()):
        checks = by_package[package]
        count = sum(len(v) for v in checks.values())
        output.append(f"### {package} ({count} issues)\n")
        
        for check in sorted(checks.keys()):
            check_issues = checks[check]
            output.append(f"#### {check} ({len(check_issues)})\n")
            for issue in sorted(check_issues, key=lambda x: (x['file'], x['line'])):
                output.append(f"- **{issue['file']}:{issue['line']}:{issue['col']}** ({issue['severity']})")
                output.append(f"  ```\n  {issue['message']}\n  ```\n")
    
    return "\n".join(output)


def main():
    fmt = 'text'
    
    # Parse arguments
    i = 1
    while i < len(sys.argv):
        arg = sys.argv[i]
        if arg == '--format' and i + 1 < len(sys.argv):
            fmt = sys.argv[i + 1].lower()
            i += 2
        elif arg.startswith('--format='):
            fmt = arg.split('=')[1].lower()
            i += 1
        elif arg in ['json', 'md', 'text']:
            fmt = arg.lower()
            i += 1
        else:
            i += 1
    
    if fmt not in ['json', 'md', 'text']:
        print(f"Unknown format: {fmt}. Use: json, md, or text", file=sys.stderr)
        sys.exit(1)
    
    # Read stdin
    issues = []
    for line in sys.stdin:
        parsed = parse_lint_line(line.strip())
        if parsed and not is_system_header(parsed['file']):
            issues.append(parsed)
    
    # Format and output
    if fmt == 'json':
        print(format_json(issues))
    elif fmt == 'md':
        print(format_md(issues))
    else:
        print(format_text(issues))
    
    # Exit with error if issues found
    sys.exit(1 if issues else 0)


if __name__ == '__main__':
    main()
