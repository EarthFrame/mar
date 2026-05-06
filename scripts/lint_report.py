#!/usr/bin/env python3
import sys
import re
import json
import argparse
from collections import defaultdict

def parse_lint_output(content):
    # Pattern to match clang-tidy errors/warnings
    # Example: /path/to/file.cpp:10:5: warning: check-name [check-name]
    pattern = re.compile(r'^(.*?):(\d+):(\d+): (warning|error): (.*?) \[(.*?)\]$', re.MULTILINE)
    
    report = {
        'summary': {
            'total_issues': 0,
            'by_type': defaultdict(int),
            'by_file': defaultdict(int)
        },
        'issues': []
    }
    
    for match in pattern.finditer(content):
        file_path, line, col, severity, message, check_name = match.groups()
        
        issue = {
            'file': file_path,
            'line': int(line),
            'column': int(col),
            'severity': severity,
            'message': message,
            'check': check_name
        }
        
        report['issues'].append(issue)
        report['summary']['total_issues'] += 1
        report['summary']['by_type'][check_name] += 1
        report['summary']['by_file'][file_path] += 1
        
    return report

def generate_markdown_report(report):
    md = "# 🛡️ Linting Analysis Report\n\n"
    md += f"**Total Issues Found:** {report['summary']['total_issues']}\n\n"
    
    md += "## 📊 Issues by Check Type\n\n"
    md += "| Check Name | Count |\n"
    md += "| :--- | :---: |\n"
    sorted_types = sorted(report['summary']['by_type'].items(), key=lambda x: x[1], reverse=True)
    for check, count in sorted_types:
        md += f"| `{check}` | {count} |\n"
    
    md += "\n## 📂 Issues by File\n\n"
    md += "| File Path | Count |\n"
    md += "| :--- | :---: |\n"
    sorted_files = sorted(report['summary']['by_file'].items(), key=lambda x: x[1], reverse=True)
    for file_path, count in sorted_files:
        md += f"| `{file_path}` | {count} |\n"
        
    md += "\n## 📝 Detailed Issues (Top 20)\n\n"
    for i, issue in enumerate(report['issues'][:20]):
        md += f"### {i+1}. `{issue['check']}`\n"
        md += f"- **File:** `{issue['file']}:{issue['line']}`\n"
        md += f"- **Severity:** {issue['severity'].upper()}\n"
        md += f"- **Message:** {issue['message']}\n\n"
        
    if len(report['issues']) > 20:
        md += f"*... and {len(report['issues']) - 20} more issues.*\n"
        
    return md

def generate_json_report(report):
    # Convert defaultdict to regular dict for JSON serialization
    report['summary']['by_type'] = dict(report['summary']['by_type'])
    report['summary']['by_file'] = dict(report['summary']['by_file'])
    return json.dumps(report, indent=2)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse clang-tidy output into a report.")
    parser.add_argument("--format", choices=["md", "json"], default="md", help="Output format (default: md)")
    args = parser.parse_args()
    
    if sys.stdin.isatty():
        print("Usage: make lint | python3 scripts/lint_report.py [--format json] > report")
        sys.exit(1)
        
    content = sys.stdin.read()
    report = parse_lint_output(content)
    
    if not report['issues']:
        if args.format == "md":
            print("✅ No linting issues found.")
        else:
            print(json.dumps({"issues": [], "summary": {"total_issues": 0}}, indent=2))
    else:
        if args.format == "md":
            print(generate_markdown_report(report))
        else:
            print(generate_json_report(report))
