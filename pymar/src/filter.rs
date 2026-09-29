use std::fs::File;
use std::io::{self, BufRead, BufReader};

/// Check if a text matches a glob pattern.
/// Supports:
/// - `*`: matches zero or more characters (except '/')
/// - `**`: matches zero or more characters (including '/')
/// - `?`: matches any single character (except '/')
/// - `[...]`: character classes and ranges, e.g. `[0-9]`, `[a-z]`, `[!abc]`, `[^abc]`
/// - Basename matching: if the pattern does not contain '/', it matches against either
///   the full relative path or just the filename basename.
pub fn glob_match(pattern: &str, text: &str) -> bool {
    // If pattern doesn't contain '/', check basename first
    if !pattern.contains('/') {
        let basename = match text.rfind('/') {
            Some(idx) => &text[idx + 1..],
            None => text,
        };
        if glob_match_inner(pattern.as_bytes(), basename.as_bytes()) {
            return true;
        }
    }

    glob_match_inner(pattern.as_bytes(), text.as_bytes())
}

fn glob_match_inner(p: &[u8], t: &[u8]) -> bool {
    let mut pi = 0;
    let mut ti = 0;

    while pi < p.len() {
        if p[pi..].starts_with(b"**") {
            // '**' can match any characters including '/'
            let mut next_p = pi + 2;
            if next_p < p.len() && p[next_p] == b'/' {
                next_p += 1;
            }
            if next_p == p.len() {
                // Trailing '**' matches all remaining characters
                return true;
            }
            // Try matching remainder of pattern starting at every possible position in t
            for next_t in ti..=t.len() {
                if glob_match_inner(&p[next_p..], &t[next_t..]) {
                    return true;
                }
            }
            return false;
        } else if p[pi] == b'*' {
            // Single '*' matches zero or more non-slash characters
            pi += 1;
            // Compress consecutive single stars
            while pi < p.len() && p[pi] == b'*' {
                pi += 1;
            }
            if pi == p.len() {
                // Must not contain '/' in remainder of t
                return !t[ti..].contains(&b'/');
            }
            for next_t in ti..=t.len() {
                if next_t > ti && t[next_t - 1] == b'/' {
                    // Cannot match past '/'
                    break;
                }
                if glob_match_inner(&p[pi..], &t[next_t..]) {
                    return true;
                }
            }
            return false;
        } else if p[pi] == b'?' {
            if ti >= t.len() || t[ti] == b'/' {
                return false;
            }
            pi += 1;
            ti += 1;
        } else if p[pi] == b'[' {
            if ti >= t.len() || t[ti] == b'/' {
                return false;
            }
            // Parse character class
            let mut end = pi + 1;
            if end < p.len() && (p[end] == b'!' || p[end] == b'^') {
                end += 1;
            }
            if end < p.len() && p[end] == b']' {
                end += 1; // Literal ']' at beginning of class
            }
            while end < p.len() && p[end] != b']' {
                end += 1;
            }
            if end >= p.len() {
                // Malformed class without closing ']', treat '[' as literal
                if p[pi] != t[ti] {
                    return false;
                }
                pi += 1;
                ti += 1;
            } else {
                let class_content = &p[pi + 1..end];
                if !match_char_class(class_content, t[ti]) {
                    return false;
                }
                pi = end + 1;
                ti += 1;
            }
        } else {
            if ti >= t.len() || p[pi] != t[ti] {
                return false;
            }
            pi += 1;
            ti += 1;
        }
    }

    ti == t.len()
}

fn match_char_class(class: &[u8], ch: u8) -> bool {
    if class.is_empty() {
        return false;
    }
    let (negated, spec) = if class[0] == b'!' || class[0] == b'^' {
        (true, &class[1..])
    } else {
        (false, class)
    };

    let mut matched = false;
    let mut i = 0;
    while i < spec.len() {
        if i + 2 < spec.len() && spec[i + 1] == b'-' {
            let start = spec[i];
            let end = spec[i + 2];
            if ch >= start && ch <= end {
                matched = true;
                break;
            }
            i += 3;
        } else {
            if ch == spec[i] {
                matched = true;
                break;
            }
            i += 1;
        }
    }

    if negated {
        !matched
    } else {
        matched
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilterRule {
    Include(String),
    Exclude(String),
}

#[derive(Debug, Clone, Default)]
pub struct AlgebraicFilter {
    rules: Vec<FilterRule>,
    has_includes: bool,
}

impl AlgebraicFilter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_include(&mut self, pattern: &str) {
        let pat = pattern.trim();
        if !pat.is_empty() {
            self.rules.push(FilterRule::Include(pat.to_string()));
            self.has_includes = true;
        }
    }

    pub fn add_exclude(&mut self, pattern: &str) {
        let pat = pattern.trim();
        if !pat.is_empty() {
            self.rules.push(FilterRule::Exclude(pat.to_string()));
        }
    }

    pub fn load_includes_from_file(&mut self, file_path: &str) -> Result<usize, String> {
        let mut count = 0;
        let reader: Box<dyn BufRead> = if file_path == "-" {
            Box::new(BufReader::new(io::stdin()))
        } else {
            let f = File::open(file_path).map_err(|e| format!("Failed to open file {}: {}", file_path, e))?;
            Box::new(BufReader::new(f))
        };

        for line in reader.lines() {
            let line = line.map_err(|e| format!("Failed to read file list: {}", e))?;
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            self.add_include(trimmed);
            count += 1;
        }
        Ok(count)
    }

    pub fn load_excludes_from_file(&mut self, file_path: &str) -> Result<usize, String> {
        let mut count = 0;
        let reader: Box<dyn BufRead> = if file_path == "-" {
            Box::new(BufReader::new(io::stdin()))
        } else {
            let f = File::open(file_path).map_err(|e| format!("Failed to open exclude file {}: {}", file_path, e))?;
            Box::new(BufReader::new(f))
        };

        for line in reader.lines() {
            let line = line.map_err(|e| format!("Failed to read exclude list: {}", e))?;
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            self.add_exclude(trimmed);
            count += 1;
        }
        Ok(count)
    }

    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    pub fn has_includes(&self) -> bool {
        self.has_includes
    }

    /// Evaluates whether a given relative path passes the algebraic filter.
    /// Rules are evaluated sequentially:
    /// - If any rule matches, the last matching rule's decision applies.
    /// - If no rule matches:
    ///   - If there are include rules present, default is exclude (false).
    ///   - If only exclude rules are present (or no rules), default is include (true).
    pub fn matches(&self, path: &str) -> bool {
        if self.rules.is_empty() {
            return true;
        }

        let mut matched_status = None;
        for rule in &self.rules {
            match rule {
                FilterRule::Include(pattern) => {
                    if glob_match(pattern, path) {
                        matched_status = Some(true);
                    }
                }
                FilterRule::Exclude(pattern) => {
                    if glob_match(pattern, path) {
                        matched_status = Some(false);
                    }
                }
            }
        }

        match matched_status {
            Some(status) => status,
            None => !self.has_includes,
        }
    }

    /// Filter a slice of names, returning a vector of (original_index, name) pairs that match.
    pub fn filter_names(&self, names: &[String]) -> Vec<(usize, String)> {
        names
            .iter()
            .enumerate()
            .filter(|(_, name)| self.matches(name))
            .map(|(idx, name)| (idx, name.clone()))
            .collect()
    }
}
