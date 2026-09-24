use crate::format::NameTableFormat;

pub fn common_prefix_len(a: &str, b: &str) -> usize {
    a.bytes()
        .zip(b.bytes())
        .take_while(|(x, y)| x == y)
        .count()
}

pub fn recommend_format(names: &[String]) -> NameTableFormat {
    if names.len() < 100 {
        return NameTableFormat::RawArray;
    }
    let is_sorted = names.windows(2).all(|w| w[0] <= w[1]);
    if !is_sorted {
        return NameTableFormat::RawArray;
    }

    let mut raw_size = 4;
    for n in names {
        raw_size += 4 + n.len();
    }

    let mut front_coded_size = 8;
    let mut prev = "";
    for (i, name) in names.iter().enumerate() {
        if i % 16 == 0 {
            front_coded_size += 4 + name.len();
        } else {
            let prefix = common_prefix_len(prev, name);
            front_coded_size += 4 + (name.len() - prefix);
        }
        prev = name;
    }

    if (front_coded_size as f64) <= (raw_size as f64) * 0.9 {
        NameTableFormat::FrontCoded
    } else {
        NameTableFormat::RawArray
    }
}

pub fn read_name_table(data: &[u8], format: NameTableFormat) -> Result<Vec<String>, String> {
    match format {
        NameTableFormat::RawArray => {
            if data.len() < 4 {
                return Err("NAME_TABLE too short".to_string());
            }
            let name_count = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
            let mut pos = 4;
            let mut names = Vec::with_capacity(name_count);
            for _ in 0..name_count {
                if pos + 4 > data.len() {
                    return Err("NAME_TABLE truncated".to_string());
                }
                let len = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
                pos += 4;
                if pos + len > data.len() {
                    return Err("NAME_TABLE name truncated".to_string());
                }
                let s = std::str::from_utf8(&data[pos..pos + len])
                    .map_err(|e| format!("Invalid UTF-8 in NAME_TABLE: {}", e))?;
                names.push(s.to_string());
                pos += len;
            }
            Ok(names)
        }
        NameTableFormat::FrontCoded => {
            if data.len() < 8 {
                return Err("FRONT_CODED NAME_TABLE too short".to_string());
            }
            let name_count = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
            let reset_interval = u32::from_le_bytes(data[4..8].try_into().unwrap()) as usize;
            let mut pos = 8;
            let mut names = Vec::with_capacity(name_count);
            let mut prev = String::new();

            for i in 0..name_count {
                if pos + 4 > data.len() {
                    return Err("FRONT_CODED truncated".to_string());
                }
                let prefix_len = u16::from_le_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
                let suffix_len = u16::from_le_bytes(data[pos + 2..pos + 4].try_into().unwrap()) as usize;
                pos += 4;

                if pos + suffix_len > data.len() {
                    return Err("FRONT_CODED suffix truncated".to_string());
                }

                if i % reset_interval == 0 && prefix_len != 0 {
                    return Err("FRONT_CODED reset interval violation".to_string());
                }

                let mut name = if prefix_len > 0 && prefix_len <= prev.len() {
                    prev[..prefix_len].to_string()
                } else {
                    String::new()
                };

                let suffix = std::str::from_utf8(&data[pos..pos + suffix_len])
                    .map_err(|e| format!("Invalid UTF-8 in suffix: {}", e))?;
                name.push_str(suffix);
                pos += suffix_len;

                prev = name.clone();
                names.push(name);
            }
            Ok(names)
        }
        NameTableFormat::CompactTrie => {
            if data.len() < 4 {
                return Err("COMPACT_TRIE too short".to_string());
            }
            if data[0] != 0x03 {
                return Err("Unknown COMPACT_TRIE version".to_string());
            }
            if data[1] != 0x00 {
                return Err("COMPACT_TRIE flags must be 0".to_string());
            }
            let mut pos = 2;
            let trie_data_size = read_varint(data, &mut pos)? as usize;
            if pos + trie_data_size > data.len() {
                return Err("COMPACT_TRIE data truncated".to_string());
            }
            let trie_end = pos + trie_data_size;
            let mut trie_pos = pos;
            let root = deserialize_trie_node(&data[..trie_end], &mut trie_pos)?;

            pos = trie_end;
            let name_count = read_varint(data, &mut pos)? as usize;
            let mut names = vec![String::new(); name_count];
            collect_trie_names(&root, "", &mut names);
            Ok(names)
        }
    }
}

struct TrieNode {
    file_index: Option<u32>,
    children: Vec<(String, TrieNode)>,
}

fn write_varint(out: &mut Vec<u8>, mut value: u32) {
    while value >= 0x80 {
        out.push(((value & 0x7F) | 0x80) as u8);
        value >>= 7;
    }
    out.push(value as u8);
}

fn read_varint(data: &[u8], pos: &mut usize) -> Result<u32, String> {
    let mut result: u32 = 0;
    let mut shift = 0;
    while *pos < data.len() {
        let byte = data[*pos];
        *pos += 1;
        result |= ((byte & 0x7F) as u32) << shift;
        if (byte & 0x80) == 0 {
            return Ok(result);
        }
        shift += 7;
        if shift > 28 {
            return Err("Varint too large".to_string());
        }
    }
    Err("Varint truncated".to_string())
}

fn serialize_trie_node(node: &TrieNode, out: &mut Vec<u8>) {
    let mut header = 0u8;
    if node.file_index.is_some() {
        header |= 0x01;
    }
    if !node.children.is_empty() {
        header |= 0x02;
    }

    let mut max_label_len = 0usize;
    for (label, _) in &node.children {
        max_label_len = max_label_len.max(label.len());
    }

    let label_encoding: u8 = if max_label_len <= 0xFF && max_label_len > 0 {
        1
    } else if max_label_len <= 0xFFFF && max_label_len > 0xFF {
        2
    } else {
        0
    };
    header |= label_encoding << 2;
    out.push(header);

    if let Some(idx) = node.file_index {
        write_varint(out, idx);
    }

    if !node.children.is_empty() {
        write_varint(out, node.children.len() as u32);
        for (label, child) in &node.children {
            match label_encoding {
                1 => out.push(label.len() as u8),
                2 => {
                    out.push((label.len() & 0xFF) as u8);
                    out.push(((label.len() >> 8) & 0xFF) as u8);
                }
                _ => write_varint(out, label.len() as u32),
            }
            out.extend_from_slice(label.as_bytes());
            serialize_trie_node(child, out);
        }
    }
}

fn deserialize_trie_node(data: &[u8], pos: &mut usize) -> Result<TrieNode, String> {
    if *pos >= data.len() {
        return Err("COMPACT_TRIE node truncated".to_string());
    }
    let header = data[*pos];
    *pos += 1;

    let is_leaf = (header & 0x01) != 0;
    let has_children = (header & 0x02) != 0;
    let label_encoding = (header >> 2) & 0x03;

    let file_index = if is_leaf {
        Some(read_varint(data, pos)?)
    } else {
        None
    };

    let mut children = Vec::new();
    if has_children {
        let child_count = read_varint(data, pos)? as usize;
        for _ in 0..child_count {
            let label_len = match label_encoding {
                1 => {
                    if *pos >= data.len() { return Err("Label length truncated".to_string()); }
                    let l = data[*pos] as usize;
                    *pos += 1;
                    l
                }
                2 => {
                    if *pos + 2 > data.len() { return Err("Label length truncated".to_string()); }
                    let l = (data[*pos] as usize) | ((data[*pos + 1] as usize) << 8);
                    *pos += 2;
                    l
                }
                _ => read_varint(data, pos)? as usize,
            };

            if *pos + label_len > data.len() {
                return Err("Edge label truncated".to_string());
            }
            let label = std::str::from_utf8(&data[*pos..*pos + label_len])
                .map_err(|e| format!("Invalid UTF-8 in label: {}", e))?
                .to_string();
            *pos += label_len;

            let child = deserialize_trie_node(data, pos)?;
            children.push((label, child));
        }
    }

    Ok(TrieNode { file_index, children })
}

fn collect_trie_names(node: &TrieNode, prefix: &str, out: &mut [String]) {
    if let Some(idx) = node.file_index {
        if (idx as usize) < out.len() {
            out[idx as usize] = prefix.to_string();
        }
    }
    for (label, child) in &node.children {
        let child_path = if prefix.is_empty() {
            label.clone()
        } else {
            format!("{}/{}", prefix, label)
        };
        collect_trie_names(child, &child_path, out);
    }
}

fn build_trie(names: &[String]) -> TrieNode {
    let mut root = TrieNode { file_index: None, children: Vec::new() };
    for (idx, path) in names.iter().enumerate() {
        let mut curr = &mut root;
        let mut start = 0;
        let mut any_segments = false;
        let bytes = path.as_bytes();
        for i in 0..=bytes.len() {
            if i == bytes.len() || bytes[i] == b'/' {
                if i > start {
                    let segment = &path[start..i];
                    any_segments = true;

                    let found_idx = curr.children.iter().rposition(|(s, _)| s == segment);
                    let next_idx = match found_idx {
                        Some(fidx) => fidx,
                        None => {
                            curr.children.push((segment.to_string(), TrieNode { file_index: None, children: Vec::new() }));
                            curr.children.len() - 1
                        }
                    };
                    curr = &mut curr.children[next_idx].1;
                }
                start = i + 1;
            }
        }
        if !any_segments {
            curr.children.push((String::new(), TrieNode { file_index: None, children: Vec::new() }));
            let last = curr.children.len() - 1;
            curr = &mut curr.children[last].1;
        }
        curr.file_index = Some(idx as u32);
    }
    root
}

pub fn write_name_table(names: &[String], format: NameTableFormat) -> Vec<u8> {
    match format {
        NameTableFormat::RawArray => {
            let mut out = Vec::new();
            out.extend_from_slice(&(names.len() as u32).to_le_bytes());
            for name in names {
                out.extend_from_slice(&(name.len() as u32).to_le_bytes());
                out.extend_from_slice(name.as_bytes());
            }
            out
        }
        NameTableFormat::FrontCoded => {
            let reset_interval = 16u32;
            let mut out = Vec::new();
            out.extend_from_slice(&(names.len() as u32).to_le_bytes());
            out.extend_from_slice(&reset_interval.to_le_bytes());

            let mut prev = "";
            for (i, name) in names.iter().enumerate() {
                let prefix_len = if i % (reset_interval as usize) != 0 {
                    common_prefix_len(prev, name)
                } else {
                    0
                };
                let suffix = &name[prefix_len..];
                out.extend_from_slice(&(prefix_len as u16).to_le_bytes());
                out.extend_from_slice(&(suffix.len() as u16).to_le_bytes());
                out.extend_from_slice(suffix.as_bytes());
                prev = name;
            }
            out
        }
        NameTableFormat::CompactTrie => {
            let mut out = Vec::new();
            out.push(0x03); // trie_version
            out.push(0x00); // flags

            let root = build_trie(names);
            let mut trie_data = Vec::new();
            serialize_trie_node(&root, &mut trie_data);

            write_varint(&mut out, trie_data.len() as u32);
            out.extend_from_slice(&trie_data);
            write_varint(&mut out, names.len() as u32);
            out
        }
    }
}
