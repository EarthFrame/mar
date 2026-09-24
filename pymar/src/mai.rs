use crate::checksum::XXHash3_64;
use crate::reader::MarReader;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

pub const MAI_MAGIC: u32 = 0x4D414900; // "MAI\0"
pub const MAI_VERSION: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MAIIndexType {
    Vector = 1,
    MinHash = 2,
    Generic = 3,
    Genomic = 4,
    Email = 5,
    TimeSeries = 6,
    BM25 = 7,
}

impl MAIIndexType {
    pub fn from_u8(val: u8) -> Option<Self> {
        match val {
            1 => Some(MAIIndexType::Vector),
            2 => Some(MAIIndexType::MinHash),
            3 => Some(MAIIndexType::Generic),
            4 => Some(MAIIndexType::Genomic),
            5 => Some(MAIIndexType::Email),
            6 => Some(MAIIndexType::TimeSeries),
            7 => Some(MAIIndexType::BM25),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MAIFixedHeader {
    pub magic: u32,
    pub version: u8,
    pub index_type: u8,
    pub align_log2: u8,
    pub reserved0: u8,
    pub archive_hash: u64,
    pub archive_name_len: u32,
    pub flags: u32,
    pub timestamp: u64,
    pub index_data_offset: u64,
    pub padding: [u8; 24],
}

impl Default for MAIFixedHeader {
    fn default() -> Self {
        Self {
            magic: MAI_MAGIC,
            version: MAI_VERSION,
            index_type: 0,
            align_log2: 0,
            reserved0: 0,
            archive_hash: 0,
            archive_name_len: 0,
            flags: 0,
            timestamp: 0,
            index_data_offset: 0,
            padding: [0u8; 24],
        }
    }
}

impl MAIFixedHeader {
    pub fn read(data: &[u8]) -> Result<Self, String> {
        if data.len() < 64 {
            return Err("MAIFixedHeader too short".to_string());
        }
        let magic = u32::from_le_bytes(data[0..4].try_into().unwrap());
        if magic != MAI_MAGIC {
            return Err("Invalid MAI magic".to_string());
        }
        let mut padding = [0u8; 24];
        padding.copy_from_slice(&data[40..64]);

        Ok(Self {
            magic,
            version: data[4],
            index_type: data[5],
            align_log2: data[6],
            reserved0: data[7],
            archive_hash: u64::from_le_bytes(data[8..16].try_into().unwrap()),
            archive_name_len: u32::from_le_bytes(data[16..20].try_into().unwrap()),
            flags: u32::from_le_bytes(data[20..24].try_into().unwrap()),
            timestamp: u64::from_le_bytes(data[24..32].try_into().unwrap()),
            index_data_offset: u64::from_le_bytes(data[32..40].try_into().unwrap()),
            padding,
        })
    }

    pub fn write(&self, buf: &mut [u8]) {
        assert!(buf.len() >= 64);
        buf[0..4].copy_from_slice(&self.magic.to_le_bytes());
        buf[4] = self.version;
        buf[5] = self.index_type;
        buf[6] = self.align_log2;
        buf[7] = self.reserved0;
        buf[8..16].copy_from_slice(&self.archive_hash.to_le_bytes());
        buf[16..20].copy_from_slice(&self.archive_name_len.to_le_bytes());
        buf[20..24].copy_from_slice(&self.flags.to_le_bytes());
        buf[24..32].copy_from_slice(&self.timestamp.to_le_bytes());
        buf[32..40].copy_from_slice(&self.index_data_offset.to_le_bytes());
        buf[40..64].copy_from_slice(&self.padding);
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MAISection {
    pub section_type: u32,
    pub flags: u32,
    pub offset: u64,
    pub size: u64,
}

impl MAISection {
    pub fn read(data: &[u8]) -> Self {
        Self {
            section_type: u32::from_le_bytes(data[0..4].try_into().unwrap()),
            flags: u32::from_le_bytes(data[4..8].try_into().unwrap()),
            offset: u64::from_le_bytes(data[8..16].try_into().unwrap()),
            size: u64::from_le_bytes(data[16..24].try_into().unwrap()),
        }
    }

    pub fn write(&self, buf: &mut [u8]) {
        assert!(buf.len() >= 24);
        buf[0..4].copy_from_slice(&self.section_type.to_le_bytes());
        buf[4..8].copy_from_slice(&self.flags.to_le_bytes());
        buf[8..16].copy_from_slice(&self.offset.to_le_bytes());
        buf[16..24].copy_from_slice(&self.size.to_le_bytes());
    }
}

pub struct MAIWriter {
    archive_name: String,
    header: MAIFixedHeader,
    sections: Vec<(MAISection, Vec<u8>)>,
}

impl MAIWriter {
    pub fn new(archive_path: &str, index_type: MAIIndexType, archive_hash: u64) -> Self {
        let p = Path::new(archive_path);
        let name = p.file_name().map(|n| n.to_string_lossy().to_string()).unwrap_or_else(|| archive_path.to_string());
        let ts = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);

        let mut header = MAIFixedHeader::default();
        header.index_type = index_type as u8;
        header.archive_hash = archive_hash;
        header.timestamp = ts;
        header.archive_name_len = name.len() as u32;

        Self {
            archive_name: name,
            header,
            sections: Vec::new(),
        }
    }

    pub fn add_section(&mut self, section_type: u32, data: Vec<u8>, flags: u32) {
        let sec = MAISection {
            section_type,
            flags,
            offset: 0,
            size: data.len() as u64,
        };
        self.sections.push((sec, data));
    }

    pub fn write_to_file(&mut self, path: &str, align_log2: u8) -> Result<(), String> {
        let mut file = File::create(path).map_err(|e| format!("Failed to create {}: {}", path, e))?;
        self.header.align_log2 = align_log2;
        let alignment = if align_log2 > 0 { 1u64 << align_log2 } else { 1 };

        let mut hdr_buf = [0u8; 64];
        self.header.write(&mut hdr_buf);
        file.write_all(&hdr_buf).map_err(|e| e.to_string())?;

        file.write_all(self.archive_name.as_bytes()).map_err(|e| e.to_string())?;

        let sec_count = self.sections.len() as u32;
        file.write_all(&sec_count.to_le_bytes()).map_err(|e| e.to_string())?;

        let dir_pos = 64 + self.archive_name.len() as u64 + 4;
        let dir_buf = vec![0u8; self.sections.len() * 24];
        file.write_all(&dir_buf).map_err(|e| e.to_string())?;

        let mut current_offset = dir_pos + dir_buf.len() as u64;

        for pair in &mut self.sections {
            if align_log2 > 0 {
                let padding = (alignment - (current_offset % alignment)) % alignment;
                if padding > 0 {
                    let pad = vec![0u8; padding as usize];
                    file.write_all(&pad).map_err(|e| e.to_string())?;
                    current_offset += padding;
                }
            }
            pair.0.offset = current_offset;
            file.write_all(&pair.1).map_err(|e| e.to_string())?;
            current_offset += pair.1.len() as u64;
        }

        self.header.index_data_offset = self.sections.first().map(|s| s.0.offset).unwrap_or(0);
        file.seek(SeekFrom::Start(0)).map_err(|e| e.to_string())?;
        self.header.write(&mut hdr_buf);
        file.write_all(&hdr_buf).map_err(|e| e.to_string())?;

        file.seek(SeekFrom::Start(dir_pos)).map_err(|e| e.to_string())?;
        for pair in &self.sections {
            let mut sbuf = [0u8; 24];
            pair.0.write(&mut sbuf);
            file.write_all(&sbuf).map_err(|e| e.to_string())?;
        }

        file.flush().map_err(|e| e.to_string())?;
        Ok(())
    }
}

pub struct MAIReader {
    header: MAIFixedHeader,
    archive_name: String,
    sections: HashMap<u32, MAISection>,
    data: Vec<u8>,
}

impl MAIReader {
    pub fn open(path: &str) -> Option<Self> {
        let mut file = File::open(path).ok()?;
        let mut data = Vec::new();
        file.read_to_end(&mut data).ok()?;

        if data.len() < 64 {
            return None;
        }

        let header = MAIFixedHeader::read(&data[0..64]).ok()?;
        if header.magic != MAI_MAGIC {
            return None;
        }

        let mut pos = 64;
        let name_len = header.archive_name_len as usize;
        if pos + name_len > data.len() {
            return None;
        }
        let archive_name = String::from_utf8_lossy(&data[pos..pos + name_len]).to_string();
        pos += name_len;

        if pos + 4 > data.len() {
            return None;
        }
        let section_count = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        let mut sections = HashMap::new();
        for _ in 0..section_count {
            if pos + 24 > data.len() {
                return None;
            }
            let sec = MAISection::read(&data[pos..pos + 24]);
            sections.insert(sec.section_type, sec);
            pos += 24;
        }

        Some(Self {
            header,
            archive_name,
            sections,
            data,
        })
    }

    pub fn header(&self) -> &MAIFixedHeader {
        &self.header
    }

    pub fn archive_name(&self) -> &str {
        &self.archive_name
    }

    pub fn has_section(&self, section_type: u32) -> bool {
        self.sections.contains_key(&section_type)
    }

    pub fn read_section(&self, section_type: u32) -> Vec<u8> {
        if let Some(sec) = self.sections.get(&section_type) {
            let start = sec.offset as usize;
            let end = start + sec.size as usize;
            if end <= self.data.len() {
                return self.data[start..end].to_vec();
            }
        }
        Vec::new()
    }
}

pub struct IndexOptions {
    pub params: HashMap<String, String>,
    pub aux_files: Vec<String>,
}

impl Default for IndexOptions {
    fn default() -> Self {
        Self {
            params: HashMap::new(),
            aux_files: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SearchResult {
    pub file_id: usize,
    pub filename: String,
    pub score: f64,
    pub content: String,
    pub metadata: HashMap<String, String>,
}

fn compute_minhash(content: &[u8], num_hashes: u32, seed: u64, bit_width: u8) -> Vec<u64> {
    const SHINGLE: usize = 8;
    let mut sketch = vec![u64::MAX; num_hashes as usize];

    let mut process = |h1: u64, h2: u64| {
        for i in 0..num_hashes {
            let v = h1.wrapping_add((i as u64).wrapping_mul(h2));
            if v < sketch[i as usize] {
                sketch[i as usize] = v;
            }
        }
    };

    let hash64 = |data: &[u8], s: u64| -> u64 {
        let mut hasher = XXHash3_64::new(s);
        hasher.update(data);
        hasher.finalize()
    };

    if content.len() < SHINGLE {
        process(hash64(content, seed), hash64(content, seed + 1));
    } else {
        for i in 0..=content.len() - SHINGLE {
            process(hash64(&content[i..i + SHINGLE], seed), hash64(&content[i..i + SHINGLE], seed + 1));
        }
    }

    if bit_width < 64 {
        let mask = (1u64 << bit_width) - 1;
        for v in &mut sketch {
            if *v != u64::MAX {
                *v &= mask;
            }
        }
    }

    sketch
}

pub fn build_minhash_index(
    reader: &MarReader,
    writer: &mut MAIWriter,
    opts: &IndexOptions,
) -> Result<(), String> {
    let num_hashes: u32 = opts.params.get("hashes")
        .or_else(|| opts.params.get("num_hashes"))
        .and_then(|s| s.parse().ok())
        .unwrap_or(128);
    let bit_width: u8 = opts.params.get("bit_width")
        .and_then(|s| s.parse().ok())
        .unwrap_or(64);
    let seed: u64 = opts.params.get("seed")
        .and_then(|s| s.parse().ok())
        .unwrap_or(42);

    let file_count = reader.file_count();
    let stride = (bit_width / 8) as usize;

    let mut all_sketches = vec![0u8; file_count * num_hashes as usize * stride];

    for i in 0..file_count {
        let content = if let Some(e) = reader.get_file_entry(i) {
            if e.entry_type == crate::format::EntryType::RegularFile {
                reader.read_file_by_index(i).unwrap_or_default()
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };

        let sketch = compute_minhash(&content, num_hashes, seed, bit_width);
        for h in 0..num_hashes as usize {
            let dest_offset = (i * num_hashes as usize + h) * stride;
            let bytes = sketch[h].to_le_bytes();
            all_sketches[dest_offset..dest_offset + stride].copy_from_slice(&bytes[..stride]);
        }
    }

    // Section 1: params block (32 bytes)
    let mut params = [0u8; 32];
    params[0..4].copy_from_slice(&(file_count as u32).to_le_bytes());
    params[4..8].copy_from_slice(&num_hashes.to_le_bytes());
    params[8] = bit_width;
    params[12..20].copy_from_slice(&seed.to_le_bytes());

    writer.add_section(1, params.to_vec(), 0);
    writer.add_section(2, all_sketches, 0);

    Ok(())
}

pub fn search_minhash(
    archive: &MarReader,
    index: &MAIReader,
    query: &str,
    opts: &IndexOptions,
) -> Result<Vec<SearchResult>, String> {
    let params_data = index.read_section(1);
    let sketch_data = index.read_section(2);

    if params_data.len() < 32 {
        return Err("Corrupt MinHash params".to_string());
    }

    let file_count = u32::from_le_bytes(params_data[0..4].try_into().unwrap()) as usize;
    let num_hashes = u32::from_le_bytes(params_data[4..8].try_into().unwrap());
    let bit_width = params_data[8];
    let seed = u64::from_le_bytes(params_data[12..20].try_into().unwrap());
    let stride = (bit_width / 8) as usize;

    let q_content = if let Some(f) = opts.params.get("file") {
        archive.read_file_by_name(f)?
    } else if Path::new(query).exists() {
        std::fs::read(query).map_err(|e| e.to_string())?
    } else {
        query.as_bytes().to_vec()
    };

    let q_sketch = compute_minhash(&q_content, num_hashes, seed, bit_width);

    let mut results = Vec::new();
    let topk: usize = opts.params.get("topk")
        .or_else(|| opts.params.get("topN"))
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);

    for i in 0..file_count {
        let mut matches = 0;
        let mut valid = 0;
        for h in 0..num_hashes as usize {
            let offset = (i * num_hashes as usize + h) * stride;
            let other_bytes = &sketch_data[offset..offset + stride];
            let q_bytes = &q_sketch[h].to_le_bytes()[..stride];

            let pad = other_bytes.iter().all(|&b| b == 0xFF);
            if !pad {
                valid += 1;
                if other_bytes == q_bytes {
                    matches += 1;
                }
            }
        }

        let score = if valid == 0 { 0.0 } else { matches as f64 / num_hashes as f64 };
        if score > 0.0 {
            let mut metadata = HashMap::new();
            metadata.insert("similarity".to_string(), format!("{:.4}", score));
            results.push(SearchResult {
                file_id: i,
                filename: archive.get_name(i).unwrap_or_else(|| format!("file_{}", i)),
                score,
                content: String::new(),
                metadata,
            });
        }
    }

    results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
    if results.len() > topk {
        results.truncate(topk);
    }

    Ok(results)
}

// ============================================================================
// BM25 Index & Search Implementation in pure Rust
// ============================================================================

pub const SEC_BM25_PARAMS: u32 = 1;
pub const SEC_BM25_TERM_DICT: u32 = 2;
pub const SEC_BM25_POSTINGS: u32 = 3;
pub const SEC_BM25_DOC_LENGTHS: u32 = 4;

pub fn tokenize_text(text: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();

    for c in text.chars() {
        if c.is_alphanumeric() {
            current.push(c.to_ascii_lowercase());
        } else if !current.is_empty() {
            if current.len() > 1 {
                tokens.push(std::mem::take(&mut current));
            } else {
                current.clear();
            }
        }
    }
    if current.len() > 1 {
        tokens.push(current);
    }

    tokens
}

#[derive(Default)]
struct RustInvertedIndex {
    postings: HashMap<String, Vec<(u32, u32)>>, // term -> [(doc_id, freq)]
    doc_lengths: Vec<u32>,
    term_to_id: HashMap<String, u32>,
    id_to_term: Vec<String>,
}

impl RustInvertedIndex {
    fn get_term_id(&mut self, term: &str) -> u32 {
        if let Some(&id) = self.term_to_id.get(term) {
            return id;
        }
        let id = self.id_to_term.len() as u32;
        self.term_to_id.insert(term.to_string(), id);
        self.id_to_term.push(term.to_string());
        id
    }

    fn add_document(&mut self, doc_id: u32, tokens: &[String]) {
        let mut term_freqs: HashMap<&str, u32> = HashMap::new();
        for token in tokens {
            *term_freqs.entry(token.as_str()).or_insert(0) += 1;
        }

        for (term, freq) in term_freqs {
            self.get_term_id(term);
            self.postings.entry(term.to_string()).or_default().push((doc_id, freq));
        }

        if doc_id as usize >= self.doc_lengths.len() {
            self.doc_lengths.resize(doc_id as usize + 1, 0);
        }
        self.doc_lengths[doc_id as usize] = tokens.len() as u32;
    }

    fn compute_avg_doc_length(&self) -> f32 {
        if self.doc_lengths.is_empty() {
            return 0.0;
        }
        let total: u64 = self.doc_lengths.iter().map(|&l| l as u64).sum();
        (total as f32) / (self.doc_lengths.len() as f32)
    }
}

pub fn build_bm25_index(
    reader: &MarReader,
    writer: &mut MAIWriter,
    opts: &IndexOptions,
) -> Result<(), String> {
    let k1: f32 = opts.params.get("bm25_k1")
        .or_else(|| opts.params.get("k1"))
        .and_then(|s| s.parse().ok())
        .unwrap_or(1.2);
    let b: f32 = opts.params.get("bm25_b")
        .or_else(|| opts.params.get("b"))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.75);

    let mut inv = RustInvertedIndex::default();
    let file_count = reader.file_count();

    for doc_id in 0..file_count {
        let entry_opt = reader.get_file_entry(doc_id);
        if entry_opt.as_ref().map(|e| e.entry_type) != Some(crate::format::EntryType::RegularFile) {
            inv.doc_lengths.push(0);
            continue;
        }

        let data = reader.read_file_by_index(doc_id).unwrap_or_default();
        let content = String::from_utf8_lossy(&data);
        let tokens = tokenize_text(&content);
        inv.add_document(doc_id as u32, &tokens);
    }

    let avg_doc_length = inv.compute_avg_doc_length();
    let num_terms = inv.id_to_term.len() as u32;
    let num_docs = inv.doc_lengths.len() as u32;

    // 1. BM25_PARAMS (48 bytes)
    // u32 num_docs, u32 num_terms, float avg_doc_length, float k1, float b, u32 reserved[7]
    let mut params_bytes = [0u8; 48];
    params_bytes[0..4].copy_from_slice(&num_docs.to_le_bytes());
    params_bytes[4..8].copy_from_slice(&num_terms.to_le_bytes());
    params_bytes[8..12].copy_from_slice(&avg_doc_length.to_le_bytes());
    params_bytes[12..16].copy_from_slice(&k1.to_le_bytes());
    params_bytes[16..20].copy_from_slice(&b.to_le_bytes());
    writer.add_section(SEC_BM25_PARAMS, params_bytes.to_vec(), 0);

    // 2. TERM_DICT: [u32 num_terms] [u16 len, string bytes...]
    let mut dict_bytes = Vec::new();
    dict_bytes.extend_from_slice(&num_terms.to_le_bytes());
    for term in &inv.id_to_term {
        let len = term.len() as u16;
        dict_bytes.extend_from_slice(&len.to_le_bytes());
        dict_bytes.extend_from_slice(term.as_bytes());
    }
    writer.add_section(SEC_BM25_TERM_DICT, dict_bytes, 0);

    // 3. POSTINGS: [u64 offset_table[num_terms]] followed by [u32 doc_count, (u32 doc_id, u32 freq)...]
    let mut postings_bytes = vec![0u8; (num_terms as usize) * 8];
    let mut offset_table: Vec<u64> = vec![0; num_terms as usize];

    for (term_id, term) in inv.id_to_term.iter().enumerate() {
        offset_table[term_id] = postings_bytes.len() as u64;
        if let Some(plist) = inv.postings.get(term) {
            let doc_count = plist.len() as u32;
            postings_bytes.extend_from_slice(&doc_count.to_le_bytes());
            for &(doc_id, freq) in plist {
                postings_bytes.extend_from_slice(&doc_id.to_le_bytes());
                postings_bytes.extend_from_slice(&freq.to_le_bytes());
            }
        } else {
            postings_bytes.extend_from_slice(&0u32.to_le_bytes());
        }
    }

    for (i, &off) in offset_table.iter().enumerate() {
        postings_bytes[i * 8..(i + 1) * 8].copy_from_slice(&off.to_le_bytes());
    }
    writer.add_section(SEC_BM25_POSTINGS, postings_bytes, 0);

    // 4. DOC_LENGTHS: [u32 len] * num_docs
    let mut lengths_bytes = Vec::with_capacity((num_docs as usize) * 4);
    for &len in &inv.doc_lengths {
        lengths_bytes.extend_from_slice(&len.to_le_bytes());
    }
    writer.add_section(SEC_BM25_DOC_LENGTHS, lengths_bytes, 0);

    Ok(())
}

pub fn search_bm25(
    archive: &MarReader,
    index: &MAIReader,
    query: &str,
    opts: &IndexOptions,
) -> Result<Vec<SearchResult>, String> {
    let params_data = index.read_section(SEC_BM25_PARAMS);
    if params_data.len() < 48 {
        return Err("Corrupt BM25 params".to_string());
    }

    let num_docs = u32::from_le_bytes(params_data[0..4].try_into().unwrap()) as usize;
    let num_terms = u32::from_le_bytes(params_data[4..8].try_into().unwrap()) as usize;
    let avg_doc_length = f32::from_le_bytes(params_data[8..12].try_into().unwrap());
    let k1 = f32::from_le_bytes(params_data[12..16].try_into().unwrap());
    let b = f32::from_le_bytes(params_data[16..20].try_into().unwrap());

    // Load term dict
    let dict_data = index.read_section(SEC_BM25_TERM_DICT);
    if dict_data.len() < 4 {
        return Err("Corrupt BM25 term dict".to_string());
    }

    let mut term_to_id = HashMap::with_capacity(num_terms);
    let mut pos = 4;
    for i in 0..num_terms {
        if pos + 2 > dict_data.len() { break; }
        let len = u16::from_le_bytes(dict_data[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;
        if pos + len > dict_data.len() { break; }
        let term = String::from_utf8_lossy(&dict_data[pos..pos + len]).to_string();
        pos += len;
        term_to_id.insert(term, i as u32);
    }

    // Load doc lengths
    let lengths_data = index.read_section(SEC_BM25_DOC_LENGTHS);
    let mut doc_lengths = Vec::with_capacity(num_docs);
    for i in 0..num_docs {
        if (i + 1) * 4 <= lengths_data.len() {
            let l = u32::from_le_bytes(lengths_data[i * 4..(i + 1) * 4].try_into().unwrap());
            doc_lengths.push(l);
        } else {
            doc_lengths.push(0);
        }
    }

    // Load postings offsets
    let postings_data = index.read_section(SEC_BM25_POSTINGS);
    if postings_data.len() < num_terms * 8 {
        return Err("Corrupt BM25 postings".to_string());
    }

    let query_terms = tokenize_text(query);
    if query_terms.is_empty() {
        return Ok(Vec::new());
    }

    let mut doc_scores: HashMap<u32, f32> = HashMap::new();

    for term in query_terms {
        let term_id = match term_to_id.get(&term) {
            Some(&id) => id as usize,
            None => continue,
        };

        let offset = u64::from_le_bytes(postings_data[term_id * 8..(term_id + 1) * 8].try_into().unwrap()) as usize;
        if offset + 4 > postings_data.len() {
            continue;
        }

        let doc_count = u32::from_le_bytes(postings_data[offset..offset + 4].try_into().unwrap()) as usize;
        let mut ppos = offset + 4;

        if doc_count == 0 {
            continue;
        }

        // IDF = ln((N - nt + 0.5) / (nt + 0.5))
        let n = num_docs as f32;
        let nt = doc_count as f32;
        let idf = ((n - nt + 0.5) / (nt + 0.5)).ln().max(0.0);

        for _ in 0..doc_count {
            if ppos + 8 > postings_data.len() { break; }
            let doc_id = u32::from_le_bytes(postings_data[ppos..ppos + 4].try_into().unwrap());
            let freq = u32::from_le_bytes(postings_data[ppos + 4..ppos + 8].try_into().unwrap());
            ppos += 8;

            if (doc_id as usize) < doc_lengths.len() {
                let dl = doc_lengths[doc_id as usize] as f32;
                let norm = 1.0 - b + b * (dl / avg_doc_length);
                let f = freq as f32;
                let denom = f + k1 * norm;
                if denom > 1e-10 {
                    let score = idf * (f * (k1 + 1.0)) / denom;
                    *doc_scores.entry(doc_id).or_insert(0.0) += score;
                }
            }
        }
    }

    let topk: usize = opts.params.get("topk")
        .or_else(|| opts.params.get("topN"))
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);

    let mut results: Vec<SearchResult> = doc_scores.into_iter().map(|(doc_id, score)| {
        let mut meta = HashMap::new();
        meta.insert("bm25_score".to_string(), format!("{:.4}", score));
        SearchResult {
            file_id: doc_id as usize,
            filename: archive.get_name(doc_id as usize).unwrap_or_else(|| format!("file_{}", doc_id)),
            score: score as f64,
            content: String::new(),
            metadata: meta,
        }
    }).collect();

    results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
    if results.len() > topk {
        results.truncate(topk);
    }

    Ok(results)
}

// ============================================================================
// Email Index & Search Implementation in pure Rust
// ============================================================================

pub const SEC_EMAIL_PARAMS: u32 = 1;
pub const SEC_EMAIL_HEADERS: u32 = 2;
pub const SEC_EMAIL_STRINGS: u32 = 3;
pub const SEC_EMAIL_INVERTED: u32 = 4;

pub fn build_email_index(
    reader: &MarReader,
    writer: &mut MAIWriter,
    _opts: &IndexOptions,
) -> Result<(), String> {
    let file_count = reader.file_count();
    let mut string_table = Vec::<String>::new();
    let mut string_offsets = HashMap::<String, u32>::new();

    let mut add_string = |s: &str| -> u32 {
        if let Some(&idx) = string_offsets.get(s) {
            return idx;
        }
        let idx = string_table.len() as u32;
        string_offsets.insert(s.to_string(), idx);
        string_table.push(s.to_string());
        idx
    };

    add_string(""); // index 0 is empty

    let mut headers_data = Vec::new();
    let mut message_count = 0u32;

    for i in 0..file_count {
        let _entry = match reader.get_file_entry(i) {
            Some(e) if e.entry_type == crate::format::EntryType::RegularFile => e,
            _ => continue,
        };

        let data = reader.read_file_by_index(i).unwrap_or_default();
        let content = String::from_utf8_lossy(&data);

        // Simple MIME header scan
        let mut from = String::new();
        let mut to = String::new();
        let mut subject = String::new();
        let mut msgid = String::new();

        for line in content.lines() {
            if line.is_empty() {
                break; // End of headers
            }
            let lower = line.to_ascii_lowercase();
            if lower.starts_with("from:") {
                from = line[5..].trim().to_string();
            } else if lower.starts_with("to:") {
                to = line[3..].trim().to_string();
            } else if lower.starts_with("subject:") {
                subject = line[8..].trim().to_string();
            } else if lower.starts_with("message-id:") {
                msgid = line[11..].trim().to_string();
            }
        }

        let from_idx = add_string(&from);
        let to_idx = add_string(&to);
        let subj_idx = add_string(&subject);
        let msgid_idx = add_string(&msgid);

        let mut entry_buf = [0u8; 48];
        entry_buf[0..4].copy_from_slice(&(i as u32).to_le_bytes());
        entry_buf[4..8].copy_from_slice(&message_count.to_le_bytes());
        entry_buf[8..12].copy_from_slice(&from_idx.to_le_bytes());
        entry_buf[12..16].copy_from_slice(&to_idx.to_le_bytes());
        entry_buf[16..20].copy_from_slice(&subj_idx.to_le_bytes());
        entry_buf[20..24].copy_from_slice(&msgid_idx.to_le_bytes());
        entry_buf[32..36].copy_from_slice(&message_count.to_le_bytes());
        headers_data.extend_from_slice(&entry_buf);

        message_count += 1;
    }

    let mut params = [0u8; 32];
    params[0..4].copy_from_slice(&(file_count as u32).to_le_bytes());
    params[4..8].copy_from_slice(&message_count.to_le_bytes());
    params[8..12].copy_from_slice(&0u32.to_le_bytes()); // token_count
    params[12..16].copy_from_slice(&message_count.to_le_bytes()); // thread_count
    params[16] = 1; // version

    let mut str_table_data = Vec::new();
    let str_count = string_table.len() as u32;
    str_table_data.extend_from_slice(&str_count.to_le_bytes());

    let mut current_str_offset = 0u32;
    let mut str_bytes = Vec::new();
    for s in &string_table {
        str_table_data.extend_from_slice(&current_str_offset.to_le_bytes());
        str_bytes.extend_from_slice(s.as_bytes());
        str_bytes.push(0); // NUL terminator
        current_str_offset += (s.len() + 1) as u32;
    }
    str_table_data.extend_from_slice(&str_bytes);

    writer.add_section(SEC_EMAIL_PARAMS, params.to_vec(), 0);
    writer.add_section(SEC_EMAIL_HEADERS, headers_data, 0);
    writer.add_section(SEC_EMAIL_STRINGS, str_table_data, 0);
    writer.add_section(SEC_EMAIL_INVERTED, Vec::new(), 0);

    Ok(())
}

pub fn search_email(
    archive: &MarReader,
    index: &MAIReader,
    query: &str,
    opts: &IndexOptions,
) -> Result<Vec<SearchResult>, String> {
    let params_data = index.read_section(SEC_EMAIL_PARAMS);
    if params_data.len() < 32 {
        return Err("Corrupt Email params".to_string());
    }
    let msg_count = u32::from_le_bytes(params_data[4..8].try_into().unwrap()) as usize;

    let headers_data = index.read_section(SEC_EMAIL_HEADERS);
    if headers_data.len() < msg_count * 48 {
        return Err("Corrupt Email headers section".to_string());
    }

    let str_data = index.read_section(SEC_EMAIL_STRINGS);
    let get_str = |idx: u32| -> String {
        if str_data.len() < 4 { return String::new(); }
        let total = u32::from_le_bytes(str_data[0..4].try_into().unwrap()) as usize;
        if idx as usize >= total || 4 + total * 4 > str_data.len() {
            return String::new();
        }
        let off = u32::from_le_bytes(str_data[4 + (idx as usize) * 4..8 + (idx as usize) * 4].try_into().unwrap()) as usize;
        let base = 4 + total * 4 + off;
        if base >= str_data.len() { return String::new(); }
        let end = str_data[base..].iter().position(|&b| b == 0).map(|p| base + p).unwrap_or(str_data.len());
        String::from_utf8_lossy(&str_data[base..end]).to_string()
    };

    let q_lower = query.to_ascii_lowercase();
    let mut results = Vec::new();

    for m in 0..msg_count {
        let entry = &headers_data[m * 48..(m + 1) * 48];
        let file_id = u32::from_le_bytes(entry[0..4].try_into().unwrap()) as usize;
        let from_idx = u32::from_le_bytes(entry[8..12].try_into().unwrap());
        let to_idx = u32::from_le_bytes(entry[12..16].try_into().unwrap());
        let subj_idx = u32::from_le_bytes(entry[16..20].try_into().unwrap());

        let from = get_str(from_idx);
        let to = get_str(to_idx);
        let subject = get_str(subj_idx);

        let mut matched = false;
        let mut score = 0.0;

        if subject.to_ascii_lowercase().contains(&q_lower) {
            matched = true;
            score += 1.0;
        }
        if from.to_ascii_lowercase().contains(&q_lower) {
            matched = true;
            score += 0.8;
        }
        if to.to_ascii_lowercase().contains(&q_lower) {
            matched = true;
            score += 0.5;
        }

        if matched {
            let mut meta = HashMap::new();
            meta.insert("from".to_string(), from);
            meta.insert("to".to_string(), to);
            meta.insert("subject".to_string(), subject);

            results.push(SearchResult {
                file_id,
                filename: archive.get_name(file_id).unwrap_or_else(|| format!("msg_{}", file_id)),
                score,
                content: String::new(),
                metadata: meta,
            });
        }
    }

    let topk: usize = opts.params.get("topk")
        .or_else(|| opts.params.get("topN"))
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);

    results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
    if results.len() > topk {
        results.truncate(topk);
    }

    Ok(results)
}

// ============================================================================
// TimeSeries Index & Search Implementation in pure Rust
// ============================================================================

pub const SEC_TS_PARAMS: u32 = 1;
pub const SEC_TS_METADATA: u32 = 2;
pub const SEC_TS_COLSTATS: u32 = 3;

#[repr(C, packed)]
#[derive(Clone, Copy)]
struct TsParams {
    file_count: u32,
    ts_col_idx: u32,
    ts_format_enum: u8,
    delim: u8,
    has_header: u8,
    reserved0: u8,
    skip_rows: u32,
    ts_col_name: [u8; 64],
    ts_format_str: [u8; 128],
    reserved1: [u8; 48],
}

impl Default for TsParams {
    fn default() -> Self {
        Self {
            file_count: 0,
            ts_col_idx: 0xFFFFFFFF,
            ts_format_enum: 0,
            delim: b',',
            has_header: 1,
            reserved0: 0,
            skip_rows: 0,
            ts_col_name: [0u8; 64],
            ts_format_str: [0u8; 128],
            reserved1: [0u8; 48],
        }
    }
}

#[repr(C, packed)]
#[derive(Clone, Copy)]
struct TsFileMetadata {
    ts_min: u64,
    ts_max: u64,
    row_count: u32,
    col_count: u32,
    value_col_count: u32,
    col_stats_start: u32,
    col_names_str_offset: u32,
    ts_indexed: u8,
    reserved: [u8; 27],
}

impl Default for TsFileMetadata {
    fn default() -> Self {
        Self {
            ts_min: u64::MAX,
            ts_max: 0,
            row_count: 0,
            col_count: 0,
            value_col_count: 0,
            col_stats_start: 0,
            col_names_str_offset: 0,
            ts_indexed: 0,
            reserved: [0u8; 27],
        }
    }
}

#[repr(C, packed)]
#[derive(Clone, Copy, Default)]
struct ColStats {
    mean: f64,
    stddev: f64,
    vmin: f64,
    vmax: f64,
    count: u64,
    col_name_str_offset: u32,
    reserved: u32,
}

fn split_row(line: &str, delim: char) -> Vec<String> {
    let mut cols = Vec::new();
    let mut cur = String::new();
    let mut in_quote = false;
    for c in line.chars() {
        if c == '"' {
            in_quote = !in_quote;
        } else if !in_quote && c == delim {
            cols.push(cur);
            cur = String::new();
        } else {
            cur.push(c);
        }
    }
    cols.push(cur);
    cols
}

fn parse_ts_val(val: &str) -> Option<u64> {
    let s = val.trim();
    if s.is_empty() {
        return None;
    }
    if let Ok(epoch_ms) = s.parse::<u64>() {
        if epoch_ms > 1_000_000_000_000 {
            return Some(epoch_ms);
        } else if epoch_ms > 1_000_000_000 {
            return Some(epoch_ms * 1000);
        }
    }
    None
}

pub fn build_timeseries_index(
    reader: &MarReader,
    writer: &mut MAIWriter,
    opts: &IndexOptions,
) -> Result<(), String> {
    let file_count = reader.file_count();
    let delim_char = opts.params.get("delim").and_then(|s| s.chars().next()).unwrap_or(',');
    let has_header = opts.params.get("has_header").map(|s| s != "false").unwrap_or(true);
    let skip_rows: usize = opts.params.get("skip_rows").and_then(|s| s.parse().ok()).unwrap_or(0);

    let mut meta_table = Vec::<TsFileMetadata>::with_capacity(file_count);
    let all_stats = Vec::<ColStats>::new();
    let col_strtab = Vec::<u8>::new();
    let _strtab_cache = HashMap::<String, u32>::new();

    for fi in 0..file_count {
        let _entry = match reader.get_file_entry(fi) {
            Some(e) if e.entry_type == crate::format::EntryType::RegularFile => e,
            _ => {
                meta_table.push(TsFileMetadata::default());
                continue;
            }
        };

        let fname = reader.get_name(fi).unwrap_or_default();
        let is_tabular = fname.ends_with(".csv") || fname.ends_with(".tsv") || fname.ends_with(".txt");
        if !is_tabular {
            meta_table.push(TsFileMetadata::default());
            continue;
        }

        let data = reader.read_file_by_index(fi).unwrap_or_default();
        let text = String::from_utf8_lossy(&data);

        let mut fm = TsFileMetadata::default();
        fm.ts_min = u64::MAX;
        fm.ts_max = 0;
        fm.col_stats_start = all_stats.len() as u32;

        let mut lines = text.lines();
        for _ in 0..skip_rows {
            lines.next();
        }

        let mut col_names = Vec::new();
        if has_header {
            if let Some(header_line) = lines.next() {
                col_names = split_row(header_line, delim_char);
            }
        }

        let mut row_count = 0u32;
        for line in lines {
            if line.is_empty() {
                continue;
            }
            let cols = split_row(line, delim_char);
            if cols.is_empty() {
                continue;
            }
            row_count += 1;
            // Best effort timestamp on first col
            if let Some(ts) = parse_ts_val(&cols[0]) {
                if ts < fm.ts_min { fm.ts_min = ts; }
                if ts > fm.ts_max { fm.ts_max = ts; }
            }
        }

        fm.row_count = row_count;
        fm.col_count = col_names.len() as u32;
        fm.ts_indexed = if fm.ts_max >= fm.ts_min && fm.ts_min != u64::MAX { 1 } else { 0 };
        meta_table.push(fm);
    }

    let mut params = TsParams::default();
    params.file_count = file_count as u32;
    params.delim = delim_char as u8;
    params.has_header = if has_header { 1 } else { 0 };
    params.skip_rows = skip_rows as u32;

    let params_bytes = unsafe {
        std::slice::from_raw_parts(&params as *const TsParams as *const u8, std::mem::size_of::<TsParams>())
    };
    writer.add_section(SEC_TS_PARAMS, params_bytes.to_vec(), 0);

    let meta_bytes = unsafe {
        std::slice::from_raw_parts(meta_table.as_ptr() as *const u8, meta_table.len() * std::mem::size_of::<TsFileMetadata>())
    };
    writer.add_section(SEC_TS_METADATA, meta_bytes.to_vec(), 0);

    let mut colstats_data = Vec::new();
    let total_stats = all_stats.len() as u32;
    let strtab_offset = 8 + total_stats * 48;
    colstats_data.extend_from_slice(&total_stats.to_le_bytes());
    colstats_data.extend_from_slice(&strtab_offset.to_le_bytes());
    let stats_bytes = unsafe {
        std::slice::from_raw_parts(all_stats.as_ptr() as *const u8, all_stats.len() * std::mem::size_of::<ColStats>())
    };
    colstats_data.extend_from_slice(stats_bytes);
    colstats_data.extend_from_slice(&col_strtab);
    writer.add_section(SEC_TS_COLSTATS, colstats_data, 0);

    Ok(())
}

pub fn search_timeseries(
    archive: &MarReader,
    index: &MAIReader,
    _query: &str,
    opts: &IndexOptions,
) -> Result<Vec<SearchResult>, String> {
    let params_data = index.read_section(SEC_TS_PARAMS);
    if params_data.len() < std::mem::size_of::<TsParams>() {
        return Err("Corrupt TS params".to_string());
    }
    let file_count = u32::from_le_bytes(params_data[0..4].try_into().unwrap()) as usize;

    let meta_data = index.read_section(SEC_TS_METADATA);
    if meta_data.len() < file_count * std::mem::size_of::<TsFileMetadata>() {
        return Err("Corrupt TS metadata".to_string());
    }

    let since_ms: u64 = opts.params.get("since").and_then(|s| parse_ts_val(s)).unwrap_or(0);
    let until_ms: u64 = opts.params.get("until").and_then(|s| parse_ts_val(s)).unwrap_or(u64::MAX);

    let mut results = Vec::new();
    let meta_slice: &[TsFileMetadata] = unsafe {
        std::slice::from_raw_parts(meta_data.as_ptr() as *const TsFileMetadata, file_count)
    };

    for (fi, m) in meta_slice.iter().enumerate() {
        let ts_indexed = m.ts_indexed;
        let ts_min = m.ts_min;
        let ts_max = m.ts_max;
        let row_count = m.row_count;

        if ts_indexed == 0 {
            continue;
        }
        if since_ms != 0 && ts_max != 0 && ts_max < since_ms {
            continue;
        }
        if until_ms != u64::MAX && ts_min != 0 && ts_min > until_ms {
            continue;
        }

        let mut meta = HashMap::new();
        meta.insert("ts_min".to_string(), ts_min.to_string());
        meta.insert("ts_max".to_string(), ts_max.to_string());
        meta.insert("row_count".to_string(), row_count.to_string());

        results.push(SearchResult {
            file_id: fi,
            filename: archive.get_name(fi).unwrap_or_else(|| format!("file_{}", fi)),
            score: 1.0,
            content: String::new(),
            metadata: meta,
        });
    }

    let topk: usize = opts.params.get("topk")
        .or_else(|| opts.params.get("topN"))
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);

    if results.len() > topk {
        results.truncate(topk);
    }

    Ok(results)
}

// ============================================================================
// Genomic Index & Search Implementation in pure Rust
// ============================================================================

pub const SEC_GENOMIC_PARAMS: u32 = 1;
pub const SEC_GENOMIC_SKETCHES: u32 = 2;
pub const SEC_GENOMIC_CONTIGS: u32 = 3;
pub const SEC_GENOMIC_REGIONS: u32 = 4;
pub const SEC_GENOMIC_COMPAT: u32 = 5;

#[repr(C, packed)]
#[derive(Clone, Copy)]
struct GenomicParams {
    file_count: u32,
    k: u32,
    num_hashes: u32,
    seed: u64,
    stranded: u8,
    reserved: [u8; 3],
    genomic_file_count: u32,
    region_entry_count: u32,
    padding: [u8; 32],
}

impl Default for GenomicParams {
    fn default() -> Self {
        Self {
            file_count: 0,
            k: 21,
            num_hashes: 256,
            seed: 42,
            stranded: 0,
            reserved: [0u8; 3],
            genomic_file_count: 0,
            region_entry_count: 0,
            padding: [0u8; 32],
        }
    }
}

#[repr(C, packed)]
#[derive(Clone, Copy, Default)]
struct FileContigDir {
    contig_list_start: u32,
    contig_count: u32,
    file_type: u8,
    reserved: [u8; 3],
}

#[repr(C, packed)]
#[derive(Clone, Copy, Default)]
#[allow(dead_code)]
struct ContigRecord {
    name_offset: u32,
    length_bp: u64,
    seq_checksum: u32,
}

pub fn build_genomic_index(
    reader: &MarReader,
    writer: &mut MAIWriter,
    opts: &IndexOptions,
) -> Result<(), String> {
    let file_count = reader.file_count();
    let k: u32 = opts.params.get("k").and_then(|s| s.parse().ok()).unwrap_or(21);
    let num_hashes: u32 = opts.params.get("num_hashes").and_then(|s| s.parse().ok()).unwrap_or(256);
    let seed: u64 = opts.params.get("seed").and_then(|s| s.parse().ok()).unwrap_or(42);

    let mut params = GenomicParams::default();
    params.file_count = file_count as u32;
    params.k = k;
    params.num_hashes = num_hashes;
    params.seed = seed;

    let params_bytes = unsafe {
        std::slice::from_raw_parts(&params as *const GenomicParams as *const u8, std::mem::size_of::<GenomicParams>())
    };
    writer.add_section(SEC_GENOMIC_PARAMS, params_bytes.to_vec(), 0);

    let sketch_size = file_count * (num_hashes as usize) * 8;
    let sketches = vec![0xFFu8; sketch_size];
    writer.add_section(SEC_GENOMIC_SKETCHES, sketches, 0);

    let mut contig_sec = Vec::new();
    let fc = file_count as u32;
    let tc = 0u32;
    let strtab_offset = 12 + fc * 8;
    contig_sec.extend_from_slice(&fc.to_le_bytes());
    contig_sec.extend_from_slice(&tc.to_le_bytes());
    contig_sec.extend_from_slice(&strtab_offset.to_le_bytes());

    let dirs = vec![FileContigDir::default(); file_count];
    let dirs_bytes = unsafe {
        std::slice::from_raw_parts(dirs.as_ptr() as *const u8, dirs.len() * std::mem::size_of::<FileContigDir>())
    };
    contig_sec.extend_from_slice(dirs_bytes);
    writer.add_section(SEC_GENOMIC_CONTIGS, contig_sec, 0);

    let mut reg_sec = Vec::new();
    reg_sec.extend_from_slice(&0u32.to_le_bytes()); // fasta_count
    reg_sec.extend_from_slice(&0u32.to_le_bytes()); // vcf_count
    reg_sec.extend_from_slice(&0u32.to_le_bytes()); // bam_count
    writer.add_section(SEC_GENOMIC_REGIONS, reg_sec, 0);

    Ok(())
}

pub fn search_genomic(
    archive: &MarReader,
    index: &MAIReader,
    query: &str,
    opts: &IndexOptions,
) -> Result<Vec<SearchResult>, String> {
    let params_data = index.read_section(SEC_GENOMIC_PARAMS);
    if params_data.len() < std::mem::size_of::<GenomicParams>() {
        return Err("Corrupt Genomic params".to_string());
    }

    let mut results = Vec::new();
    let topk: usize = opts.params.get("topk")
        .or_else(|| opts.params.get("topN"))
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);

    for fi in 0..archive.file_count() {
        if let Some(name) = archive.get_name(fi) {
            if name.contains(query) {
                results.push(SearchResult {
                    file_id: fi,
                    filename: name,
                    score: 1.0,
                    content: String::new(),
                    metadata: HashMap::new(),
                });
                if results.len() >= topk {
                    break;
                }
            }
        }
    }

    Ok(results)
}


