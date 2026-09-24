use crate::format::*;
use crate::name_index::{recommend_format, write_name_table};
use crate::reader::MarReader;
use std::collections::HashSet;
use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

pub fn redact_archive(
    input_path: &str,
    output_path: &str,
    files_to_redact: &[String],
    in_place: bool,
    force: bool,
) -> Result<(), String> {
    if files_to_redact.is_empty() {
        return Err("No files specified for redaction".to_string());
    }

    let target_path = if in_place {
        input_path.to_string()
    } else {
        if output_path.is_empty() {
            return Err("Missing output path (use -o, or -I for in-place)".to_string());
        }
        if !force && Path::new(output_path).exists() {
            return Err(format!("Output archive exists (use --force): {}", output_path));
        }
        fs::copy(input_path, output_path)
            .map_err(|e| format!("Failed to copy archive to {}: {}", output_path, e))?;
        output_path.to_string()
    };

    let reader = MarReader::open(&target_path).map_err(|e| format!("Failed to open archive: {}", e))?;

    // Refuse to redact archives with compressed data blocks.
    for i in 0..reader.block_count() {
        let bh = reader.get_block_header(i).ok_or_else(|| format!("Invalid block {}", i))?;
        if bh.comp_algo != CompressionAlgo::None {
            return Err(format!(
                "Redaction is not supported for compressed archives. Block {} uses {:?}.",
                i, bh.comp_algo
            ));
        }
    }

    // Resolve requested indices
    let mut requested = HashSet::new();
    for name in files_to_redact {
        match reader.find_file(name) {
            Some((idx, _)) => {
                requested.insert(idx);
            }
            None => {
                return Err(format!("File not found in archive: {}", name));
            }
        }
    }

    // Collect blocks to zero
    let mut blocks_to_zero = HashSet::new();
    for &idx in &requested {
        for b in reader.get_block_ids_for_file(idx) {
            blocks_to_zero.insert(b);
        }
    }

    // Expand to all files that share any affected block (e.g. dedup)
    let mut affected_files = requested.clone();
    for i in 0..reader.file_count() {
        let _entry = match reader.get_file_entry(i) {
            Some(e) if e.entry_type == EntryType::RegularFile => e,
            _ => continue,
        };
        for b in reader.get_block_ids_for_file(i) {
            if blocks_to_zero.contains(&b) {
                affected_files.insert(i);
                break;
            }
        }
    }

    // Overwrite block payloads with zeros
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&target_path)
        .map_err(|e| format!("Failed to open target archive for writing: {}", e))?;

    for &b in &blocks_to_zero {
        if (b as usize) >= reader.block_count() {
            return Err(format!("Invalid block id for redaction: {}", b));
        }
        let bh = reader.get_block_header(b as usize).unwrap();
        let off = reader.get_block_offset(b as usize);
        let payload_off = off + BLOCK_HEADER_SIZE as u64;

        file.seek(SeekFrom::Start(payload_off))
            .map_err(|e| format!("Failed to seek to block payload: {}", e))?;

        let zeros = vec![0u8; bh.stored_size as usize];
        file.write_all(&zeros)
            .map_err(|e| format!("Failed to write zero payload: {}", e))?;
    }

    // Rebuild metadata container (uncompressed) with REDACTED flags set
    let file_count = reader.file_count();
    let mut new_entries = Vec::with_capacity(file_count);
    for i in 0..file_count {
        let mut entry = reader.get_file_entry(i).unwrap().clone();
        if affected_files.contains(&i) {
            entry.entry_flags |= entry_flags::REDACTED;
            entry.entry_flags &= !entry_flags::HAS_STRONG_HASH;
        }
        new_entries.push(entry);
    }

    let names: Vec<String> = reader.get_names().to_vec();
    let name_format = recommend_format(&names);
    let name_data = write_name_table(&names, name_format);

    let mut file_data = Vec::with_capacity(4 + new_entries.len() * FILE_ENTRY_SIZE);
    file_data.extend_from_slice(&(new_entries.len() as u32).to_le_bytes());
    for e in &new_entries {
        let mut buf = [0u8; FILE_ENTRY_SIZE];
        e.write(&mut buf);
        file_data.extend_from_slice(&buf);
    }

    struct Sec {
        stype: u32,
        flags: u32,
        data: Vec<u8>,
    }
    let mut sections: Vec<Sec> = Vec::new();
    sections.push(Sec { stype: section_type::NAME_TABLE, flags: name_format as u32, data: name_data });
    sections.push(Sec { stype: section_type::FILE_TABLE, flags: 0, data: file_data });

    if reader.header().index_type == IndexType::Multiblock {
        let mut spans_data = Vec::new();
        let fc = file_count as u32;
        let mut all_spans = Vec::with_capacity(file_count);
        for i in 0..file_count {
            all_spans.push(reader.get_file_spans(i));
        }
        let total_spans: u32 = all_spans.iter().map(|s| s.len() as u32).sum();
        spans_data.extend_from_slice(&fc.to_le_bytes());
        spans_data.extend_from_slice(&total_spans.to_le_bytes());

        let mut offset = 0u32;
        for s in &all_spans {
            spans_data.extend_from_slice(&offset.to_le_bytes());
            offset += s.len() as u32;
        }
        for s in &all_spans {
            spans_data.extend_from_slice(&(s.len() as u32).to_le_bytes());
        }
        for s in &all_spans {
            for span in s {
                let mut sbuf = [0u8; SPAN_SIZE];
                span.write(&mut sbuf);
                spans_data.extend_from_slice(&sbuf);
            }
        }
        sections.push(Sec { stype: section_type::FILE_SPANS, flags: 0, data: spans_data });
    }

    // Block table
    let mut block_table_data = Vec::with_capacity(4 + reader.block_count() * BLOCK_DESC_SIZE);
    block_table_data.extend_from_slice(&(reader.block_count() as u32).to_le_bytes());
    for i in 0..reader.block_count() {
        let off = reader.get_block_offset(i);
        let bh = reader.get_block_header(i).unwrap();
        let desc = BlockDesc {
            block_offset: off,
            raw_size: bh.raw_size,
            stored_size: bh.stored_size,
        };
        let mut bbuf = [0u8; BLOCK_DESC_SIZE];
        desc.write(&mut bbuf);
        block_table_data.extend_from_slice(&bbuf);
    }
    sections.push(Sec { stype: section_type::BLOCK_TABLE, flags: 0, data: block_table_data });

    // Posix meta
    if reader.has_posix_meta() {
        let mut pdata = Vec::with_capacity(4 + file_count * POSIX_ENTRY_SIZE);
        pdata.extend_from_slice(&(file_count as u32).to_le_bytes());
        for i in 0..file_count {
            let p = reader.get_posix_meta(i).unwrap_or(PosixEntry {
                mode: 0, uid: 0, gid: 0, mtime: 0, atime: 0, ctime: 0,
            });
            let mut pbuf = [0u8; POSIX_ENTRY_SIZE];
            p.write(&mut pbuf);
            pdata.extend_from_slice(&pbuf);
        }
        sections.push(Sec { stype: section_type::POSIX_META, flags: 0, data: pdata });
    }

    // Hashes
    if reader.has_hashes() {
        let mut hdata = Vec::new();
        hdata.extend_from_slice(&(file_count as u32).to_le_bytes());
        hdata.push(reader.get_hash_algo() as u8);
        hdata.push(32u8);
        hdata.extend_from_slice(&[0u8; 2]);

        let bitset_size = (file_count + 7) / 8;
        let mut bitset = vec![0u8; bitset_size];
        for i in 0..file_count {
            if !affected_files.contains(&i) && reader.get_hash(i).is_some() {
                bitset[i / 8] |= 1 << (i % 8);
            }
        }
        hdata.extend_from_slice(&bitset);
        for i in 0..file_count {
            if !affected_files.contains(&i) {
                if let Some(h) = reader.get_hash(i) {
                    hdata.extend_from_slice(&h);
                }
            }
        }
        sections.push(Sec { stype: section_type::FILE_HASHES, flags: 0, data: hdata });
    }

    // Assemble metadata container
    let mut meta = Vec::new();
    let section_count = sections.len() as u32;
    meta.extend_from_slice(&section_count.to_le_bytes());
    meta.extend_from_slice(&0u32.to_le_bytes());

    let mut offset = (META_CONTAINER_HEADER_SIZE + sections.len() * SECTION_ENTRY_SIZE) as u64;
    for s in &sections {
        let se = SectionEntry {
            section_type: s.stype,
            flags: s.flags,
            payload_offset: offset,
            stored_size: s.data.len() as u64,
            raw_size: 0,
        };
        let mut se_buf = [0u8; SECTION_ENTRY_SIZE];
        se.write(&mut se_buf);
        meta.extend_from_slice(&se_buf);
        offset += s.data.len() as u64;
    }
    for s in &sections {
        meta.extend_from_slice(&s.data);
    }

    let meta_offset = file.seek(SeekFrom::End(0)).map_err(|e| e.to_string())?;
    file.write_all(&meta)
        .map_err(|e| format!("Failed to append updated metadata: {}", e))?;

    let mut new_hdr = reader.header().clone();
    new_hdr.meta_offset = meta_offset;
    new_hdr.meta_stored_size = meta.len() as u64;
    new_hdr.meta_raw_size = 0;
    new_hdr.meta_comp_algo = CompressionAlgo::None;
    new_hdr.header_crc32c = new_hdr.compute_crc32c();

    let mut hdr_bytes = [0u8; FIXED_HEADER_SIZE];
    new_hdr.write(&mut hdr_bytes);

    file.seek(SeekFrom::Start(0)).map_err(|e| e.to_string())?;
    file.write_all(&hdr_bytes)
        .map_err(|e| format!("Failed to update fixed header: {}", e))?;

    file.flush().map_err(|e| e.to_string())?;

    Ok(())
}
