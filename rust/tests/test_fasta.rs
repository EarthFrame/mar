use mar_core::mai::*;
use mar_core::reader::MarReader;
use mar_core::writer::{MarWriter, WriteOptions};
use std::fs;

#[test]
fn test_rust_fasta_index_multi_file() {
    let test_dir = std::env::temp_dir().join("mar_rust_fasta_test");
    let _ = fs::remove_dir_all(&test_dir);
    fs::create_dir_all(&test_dir).unwrap();

    let fa1_path = test_dir.join("human.fasta");
    let fa2_path = test_dir.join("mouse.fa");
    let mar_path = test_dir.join("mammals.mar");
    let mai_path = test_dir.join("mammals.fasta.mai");

    fs::write(
        &fa1_path,
        ">AF-A0A022R2B6-F1 AlphaFold human protein\nMKFLVNVALVFMVVYISYIYAAFPSQ\nEKSNEEQKEEEREEEEKK\n>P12345 Human protein\nACDEFGHIKLMNPQRSTVWY\n",
    )
    .unwrap();

    fs::write(
        &fa2_path,
        ">MOUSE_001 Mouse protein\nMVKVGVNGFGRIGRLVTRAAFNSG\n>AF-A0A022R2B6-F1 Shared isoform\nACDEFGHIKLMNPQRSTVWY\n",
    )
    .unwrap();

    let mut opts = WriteOptions::default();
    opts.multiblock = true;
    opts.block_size = 4096;

    let mut writer = MarWriter::new(mar_path.to_str().unwrap(), opts);
    writer.add_file(fa1_path.to_str().unwrap(), "human.fasta").unwrap();
    writer.add_file(fa2_path.to_str().unwrap(), "mouse.fa").unwrap();
    writer.finish().unwrap();

    let reader = MarReader::open(mar_path.to_str().unwrap()).unwrap();
    let mut mai_writer = MAIWriter::new(mar_path.to_str().unwrap(), MAIIndexType::Fasta, 12345);
    let index_opts = IndexOptions::default();
    build_fasta_index(&reader, &mut mai_writer, &index_opts).unwrap();
    mai_writer.write_to_file(mai_path.to_str().unwrap(), 0).unwrap();

    let mai_reader = MAIReader::open(mai_path.to_str().unwrap()).unwrap();
    assert_eq!(mai_reader.header().index_type, MAIIndexType::Fasta as u8);

    // 1. Unique search
    let results = search_fasta(&reader, &mai_reader, "P12345", &IndexOptions::default()).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].filename, "human.fasta");
    assert_eq!(results[0].metadata.get("id").unwrap(), "P12345");
    assert_eq!(results[0].metadata.get("seq_len").unwrap(), "20");

    // 2. Shared accession query (matches in both files)
    let results_shared = search_fasta(&reader, &mai_reader, "AF-A0A022R2B6-F1", &IndexOptions::default()).unwrap();
    assert_eq!(results_shared.len(), 2);

    // 3. Qualified query to disambiguate
    let results_qualified = search_fasta(&reader, &mai_reader, "mouse.fa:AF-A0A022R2B6-F1", &IndexOptions::default()).unwrap();
    assert_eq!(results_qualified.len(), 1);
    assert_eq!(results_qualified[0].filename, "mouse.fa");

    // 4. File-scoped iteration
    let mut file_opts = IndexOptions::default();
    file_opts.params.insert("file".to_string(), "human.fasta".to_string());
    let results_iter = search_fasta(&reader, &mai_reader, "", &file_opts).unwrap();
    assert_eq!(results_iter.len(), 2);

    let _ = fs::remove_dir_all(&test_dir);
}
