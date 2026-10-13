use mar_core::reader::MarReader;
use mar_core::writer::{BlockMode, MarWriter, WriteOptions};
use std::fs;

#[test]
fn test_block_modes_and_packing() {
    let test_dir = std::env::temp_dir().join("mar_block_modes_test");
    let _ = fs::remove_dir_all(&test_dir);
    fs::create_dir_all(&test_dir).unwrap();

    let f1_path = test_dir.join("f1.txt");
    let f2_path = test_dir.join("f2.txt");
    let f3_path = test_dir.join("f3.txt");

    fs::write(&f1_path, b"Hello from file 1!").unwrap();
    fs::write(&f2_path, b"File 2 contents are slightly longer but still small.").unwrap();
    let large_data = vec![b'X'; 16384]; // 16KB
    fs::write(&f3_path, &large_data).unwrap();

    for mode in [BlockMode::Shared, BlockMode::Multi, BlockMode::Single] {
        let mar_name = format!("test_{:?}.mar", mode);
        let mar_path = test_dir.join(&mar_name);

        let mut opts = WriteOptions::default();
        opts.block_mode = mode;
        opts.block_size = 4096; // 4KB blocks
        opts.multiblock = mode != BlockMode::Single;

        let mut writer = MarWriter::new(mar_path.to_str().unwrap(), opts);
        writer.add_file(f1_path.to_str().unwrap(), "f1.txt").unwrap();
        writer.add_file(f2_path.to_str().unwrap(), "f2.txt").unwrap();
        writer.add_file(f3_path.to_str().unwrap(), "f3.txt").unwrap();
        writer.finish().unwrap();

        let reader = MarReader::open(mar_path.to_str().unwrap()).unwrap();
        assert_eq!(reader.file_count(), 3);

        let c1 = reader.read_file_by_name("f1.txt").unwrap();
        assert_eq!(c1, b"Hello from file 1!");

        let c2 = reader.read_file_by_name("f2.txt").unwrap();
        assert_eq!(c2, b"File 2 contents are slightly longer but still small.");

        let c3 = reader.read_file_by_name("f3.txt").unwrap();
        assert_eq!(c3, large_data);

        // Verify extraction
        let ext_dir = test_dir.join(format!("ext_{:?}", mode));
        reader.extract(ext_dir.to_str().unwrap(), None).unwrap();
        assert_eq!(fs::read(ext_dir.join("f1.txt")).unwrap(), b"Hello from file 1!");
        assert_eq!(fs::read(ext_dir.join("f2.txt")).unwrap(), b"File 2 contents are slightly longer but still small.");
        assert_eq!(fs::read(ext_dir.join("f3.txt")).unwrap(), large_data);

        if mode == BlockMode::Shared {
            // Check that f1 and f2 share block 0
            let (idx1, _) = reader.find_file("f1.txt").unwrap();
            let (idx2, _) = reader.find_file("f2.txt").unwrap();
            let spans1 = reader.get_file_spans(idx1);
            let spans2 = reader.get_file_spans(idx2);
            assert_eq!(spans1.len(), 1);
            assert_eq!(spans2.len(), 1);
            assert_eq!(spans1[0].block_id, spans2[0].block_id);
            assert_eq!(spans1[0].offset_in_block, 0);
            assert_eq!(spans2[0].offset_in_block, spans1[0].length);
        }
    }
}
