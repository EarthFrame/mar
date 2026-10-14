use mar_core::delta::{create_delta_archives, create_delta_dir, DeltaJoin};
use mar_core::reader::MarReader;
use mar_core::writer::{MarWriter, WriteOptions};
use std::fs;
use std::path::Path;

#[test]
fn test_delta_archives_right_join() {
    let test_dir = std::env::temp_dir().join("mar_delta_test_right");
    let _ = fs::remove_dir_all(&test_dir);
    fs::create_dir_all(&test_dir).unwrap();

    let base_mar = test_dir.join("base.mar");
    let target_mar = test_dir.join("target.mar");
    let delta_mar = test_dir.join("delta.mar");

    // Create base archive: a.txt, b.txt, c.txt
    let mut writer = MarWriter::new(base_mar.to_str().unwrap(), WriteOptions::default());
    writer.add_memory("a.txt", b"file a original", 0o644, 1000);
    writer.add_memory("b.txt", b"file b original", 0o644, 2000);
    writer.add_memory("c.txt", b"file c original", 0o644, 3000);
    writer.finish().unwrap();

    // Create target archive:
    // a.txt (identical)
    // b.txt (modified content and newer timestamp)
    // c.txt (deleted / omitted)
    // d.txt (added)
    let mut writer = MarWriter::new(target_mar.to_str().unwrap(), WriteOptions::default());
    writer.add_memory("a.txt", b"file a original", 0o644, 1000);
    writer.add_memory("b.txt", b"file b modified data!", 0o644, 2500);
    writer.add_memory("d.txt", b"file d brand new", 0o644, 4000);
    writer.finish().unwrap();

    let src_reader = MarReader::open(base_mar.to_str().unwrap()).unwrap();
    let tgt_reader = MarReader::open(target_mar.to_str().unwrap()).unwrap();

    // Default right join: target perspective (Added + Modified)
    let stats = create_delta_archives(
        &src_reader,
        &tgt_reader,
        delta_mar.to_str().unwrap(),
        DeltaJoin::Right,
        WriteOptions::default(),
    )
    .unwrap();

    assert_eq!(stats.files_added, 1);
    assert_eq!(stats.files_modified, 1);
    assert_eq!(stats.files_deleted, 1);
    assert_eq!(stats.total_entries, 2);

    let delta_reader = MarReader::open(delta_mar.to_str().unwrap()).unwrap();
    assert_eq!(delta_reader.file_count(), 2);
    assert!(delta_reader.find_file("a.txt").is_none());
    assert!(delta_reader.find_file("c.txt").is_none());

    let b_data = delta_reader.read_file_by_name("b.txt").unwrap();
    assert_eq!(b_data, b"file b modified data!");
    let (b_idx, _) = delta_reader.find_file("b.txt").unwrap();
    let b_posix = delta_reader.get_posix_meta(b_idx).unwrap();
    assert_eq!(b_posix.mtime, 2500);

    let d_data = delta_reader.read_file_by_name("d.txt").unwrap();
    assert_eq!(d_data, b"file d brand new");
    let (d_idx, _) = delta_reader.find_file("d.txt").unwrap();
    let d_posix = delta_reader.get_posix_meta(d_idx).unwrap();
    assert_eq!(d_posix.mtime, 4000);

    let _ = fs::remove_dir_all(&test_dir);
}

#[test]
fn test_delta_archives_left_and_outer_join() {
    let test_dir = std::env::temp_dir().join("mar_delta_test_joins");
    let _ = fs::remove_dir_all(&test_dir);
    fs::create_dir_all(&test_dir).unwrap();

    let base_mar = test_dir.join("base.mar");
    let target_mar = test_dir.join("target.mar");

    let mut writer = MarWriter::new(base_mar.to_str().unwrap(), WriteOptions::default());
    writer.add_memory("a.txt", b"file a", 0o644, 1000);
    writer.add_memory("b.txt", b"file b base", 0o644, 2000);
    writer.add_memory("c.txt", b"file c base", 0o644, 3000);
    writer.finish().unwrap();

    let mut writer = MarWriter::new(target_mar.to_str().unwrap(), WriteOptions::default());
    writer.add_memory("a.txt", b"file a", 0o644, 1000);
    writer.add_memory("b.txt", b"file b target", 0o644, 2500);
    writer.add_memory("d.txt", b"file d target", 0o644, 4000);
    writer.finish().unwrap();

    let src_reader = MarReader::open(base_mar.to_str().unwrap()).unwrap();
    let tgt_reader = MarReader::open(target_mar.to_str().unwrap()).unwrap();

    // 1. Left join: Source perspective (Deleted + Modified in source)
    let delta_left = test_dir.join("delta_left.mar");
    let stats_left = create_delta_archives(
        &src_reader,
        &tgt_reader,
        delta_left.to_str().unwrap(),
        DeltaJoin::Left,
        WriteOptions::default(),
    )
    .unwrap();

    assert_eq!(stats_left.total_entries, 2);
    let r_left = MarReader::open(delta_left.to_str().unwrap()).unwrap();
    assert!(r_left.find_file("b.txt").is_some());
    assert!(r_left.find_file("c.txt").is_some());
    assert!(r_left.find_file("a.txt").is_none());
    assert!(r_left.find_file("d.txt").is_none());
    // In left join, b.txt comes from base
    assert_eq!(r_left.read_file_by_name("b.txt").unwrap(), b"file b base");

    // 2. Outer join: Symmetric difference (Added & Modified in target + Deleted in source)
    let delta_outer = test_dir.join("delta_outer.mar");
    let stats_outer = create_delta_archives(
        &src_reader,
        &tgt_reader,
        delta_outer.to_str().unwrap(),
        DeltaJoin::Outer,
        WriteOptions::default(),
    )
    .unwrap();

    assert_eq!(stats_outer.total_entries, 3);
    let r_outer = MarReader::open(delta_outer.to_str().unwrap()).unwrap();
    assert!(r_outer.find_file("b.txt").is_some());
    assert!(r_outer.find_file("c.txt").is_some());
    assert!(r_outer.find_file("d.txt").is_some());
    assert!(r_outer.find_file("a.txt").is_none());
    assert_eq!(r_outer.read_file_by_name("b.txt").unwrap(), b"file b target");
    assert_eq!(r_outer.read_file_by_name("c.txt").unwrap(), b"file c base");
    assert_eq!(r_outer.read_file_by_name("d.txt").unwrap(), b"file d target");

    let _ = fs::remove_dir_all(&test_dir);
}

#[test]
fn test_delta_archive_to_directory_snapshot() {
    let test_dir = std::env::temp_dir().join("mar_delta_test_dir");
    let _ = fs::remove_dir_all(&test_dir);
    fs::create_dir_all(&test_dir).unwrap();

    let base_mar = test_dir.join("base.mar");
    let live_dir = test_dir.join("data_tree");
    fs::create_dir_all(&live_dir).unwrap();

    // Setup base archive
    let mut writer = MarWriter::new(base_mar.to_str().unwrap(), WriteOptions::default());
    writer.add_memory("f1.txt", b"constant text", 0o644, 1000);
    writer.add_memory("f2.txt", b"v1 data", 0o644, 2000);
    writer.add_memory("f3.txt", b"doomed to be deleted", 0o644, 3000);
    writer.finish().unwrap();

    // Setup live filesystem directory
    fs::write(live_dir.join("f1.txt"), b"constant text").unwrap();
    #[cfg(unix)]
    {
        use std::ffi::CString;
        let c_path = CString::new(live_dir.join("f1.txt").to_str().unwrap()).unwrap();
        let times = [
            libc::timespec { tv_sec: 1000, tv_nsec: 0 },
            libc::timespec { tv_sec: 1000, tv_nsec: 0 },
        ];
        unsafe {
            libc::utimensat(libc::AT_FDCWD, c_path.as_ptr(), times.as_ptr(), 0);
        }
    }
    fs::write(live_dir.join("f2.txt"), b"v2 updated data!").unwrap();
    fs::write(live_dir.join("f4.txt"), b"brand new file").unwrap();

    let src_reader = MarReader::open(base_mar.to_str().unwrap()).unwrap();
    let delta_mar = test_dir.join("delta_snap.mar");

    let stats = create_delta_dir(
        &src_reader,
        Path::new(&live_dir),
        delta_mar.to_str().unwrap(),
        DeltaJoin::Right,
        WriteOptions::default(),
        None,
    )
    .unwrap();

    assert_eq!(stats.files_added, 1);
    assert_eq!(stats.files_modified, 1);
    assert_eq!(stats.files_deleted, 1);
    assert_eq!(stats.total_entries, 2);

    let delta_reader = MarReader::open(delta_mar.to_str().unwrap()).unwrap();
    assert_eq!(delta_reader.file_count(), 2);
    assert!(delta_reader.find_file("f1.txt").is_none());
    assert!(delta_reader.find_file("f3.txt").is_none());

    assert_eq!(delta_reader.read_file_by_name("f2.txt").unwrap(), b"v2 updated data!");
    assert_eq!(delta_reader.read_file_by_name("f4.txt").unwrap(), b"brand new file");

    let _ = fs::remove_dir_all(&test_dir);
}

#[test]
fn test_delta_preserves_posix_metadata_and_mtime() {
    let test_dir = std::env::temp_dir().join("mar_delta_test_posix");
    let _ = fs::remove_dir_all(&test_dir);
    fs::create_dir_all(&test_dir).unwrap();

    let base_mar = test_dir.join("base.mar");
    let target_mar = test_dir.join("target.mar");
    let delta_mar = test_dir.join("delta.mar");

    // Base archive: base file with mode 0o644 and mtime 1500000000
    let mut writer = MarWriter::new(base_mar.to_str().unwrap(), WriteOptions::default());
    writer.add_memory("doc.txt", b"version 1.0", 0o644, 1500000000);
    writer.finish().unwrap();

    // Target archive: doc.txt modified with mode 0o755 and mtime 1600000000
    let mut writer = MarWriter::new(target_mar.to_str().unwrap(), WriteOptions::default());
    writer.add_memory("doc.txt", b"version 2.0 with updates", 0o755, 1600000000);
    writer.finish().unwrap();

    let src = MarReader::open(base_mar.to_str().unwrap()).unwrap();
    let tgt = MarReader::open(target_mar.to_str().unwrap()).unwrap();

    create_delta_archives(
        &src,
        &tgt,
        delta_mar.to_str().unwrap(),
        DeltaJoin::Right,
        WriteOptions::default(),
    )
    .unwrap();

    let delta_reader = MarReader::open(delta_mar.to_str().unwrap()).unwrap();
    assert_eq!(delta_reader.file_count(), 1);

    let (idx, _) = delta_reader.find_file("doc.txt").unwrap();
    let posix = delta_reader.get_posix_meta(idx).unwrap();
    assert_eq!(posix.mode, 0o755);
    assert_eq!(posix.mtime, 1600000000);

    // Extract delta and verify extraction maintains file attributes
    let ext_dir = test_dir.join("extracted");
    delta_reader.extract(ext_dir.to_str().unwrap(), None).unwrap();

    let ext_doc = ext_dir.join("doc.txt");
    assert_eq!(fs::read(&ext_doc).unwrap(), b"version 2.0 with updates");

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perm = fs::metadata(&ext_doc).unwrap().permissions();
        assert_eq!(perm.mode() & 0o777, 0o755);
        let mtime = fs::metadata(&ext_doc).unwrap().modified().unwrap()
            .duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64;
        assert_eq!(mtime, 1600000000);
    }

    let _ = fs::remove_dir_all(&test_dir);
}

#[test]
fn test_delta_subdirectories_and_nested_trees() {
    let test_dir = std::env::temp_dir().join("mar_delta_test_nested");
    let _ = fs::remove_dir_all(&test_dir);
    fs::create_dir_all(&test_dir).unwrap();

    let base_mar = test_dir.join("base.mar");
    let target_mar = test_dir.join("target.mar");
    let delta_mar = test_dir.join("delta.mar");

    // Base archive with nested structure
    let mut writer = MarWriter::new(base_mar.to_str().unwrap(), WriteOptions::default());
    writer.add_directory_entry("dirA", 0o755, 1000);
    writer.add_memory("dirA/sub1.txt", b"sub1 v1", 0o644, 1000);
    writer.add_memory("dirA/sub2.txt", b"sub2 constant", 0o644, 1000);
    writer.finish().unwrap();

    // Target archive with modified sub1.txt and new dirB/new.txt
    let mut writer = MarWriter::new(target_mar.to_str().unwrap(), WriteOptions::default());
    writer.add_directory_entry("dirA", 0o755, 1000);
    writer.add_memory("dirA/sub1.txt", b"sub1 v2 updated", 0o644, 2000);
    writer.add_memory("dirA/sub2.txt", b"sub2 constant", 0o644, 1000);
    writer.add_directory_entry("dirB", 0o755, 2000);
    writer.add_memory("dirB/new.txt", b"new nested file", 0o644, 2000);
    writer.finish().unwrap();

    let src = MarReader::open(base_mar.to_str().unwrap()).unwrap();
    let tgt = MarReader::open(target_mar.to_str().unwrap()).unwrap();

    let stats = create_delta_archives(
        &src,
        &tgt,
        delta_mar.to_str().unwrap(),
        DeltaJoin::Right,
        WriteOptions::default(),
    )
    .unwrap();

    let delta_reader = MarReader::open(delta_mar.to_str().unwrap()).unwrap();
    assert!(delta_reader.find_file("dirA/sub1.txt").is_some());
    assert!(delta_reader.find_file("dirB/new.txt").is_some());
    assert!(delta_reader.find_file("dirA/sub2.txt").is_none());

    assert_eq!(stats.files_added, 2); // dirB and dirB/new.txt
    assert_eq!(stats.files_modified, 1); // dirA/sub1.txt

    let _ = fs::remove_dir_all(&test_dir);
}
