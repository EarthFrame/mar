use std::fs;
use std::path::PathBuf;
use std::process::{Command, Output};

const MAR_BIN: &str = env!("CARGO_BIN_EXE_mar");

fn run_mar(args: &[&str]) -> Output {
    Command::new(MAR_BIN)
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("failed to execute {} with args {:?}: {}", MAR_BIN, args, e))
}

fn run_mar_success(args: &[&str]) -> Output {
    let out = run_mar(args);
    if !out.status.success() {
        panic!(
            "Command {:?} failed with status {:?}.\nStdout: {}\nStderr: {}",
            args,
            out.status.code(),
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }
    out
}

fn temp_test_dir(prefix: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("mar_cli_test_{}_{}", prefix, std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

// ============================================================================
// Core Lifecycle Tests: create, extract, list, get, cat
// ============================================================================

#[test]
fn test_cli_create_and_extract_basic() {
    let dir = temp_test_dir("create_extract");
    let src_dir = dir.join("src");
    fs::create_dir_all(src_dir.join("sub")).unwrap();
    fs::write(src_dir.join("hello.txt"), b"Hello World").unwrap();
    fs::write(src_dir.join("sub").join("nested.txt"), b"Nested content").unwrap();

    let mar_path = dir.join("test.mar");
    let out_dir = dir.join("out");

    // 1. Create archive
    let out = run_mar_success(&[
        "create",
        "-f",
        mar_path.to_str().unwrap(),
        src_dir.to_str().unwrap(),
    ]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("Created:"));
    assert!(mar_path.is_file());

    // 2. Extract archive
    run_mar_success(&[
        "extract",
        mar_path.to_str().unwrap(),
        "-o",
        out_dir.to_str().unwrap(),
    ]);

    // Check extracted files
    let ext_hello = out_dir.join("src").join("hello.txt");
    let ext_nested = out_dir.join("src").join("sub").join("nested.txt");
    assert_eq!(fs::read(&ext_hello).unwrap(), b"Hello World");
    assert_eq!(fs::read(&ext_nested).unwrap(), b"Nested content");

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_cli_create_codecs_checksums_and_block_modes() {
    let dir = temp_test_dir("codecs_blocks");
    let f1 = dir.join("file1.txt");
    let f2 = dir.join("file2.txt");
    fs::write(&f1, b"Data payload 1").unwrap();
    fs::write(&f2, b"Data payload 2 repeated ".repeat(50)).unwrap();

    let combinations = [
        ("zstd", "xxhash3", "--pack-files"),
        ("lz4", "blake3", "--single-file"),
        ("none", "crc32c", "--multiblock"),
        ("gzip", "xxhash32", "--block-mode"),
    ];

    for (i, (comp, csum, mode)) in combinations.iter().enumerate() {
        let mar_file = dir.join(format!("test_{}.mar", i));
        let mut args = vec![
            "create",
            "-f",
            "-c",
            comp,
            "--checksum",
            csum,
        ];
        if *mode == "--block-mode" {
            args.push("--block-mode");
            args.push("shared");
        } else {
            args.push(mode);
        }
        args.push(mar_file.to_str().unwrap());
        args.push(f1.to_str().unwrap());
        args.push(f2.to_str().unwrap());

        run_mar_success(&args);

        // Validate created archive
        run_mar_success(&["validate", mar_file.to_str().unwrap()]);
    }

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_cli_create_force_and_files_from() {
    let dir = temp_test_dir("force_files_from");
    let f1 = dir.join("a.txt");
    let f2 = dir.join("b.txt");
    let f3 = dir.join("c.txt");
    fs::write(&f1, b"A").unwrap();
    fs::write(&f2, b"B").unwrap();
    fs::write(&f3, b"C").unwrap();

    let mar_path = dir.join("test.mar");
    run_mar_success(&["create", mar_path.to_str().unwrap(), f1.to_str().unwrap()]);

    // Overwrite without -f should fail with exit code 2 (usage/error)
    let fail_out = run_mar(&["create", mar_path.to_str().unwrap(), f2.to_str().unwrap()]);
    assert_ne!(fail_out.status.code(), Some(0));

    // Overwrite with -f should succeed
    run_mar_success(&["create", "-f", mar_path.to_str().unwrap(), f2.to_str().unwrap()]);

    // Test -T / --files-from
    let list_file = dir.join("files.txt");
    fs::write(&list_file, format!("{}\n{}\n", f1.to_str().unwrap(), f3.to_str().unwrap())).unwrap();

    let mar_from_list = dir.join("list.mar");
    run_mar_success(&[
        "create",
        "-f",
        "-T",
        list_file.to_str().unwrap(),
        mar_from_list.to_str().unwrap(),
    ]);

    let list_out = run_mar_success(&["list", mar_from_list.to_str().unwrap()]);
    let stdout = String::from_utf8_lossy(&list_out.stdout);
    assert!(stdout.contains("a.txt"));
    assert!(stdout.contains("c.txt"));
    assert!(!stdout.contains("b.txt"));

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_cli_extract_strip_components_and_selective() {
    let dir = temp_test_dir("extract_strip");
    let src = dir.join("deep").join("path").join("to");
    fs::create_dir_all(&src).unwrap();
    fs::write(src.join("target.txt"), b"Deep content").unwrap();
    fs::write(src.join("other.txt"), b"Other content").unwrap();

    let mar_path = dir.join("deep.mar");
    run_mar_success(&["create", "-f", mar_path.to_str().unwrap(), dir.join("deep").to_str().unwrap()]);

    // 1. Selective extraction
    let selective_dir = dir.join("selective");
    run_mar_success(&[
        "extract",
        mar_path.to_str().unwrap(),
        "-o",
        selective_dir.to_str().unwrap(),
        "deep/path/to/target.txt",
    ]);
    assert!(selective_dir.join("deep/path/to/target.txt").is_file());
    assert!(!selective_dir.join("deep/path/to/other.txt").exists());

    // 2. Strip components
    let stripped_dir = dir.join("stripped");
    run_mar_success(&[
        "extract",
        mar_path.to_str().unwrap(),
        "--strip-components",
        "2",
        "-o",
        stripped_dir.to_str().unwrap(),
    ]);
    assert!(stripped_dir.join("to/target.txt").is_file());
    assert!(stripped_dir.join("to/other.txt").is_file());

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_cli_list_table_json_and_nometa() {
    let dir = temp_test_dir("list_formats");
    let f1 = dir.join("alpha.txt");
    let f2 = dir.join("beta.txt");
    fs::write(&f1, b"Alpha 123").unwrap();
    fs::write(&f2, b"Beta 456789").unwrap();

    let mar_path = dir.join("archive.mar");
    run_mar_success(&["create", "-f", mar_path.to_str().unwrap(), f1.to_str().unwrap(), f2.to_str().unwrap()]);

    // 1. Standard list
    let out_plain = run_mar_success(&["list", mar_path.to_str().unwrap()]);
    let plain_txt = String::from_utf8_lossy(&out_plain.stdout);
    assert!(plain_txt.contains("alpha.txt"));
    assert!(plain_txt.contains("beta.txt"));

    // 2. Table list
    let out_table = run_mar_success(&["list", "--table", "--header", mar_path.to_str().unwrap()]);
    let table_txt = String::from_utf8_lossy(&out_table.stdout);
    assert!(table_txt.contains("alpha.txt"));
    assert!(table_txt.contains("beta.txt"));

    // 3. JSON format
    let out_json = run_mar_success(&["list", "--format", "json", mar_path.to_str().unwrap()]);
    let json_txt = String::from_utf8_lossy(&out_json.stdout);
    assert!(json_txt.trim().starts_with('['));
    assert!(json_txt.trim().ends_with(']'));
    assert!(json_txt.contains("\"name\":"));
    assert!(json_txt.contains("alpha.txt"));

    // 4. No-meta list
    let out_nometa = run_mar_success(&["list", "--no-meta", mar_path.to_str().unwrap()]);
    let nometa_txt = String::from_utf8_lossy(&out_nometa.stdout);
    assert!(nometa_txt.contains("alpha.txt"));

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_cli_get_and_cat_commands() {
    let dir = temp_test_dir("get_cat");
    let src_dir = dir.join("src");
    fs::create_dir_all(&src_dir).unwrap();
    let f1 = src_dir.join("doc.txt");
    let f2 = src_dir.join("notes.txt");
    fs::write(&f1, b"Documentation content").unwrap();
    fs::write(&f2, b"Notebook entries").unwrap();

    let mar_path = dir.join("archive.mar");
    run_mar_success(&["create", "-f", mar_path.to_str().unwrap(), src_dir.to_str().unwrap()]);

    // 1. get to stdout
    let get_out = run_mar_success(&["get", "-c", mar_path.to_str().unwrap(), "src/doc.txt"]);
    assert_eq!(get_out.stdout, b"Documentation content");

    // 2. get -o to directory
    let get_dir = dir.join("get_target");
    run_mar_success(&["get", "-o", get_dir.to_str().unwrap(), mar_path.to_str().unwrap(), "src/notes.txt"]);
    assert_eq!(fs::read(get_dir.join("src/notes.txt")).unwrap(), b"Notebook entries");

    // 3. get with --json
    let get_json = run_mar_success(&["get", "--json", mar_path.to_str().unwrap(), "src/doc.txt"]);
    let json_str = String::from_utf8_lossy(&get_json.stdout);
    assert!(json_str.contains("\"filename\": \"src/doc.txt\""));
    assert!(json_str.contains("\"contents\":"));

    // 4. cat to stdout
    let cat_out = run_mar_success(&["cat", mar_path.to_str().unwrap(), "src/notes.txt"]);
    assert_eq!(cat_out.stdout, b"Notebook entries");

    // 5. cat -o to file
    let cat_file = dir.join("cat_out.txt");
    run_mar_success(&["cat", "-o", cat_file.to_str().unwrap(), mar_path.to_str().unwrap(), "src/doc.txt"]);
    assert_eq!(fs::read(&cat_file).unwrap(), b"Documentation content");

    // 6. cat --fmt json --all
    let cat_json = run_mar_success(&["cat", "--all", "--fmt", "json", mar_path.to_str().unwrap()]);
    let cat_json_str = String::from_utf8_lossy(&cat_json.stdout);
    assert!(cat_json_str.contains("\"contents\":"));
    assert!(cat_json_str.contains("src/doc.txt"));

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_cli_diff_command() {
    let dir = temp_test_dir("diff");
    let src1 = dir.join("v1").join("data");
    let src2 = dir.join("v2").join("data");
    fs::create_dir_all(&src1).unwrap();
    fs::create_dir_all(&src2).unwrap();

    fs::write(src1.join("same.txt"), b"Same content").unwrap();
    fs::write(src1.join("deleted.txt"), b"Will be deleted").unwrap();
    fs::write(src1.join("modified.txt"), b"Original text").unwrap();

    fs::write(src2.join("same.txt"), b"Same content").unwrap();
    fs::write(src2.join("added.txt"), b"Newly added").unwrap();
    fs::write(src2.join("modified.txt"), b"Updated text modified").unwrap();

    let mar1 = dir.join("v1.mar");
    let mar2 = dir.join("v2.mar");
    run_mar_success(&["create", "-f", mar1.to_str().unwrap(), src1.to_str().unwrap()]);
    run_mar_success(&["create", "-f", mar2.to_str().unwrap(), src2.to_str().unwrap()]);

    // 1. Identical diff
    let same_diff = run_mar_success(&["diff", mar1.to_str().unwrap(), mar1.to_str().unwrap()]);
    let same_txt = String::from_utf8_lossy(&same_diff.stdout);
    assert!(same_txt.contains("Unchanged:"));
    assert!(same_txt.contains("Added:          0"));
    assert!(same_txt.contains("Deleted:        0"));
    assert!(same_txt.contains("Modified:       0"));

    // 2. Summary diff
    let summary_diff = run_mar_success(&["diff", mar1.to_str().unwrap(), mar2.to_str().unwrap()]);
    let sum_txt = String::from_utf8_lossy(&summary_diff.stdout);
    assert!(sum_txt.contains("Added:          1"));
    assert!(sum_txt.contains("Deleted:        1"));
    assert!(sum_txt.contains("Modified:       1"));

    // 3. Detailed delta diff (-d)
    let delta_diff = run_mar_success(&["diff", "-d", mar1.to_str().unwrap(), mar2.to_str().unwrap()]);
    let d_txt = String::from_utf8_lossy(&delta_diff.stdout);
    assert!(d_txt.contains("A  data/added.txt"));
    assert!(d_txt.contains("D  data/deleted.txt"));
    assert!(d_txt.contains("M  data/modified.txt"));

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_cli_delta_command() {
    let dir = temp_test_dir("delta");
    let src1 = dir.join("v1").join("data");
    let src2 = dir.join("v2").join("data");
    fs::create_dir_all(&src1).unwrap();
    fs::create_dir_all(&src2).unwrap();

    fs::write(src1.join("base.txt"), b"Base content").unwrap();
    fs::write(src1.join("mod.txt"), b"Version 1").unwrap();

    fs::write(src2.join("base.txt"), b"Base content").unwrap();
    fs::write(src2.join("mod.txt"), b"Version 2 updated").unwrap();
    fs::write(src2.join("extra.txt"), b"Extra file").unwrap();

    let mar1 = dir.join("v1.mar");
    let mar2 = dir.join("v2.mar");
    run_mar_success(&["create", "-f", mar1.to_str().unwrap(), src1.to_str().unwrap()]);
    run_mar_success(&["create", "-f", mar2.to_str().unwrap(), src2.to_str().unwrap()]);

    // 1. Archive-to-archive delta (default join: right)
    let delta_arc = dir.join("delta_arc.mar");
    run_mar_success(&[
        "delta",
        mar1.to_str().unwrap(),
        mar2.to_str().unwrap(),
        "-o",
        delta_arc.to_str().unwrap(),
        "-f",
    ]);
    run_mar_success(&["validate", delta_arc.to_str().unwrap()]);
    let list_arc = run_mar_success(&["list", delta_arc.to_str().unwrap()]);
    let list_arc_txt = String::from_utf8_lossy(&list_arc.stdout);
    assert!(list_arc_txt.contains("mod.txt"));
    assert!(list_arc_txt.contains("extra.txt"));
    assert!(!list_arc_txt.contains("base.txt"));

    // 2. Archive-to-directory delta
    let delta_dir_out = dir.join("delta_dir.mar");
    run_mar_success(&[
        "delta",
        mar1.to_str().unwrap(),
        src2.to_str().unwrap(),
        "-o",
        delta_dir_out.to_str().unwrap(),
        "-f",
    ]);
    run_mar_success(&["validate", delta_dir_out.to_str().unwrap()]);

    // 3. Different join options: --join left and --join outer
    let delta_left = dir.join("delta_left.mar");
    run_mar_success(&[
        "delta",
        mar1.to_str().unwrap(),
        mar2.to_str().unwrap(),
        "--join",
        "left",
        "-o",
        delta_left.to_str().unwrap(),
        "-f",
    ]);
    run_mar_success(&["validate", delta_left.to_str().unwrap()]);

    let delta_outer = dir.join("delta_outer.mar");
    run_mar_success(&[
        "delta",
        mar1.to_str().unwrap(),
        mar2.to_str().unwrap(),
        "--join",
        "outer",
        "-o",
        delta_outer.to_str().unwrap(),
        "-f",
    ]);
    run_mar_success(&["validate", delta_outer.to_str().unwrap()]);

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_cli_slice_command() {
    let dir = temp_test_dir("slice");
    let src = dir.join("src");
    fs::create_dir_all(&src).unwrap();

    fs::write(src.join("data.txt"), b"Data text").unwrap();
    fs::write(src.join("readme.md"), b"Markdown doc").unwrap();
    fs::write(src.join("script.rs"), b"fn main() {}").unwrap();

    let mar_full = dir.join("full.mar");
    run_mar_success(&["create", "-f", mar_full.to_str().unwrap(), src.to_str().unwrap()]);

    // 1. Slice by include pattern -i
    let mar_sliced = dir.join("sliced.mar");
    run_mar_success(&[
        "slice",
        mar_full.to_str().unwrap(),
        "-o",
        mar_sliced.to_str().unwrap(),
        "-i",
        "*.txt",
        "-f",
    ]);
    run_mar_success(&["validate", mar_sliced.to_str().unwrap()]);
    let list_txt = run_mar_success(&["list", mar_sliced.to_str().unwrap()]);
    let s = String::from_utf8_lossy(&list_txt.stdout);
    assert!(s.contains("data.txt"));
    assert!(!s.contains("readme.md"));
    assert!(!s.contains("script.rs"));

    // 2. Slice by exclude pattern -x
    let mar_ex = dir.join("sliced_ex.mar");
    run_mar_success(&[
        "slice",
        mar_full.to_str().unwrap(),
        "-o",
        mar_ex.to_str().unwrap(),
        "-x",
        "*.md",
        "-f",
    ]);
    run_mar_success(&["validate", mar_ex.to_str().unwrap()]);
    let list_ex = run_mar_success(&["list", mar_ex.to_str().unwrap()]);
    let s_ex = String::from_utf8_lossy(&list_ex.stdout);
    assert!(s_ex.contains("data.txt"));
    assert!(s_ex.contains("script.rs"));
    assert!(!s_ex.contains("readme.md"));

    // 3. Slice using files-from list
    let list_file = dir.join("slice_targets.txt");
    fs::write(&list_file, b"src/data.txt\nsrc/script.rs\n").unwrap();
    let mar_from = dir.join("sliced_from.mar");
    run_mar_success(&[
        "slice",
        mar_full.to_str().unwrap(),
        "-o",
        mar_from.to_str().unwrap(),
        "-T",
        list_file.to_str().unwrap(),
        "-f",
    ]);
    run_mar_success(&["validate", mar_from.to_str().unwrap()]);
    let list_from = run_mar_success(&["list", mar_from.to_str().unwrap()]);
    let s_from = String::from_utf8_lossy(&list_from.stdout);
    assert!(s_from.contains("data.txt"));
    assert!(s_from.contains("script.rs"));
    assert!(!s_from.contains("readme.md"));

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_cli_redact_command() {
    let dir = temp_test_dir("redact");
    let src = dir.join("src");
    fs::create_dir_all(&src).unwrap();

    fs::write(src.join("confidential.txt"), b"Top secret classified content").unwrap();
    fs::write(src.join("public.txt"), b"Public open content").unwrap();

    let mar_orig = dir.join("orig.mar");
    run_mar_success(&[
        "create",
        "-f",
        "-c",
        "none",
        "--single-file",
        mar_orig.to_str().unwrap(),
        src.to_str().unwrap(),
    ]);

    // 1. Out-of-place redaction (-o)
    let mar_redacted = dir.join("redacted.mar");
    run_mar_success(&[
        "redact",
        mar_orig.to_str().unwrap(),
        "-o",
        mar_redacted.to_str().unwrap(),
        "src/confidential.txt",
        "-f",
    ]);
    run_mar_success(&["validate", mar_redacted.to_str().unwrap()]);

    // Verify confidential content is redacted (empty output)
    let cat_conf = run_mar_success(&["cat", mar_redacted.to_str().unwrap(), "src/confidential.txt"]);
    assert_eq!(cat_conf.stdout, b"");

    // Verify public content is intact
    let cat_pub = run_mar_success(&["cat", mar_redacted.to_str().unwrap(), "src/public.txt"]);
    assert_eq!(cat_pub.stdout, b"Public open content");

    // 2. In-place redaction (-I)
    let mar_inplace = dir.join("inplace.mar");
    fs::copy(&mar_orig, &mar_inplace).unwrap();
    run_mar_success(&["redact", "-I", mar_inplace.to_str().unwrap(), "src/confidential.txt"]);
    run_mar_success(&["validate", mar_inplace.to_str().unwrap()]);

    let cat_in_conf = run_mar_success(&["cat", mar_inplace.to_str().unwrap(), "src/confidential.txt"]);
    assert_eq!(cat_in_conf.stdout, b"");

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_cli_validate_and_corrupt() {
    let dir = temp_test_dir("validate");
    let src = dir.join("src");
    fs::create_dir_all(&src).unwrap();

    fs::write(src.join("payload.bin"), vec![0xAB; 8192]).unwrap();
    let mar_path = dir.join("archive.mar");
    run_mar_success(&[
        "create",
        "-f",
        "-c",
        "none",
        mar_path.to_str().unwrap(),
        src.to_str().unwrap(),
        "--checksum",
        "crc32c",
    ]);

    // Healthy validation
    let valid_out = run_mar_success(&["validate", mar_path.to_str().unwrap()]);
    assert!(valid_out.status.success());

    // Healthy quiet validation (-q)
    let valid_q = run_mar_success(&["-q", "validate", mar_path.to_str().unwrap()]);
    assert!(valid_q.status.success());
    assert!(valid_q.stdout.is_empty());

    // Corrupt archive byte inside block payload
    let mut bytes = fs::read(&mar_path).unwrap();
    assert!(bytes.len() > 100);
    let tamper_idx = bytes.len() - 100;
    bytes[tamper_idx] ^= 0xFF;
    let corrupted_path = dir.join("corrupted.mar");
    fs::write(&corrupted_path, &bytes).unwrap();

    // Verify corrupted archive fails with exit code 65 (EXIT_INTEGRITY)
    let corrupt_out = run_mar(&["validate", corrupted_path.to_str().unwrap()]);
    assert_eq!(corrupt_out.status.code(), Some(65));

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_cli_header_and_hash_commands() {
    let dir = temp_test_dir("header_hash");
    let src = dir.join("src");
    fs::create_dir_all(&src).unwrap();

    fs::write(src.join("info.txt"), b"Some header test info").unwrap();
    let mar_path = dir.join("archive.mar");
    run_mar_success(&["create", "-f", mar_path.to_str().unwrap(), src.to_str().unwrap()]);

    // 1. Header command
    let header_out = run_mar_success(&["header", mar_path.to_str().unwrap()]);
    let header_txt = String::from_utf8_lossy(&header_out.stdout);
    assert!(header_txt.contains("MAR Archive Header Information:"));
    assert!(header_txt.contains("Format Version:"));
    assert!(header_txt.contains("Files:"));
    assert!(header_txt.contains("Blocks:"));

    // 2. Hash command - default xxhash64 (16 hex chars)
    let hash_out = run_mar_success(&["hash", mar_path.to_str().unwrap()]);
    let hash_txt = String::from_utf8_lossy(&hash_out.stdout).trim().to_string();
    assert_eq!(hash_txt.len(), 16);
    assert!(hash_txt.chars().all(|c| c.is_ascii_hexdigit()));

    // 3. Hash command - blake3 (64 hex chars)
    let blake_out = run_mar_success(&["hash", "--algo", "blake3", mar_path.to_str().unwrap()]);
    let blake_txt = String::from_utf8_lossy(&blake_out.stdout).trim().to_string();
    assert_eq!(blake_txt.len(), 64);
    assert!(blake_txt.chars().all(|c| c.is_ascii_hexdigit()));

    // 4. Hash command - md5 (32 hex chars)
    let md5_out = run_mar_success(&["hash", "--algo", "md5", mar_path.to_str().unwrap()]);
    let md5_txt = String::from_utf8_lossy(&md5_out.stdout).trim().to_string();
    assert_eq!(md5_txt.len(), 32);
    assert!(md5_txt.chars().all(|c| c.is_ascii_hexdigit()));

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_cli_version_and_flags() {
    // 1. --version (prints tool version)
    let v_long = run_mar_success(&["--version"]);
    let v_long_txt = String::from_utf8_lossy(&v_long.stdout).trim().to_string();
    assert!(!v_long_txt.is_empty());
    assert!(v_long_txt.contains('.'));

    // 2. version command (prints formatted mar version + spec)
    let v_cmd = run_mar_success(&["version"]);
    let v_cmd_txt = String::from_utf8_lossy(&v_cmd.stdout);
    assert!(v_cmd_txt.contains("mar version"));

    // 3. version -n / --mar-version (prints spec version)
    let v_n = run_mar_success(&["version", "-n"]);
    let v_n_txt = String::from_utf8_lossy(&v_n.stdout).trim().to_string();
    assert!(!v_n_txt.is_empty());
    assert!(v_n_txt.contains('.'));

    // 4. version -c / --capabilities
    let v_c = run_mar_success(&["version", "-c"]);
    let v_c_txt = String::from_utf8_lossy(&v_c.stdout);
    assert!(v_c_txt.contains("Compression algorithms:"));
    assert!(v_c_txt.contains("Checksums:"));
}

#[test]
fn test_cli_global_and_subcommand_help() {
    // 1. Global help
    let h1 = run_mar_success(&["-h"]);
    let h1_txt = String::from_utf8_lossy(&h1.stdout);
    assert!(h1_txt.contains("Usage:"));
    assert!(h1_txt.contains("Commands:"));

    let h2 = run_mar_success(&["--help"]);
    let h2_txt = String::from_utf8_lossy(&h2.stdout);
    assert!(h2_txt.contains("Usage:"));

    // 2. Per-command help for all core subcommands
    let subcommands = [
        "create", "extract", "list", "get", "cat", "diff", "delta", "slice", "redact",
        "validate", "header", "hash", "version",
    ];
    for cmd in &subcommands {
        let help_out = run_mar_success(&[cmd, "--help"]);
        let help_txt = String::from_utf8_lossy(&help_out.stdout);
        assert!(
            help_txt.contains("Usage:"),
            "Command '{} --help' did not contain usage info: {}",
            cmd,
            help_txt
        );

        let h_short_out = run_mar_success(&[cmd, "-h"]);
        let h_short_txt = String::from_utf8_lossy(&h_short_out.stdout);
        assert!(
            h_short_txt.contains("Usage:"),
            "Command '{} -h' did not contain usage info",
            cmd
        );
    }
}

#[test]
fn test_cli_usage_errors_exit_code_2() {
    // Unknown command
    let unknown_out = run_mar(&["nonexistent_command_xyz"]);
    assert_eq!(unknown_out.status.code(), Some(2));
    let err_txt = String::from_utf8_lossy(&unknown_out.stderr);
    assert!(err_txt.contains("Unknown command"));

    // Missing arguments across commands
    let cmds_missing_args = [
        "create", "extract", "list", "get", "cat", "diff", "delta", "slice", "redact",
        "validate", "header", "hash",
    ];
    for cmd in &cmds_missing_args {
        let out = run_mar(&[cmd]);
        assert_eq!(
            out.status.code(),
            Some(2),
            "Command '{}' without args should return exit code 2 (EXIT_USAGE)",
            cmd
        );
    }
}

#[test]
fn test_cli_enterprise_delegations() {
    let mar_bin = env!("CARGO_BIN_EXE_mar");

    // 1. Missing enterprise binary -> exit code 69 and license notification message
    for cmd in &["index", "search", "okf"] {
        let out = std::process::Command::new(mar_bin)
            .arg(cmd)
            .env("MAR_ENTERPRISE_BIN", "/nonexistent/enterprise-bin")
            .env("MAR_INDEX_BIN", "/nonexistent/index-bin")
            .output()
            .unwrap();

        assert_eq!(
            out.status.code(),
            Some(69),
            "Enterprise command '{}' without binary must exit with code 69",
            cmd
        );
        let err = String::from_utf8_lossy(&out.stderr);
        assert!(
            err.contains("requires the MAR Enterprise Extension (mar-enterprise)"),
            "Command '{}' missing enterprise banner in stderr: {}",
            cmd,
            err
        );
        assert!(err.contains("https://earthframe.com/enterprise"));
    }

    // 2. Mock enterprise binary invocation override
    let dir = temp_test_dir("mock_ent");
    let mock_bin = dir.join("mock-enterprise");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::write(
            &mock_bin,
            "#!/bin/sh\necho \"MOCK_ENTERPRISE: $@\"\nexit 0\n",
        )
        .unwrap();
        let mut perms = fs::metadata(&mock_bin).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&mock_bin, perms).unwrap();
    }

    for cmd in &["index", "search", "okf"] {
        let out = std::process::Command::new(mar_bin)
            .args(&[cmd, "--sample-flag", "sample-target"])
            .env("MAR_ENTERPRISE_BIN", &mock_bin)
            .output()
            .unwrap();

        assert_eq!(out.status.code(), Some(0));
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert!(stdout.contains(&format!("MOCK_ENTERPRISE: {} --sample-flag sample-target", cmd)));
    }

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_cli_slice_files_from_literal_and_brackets() {
    let dir = temp_test_dir("slice_literal");
    let src = dir.join("src");
    let bracket_dir = src.join("organized").join("03 - Business Code [BC]");
    let other_dir = src.join("organized").join("04 - Other Code [OC]");
    fs::create_dir_all(&bracket_dir).unwrap();
    fs::create_dir_all(&other_dir).unwrap();

    fs::write(bracket_dir.join("bc.1.pdf"), b"BC 1 content").unwrap();
    fs::write(bracket_dir.join("bc.2.pdf"), b"BC 2 content").unwrap();
    fs::write(other_dir.join("oc.1.pdf"), b"OC 1 content").unwrap();

    let orig_mar = dir.join("orig.mar");
    run_mar_success(&[
        "create",
        "-f",
        orig_mar.to_str().unwrap(),
        src.to_str().unwrap(),
    ]);

    // 1. Verify direct glob slicing still works on bracket directory
    let glob_mar = dir.join("glob.mar");
    run_mar_success(&[
        "slice",
        orig_mar.to_str().unwrap(),
        "-o",
        glob_mar.to_str().unwrap(),
        "**Business**",
    ]);
    let list_glob = run_mar_success(&["list", glob_mar.to_str().unwrap()]);
    let out_glob = String::from_utf8_lossy(&list_glob.stdout);
    assert!(out_glob.contains("bc.1.pdf"));
    assert!(out_glob.contains("bc.2.pdf"));
    assert!(!out_glob.contains("oc.1.pdf"));

    // 2. Test literal list file containing brackets directly (e.g. from mar list | grep ...)
    let list_file = dir.join("list.txt");
    fs::write(
        &list_file,
        format!(
            "{}\n{}\n",
            "src/organized/03 - Business Code [BC]/bc.1.pdf",
            "src/organized/03 - Business Code [BC]/bc.2.pdf"
        ),
    )
    .unwrap();

    let sliced_mar = dir.join("sliced.mar");
    run_mar_success(&[
        "slice",
        "-T",
        list_file.to_str().unwrap(),
        orig_mar.to_str().unwrap(),
        "-o",
        sliced_mar.to_str().unwrap(),
    ]);

    let list_out = run_mar_success(&["list", sliced_mar.to_str().unwrap()]);
    let out_str = String::from_utf8_lossy(&list_out.stdout);
    assert!(out_str.contains("bc.1.pdf"));
    assert!(out_str.contains("bc.2.pdf"));
    assert!(!out_str.contains("oc.1.pdf"));

    // 3. Test directory prefix in literal list file
    let dir_list_file = dir.join("dir_list.txt");
    fs::write(
        &dir_list_file,
        "src/organized/03 - Business Code [BC]/\n",
    )
    .unwrap();

    let dir_sliced_mar = dir.join("dir_sliced.mar");
    run_mar_success(&[
        "slice",
        "-T",
        dir_list_file.to_str().unwrap(),
        orig_mar.to_str().unwrap(),
        "-o",
        dir_sliced_mar.to_str().unwrap(),
    ]);

    let list_dir_out = run_mar_success(&["list", dir_sliced_mar.to_str().unwrap()]);
    let out_dir_str = String::from_utf8_lossy(&list_dir_out.stdout);
    assert!(out_dir_str.contains("bc.1.pdf"));
    assert!(out_dir_str.contains("bc.2.pdf"));
    assert!(!out_dir_str.contains("oc.1.pdf"));

    let _ = fs::remove_dir_all(&dir);
}
