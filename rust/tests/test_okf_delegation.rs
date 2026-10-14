use std::process::Command;

#[test]
fn test_mar_okf_enterprise_delegation_missing_license() {
    let mar_bin = env!("CARGO_BIN_EXE_mar");

    // When MAR_ENTERPRISE_BIN is unset or points to nonexistent path
    let output = Command::new(mar_bin)
        .arg("okf")
        .env("MAR_ENTERPRISE_BIN", "/nonexistent/mar-enterprise-path")
        .output()
        .expect("failed to execute mar");

    assert_eq!(output.status.code(), Some(69));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("'mar okf' requires the MAR Enterprise Extension (mar-enterprise)."));
    assert!(stderr.contains("https://earthframe.com/enterprise"));
}

#[test]
fn test_mar_okf_enterprise_delegation_override() {
    let mar_bin = env!("CARGO_BIN_EXE_mar");
    let temp_dir = std::env::temp_dir().join("mar_test_enterprise_mock");
    let _ = std::fs::remove_dir_all(&temp_dir);
    std::fs::create_dir_all(&temp_dir).expect("create temp dir");

    let mock_bin = temp_dir.join("mock-enterprise");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::write(
            &mock_bin,
            "#!/bin/sh\necho \"MOCK_ENTERPRISE_CALLED: $@\"\nexit 0\n",
        )
        .expect("write mock script");
        let mut perms = std::fs::metadata(&mock_bin).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&mock_bin, perms).unwrap();
    }

    let output = Command::new(mar_bin)
        .args(&["okf", "pack", "./foo", "-f", "bar.mar"])
        .env("MAR_ENTERPRISE_BIN", &mock_bin)
        .output()
        .expect("failed to execute mar with mock enterprise");

    assert_eq!(output.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("MOCK_ENTERPRISE_CALLED: okf pack ./foo -f bar.mar"));

    let _ = std::fs::remove_dir_all(&temp_dir);
}
