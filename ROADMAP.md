# MAR Roadmap

## Archive Format Enhancements

### Storage Efficiency
- [ ] **Eliminate Metadata Padding**: Currently, the archive format uses zero-padding to maintain data block alignment when metadata is compressed. This can lead to small amounts of wasted space.
    - *Potential Solution*: Move metadata to the end of the archive (tail-loading), similar to ZIP or 7z. This allows the metadata container to be exactly as large as needed without requiring pre-allocation or padding.
    - *Trade-off*: Requires seeking to the end of the file for the initial read, and may complicate "streaming" extraction scenarios.

### Performance & Robustness
- [ ] **Parallel Validation Improvements**: Continue refining the parallel validation logic to ensure deep integrity checks (like full decompression) are performed efficiently across all supported compression algorithms.
- [ ] **Streaming Checksum Verification**: Ensure that checksums generated during streaming writes are consistently verified during all read operations.
