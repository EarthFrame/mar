class Mar < Formula
  desc "High-performance archival utility for efficient compression, storage, and retrieval of large datasets"
  homepage "https://github.com/earthframe/mar"
  url "https://github.com/earthframe/mar/archive/refs/tags/v0.1.1.tar.gz"
  sha256 "0000000000000000000000000000000000000000000000000000000000000000"
  license "MIT"

  depends_on "pkg-config" => :build
  depends_on "libzstd"
  depends_on "lz4"
  depends_on "libdeflate"
  depends_on "bzip2"

  def install
    # Build with optimizations for distribution
    system "make", "release"
    bin.install "mar"
  end

  test do
    # Simple smoke test to verify installation
    system "#{bin}/mar", "--version"
  end
end
