class Mar < Formula
  desc "High-performance archival utility for efficient compression, storage, and retrieval of large datasets"
  homepage "https://github.com/earthframe/mar"
  url "https://github.com/earthframe/mar/archive/refs/tags/v0.3.0.tar.gz"
  sha256 "0000000000000000000000000000000000000000000000000000000000000000"
  license "Apache-2.0"

  depends_on "rust" => :build

  def install
    system "make", "mar"
    bin.install "mar"
  end

  test do
    system "#{bin}/mar", "version"
  end
end
