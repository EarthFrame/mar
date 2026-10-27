class Mar < Formula
  desc "Universal Data Container and high-performance archival utility"
  homepage "https://github.com/earthframe/mar"
  url "https://github.com/earthframe/mar/archive/refs/tags/v0.3.1.tar.gz"
  sha256 "412afa091c1a0b79dc186ee32b82667a0baf97c926de7e9756179d3aa1639e79"
  license "Apache-2.0"
  head "https://github.com/earthframe/mar.git", branch: "main"

  depends_on "rust" => :build

  def install
    system "cargo", "install", *std_cargo_args(path: "rust"), "--bin", "mar"
  end

  test do
    assert_match "mar version #{version}", shell_output("#{bin}/mar version")

    (testpath/"test.txt").write("Hello, MAR!")
    system bin/"mar", "create", "test.mar", "test.txt"
    assert_predicate testpath/"test.mar", :exist?
    assert_match "test.txt", shell_output("#{bin}/mar list test.mar")
  end
end
