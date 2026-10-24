class Mar < Formula
  desc "Universal Data Container and high-performance archival utility"
  homepage "https://github.com/earthframe/mar"
  url "https://github.com/earthframe/mar/archive/refs/tags/v0.3.0.tar.gz"
  sha256 "b15a48417ada9f4b5a3935260d8b2ef101403514b50c7a1d3d03619f222a19ff"
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
