#!/bin/bash
# Quick installer for R dependencies needed for MAR benchmarking
# Usage: ./setup_r_packages.sh

set -e

echo "=========================================="
echo "MAR Benchmarking - R Package Setup"
echo "=========================================="
echo ""

# Check if R is installed
if ! command -v R &> /dev/null; then
    echo "✗ R is not installed."
    echo ""
    echo "Install R first using one of these methods:"
    echo ""
    echo "macOS (Homebrew):"
    echo "  brew install r"
    echo ""
    echo "Ubuntu/Debian:"
    echo "  sudo apt-get install r-base-dev"
    echo ""
    echo "Fedora/RHEL:"
    echo "  sudo dnf install R-devel"
    echo ""
    echo "Arch:"
    echo "  sudo pacman -S r"
    echo ""
    exit 1
fi

R_VERSION=$(R --version | head -1)
echo "Found R: $R_VERSION"
echo ""

# Check for required packages
echo "Checking R packages..."
R --slave -e "
  packages <- c('tidyverse', 'scales')
  installed <- sapply(packages, function(pkg) {
    suppressWarnings(require(pkg, character.only = TRUE, quietly = TRUE))
  })
  
  missing <- packages[!installed]
  if (length(missing) == 0) {
    cat('✓ All packages already installed\n')
    quit(status = 0)
  } else {
    cat('Missing packages:', paste(missing, collapse = ', '), '\n')
    quit(status = 1)
  }
" && echo "" && exit 0

# Install missing packages
echo ""
echo "Installing missing packages..."
echo "(This may take a few minutes on first run)"
echo ""

R --slave -e "
  packages <- c('tidyverse', 'scales')
  cat('Installing:', paste(packages, collapse = ', '), '\n\n')
  
  tryCatch({
    install.packages(
      packages,
      dependencies = TRUE,
      quiet = FALSE,
      repos = 'https://cran.r-project.org'
    )
    
    # Verify
    installed <- sapply(packages, function(pkg) {
      suppressWarnings(require(pkg, character.only = TRUE, quietly = TRUE))
    })
    
    if (all(installed)) {
      cat('\n✓ Installation successful!\n')
      quit(status = 0)
    } else {
      cat('\n✗ Installation incomplete. Some packages failed.\n')
      quit(status = 1)
    }
  }, error = function(e) {
    cat('\n✗ Installation failed:', e\$message, '\n')
    quit(status = 1)
  })
" 

RESULT=$?
if [ $RESULT -eq 0 ]; then
    echo ""
    echo "=========================================="
    echo "✓ Setup complete!"
    echo "=========================================="
    echo ""
    echo "You can now run:"
    echo "  cd benchmarks"
    echo "  make plot"
    echo ""
else
    echo ""
    echo "=========================================="
    echo "✗ Setup failed"
    echo "=========================================="
    echo ""
    echo "Troubleshooting:"
    echo "1. Check your internet connection"
    echo "2. Try a different CRAN mirror:"
    echo "   R --slave -e \"options(repos=c(CRAN='https://cloud.r-project.org')); install.packages(c('tidyverse', 'scales'))\""
    echo "3. See R_DEPENDENCIES.md for platform-specific setup"
    echo ""
    exit 1
fi
