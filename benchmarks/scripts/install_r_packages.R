#!/usr/bin/env Rscript
# Automatically install required R packages for MAR benchmarking
# This script is called by the Makefile before plotting
# It handles system dependencies and provides helpful diagnostics

required_packages <- c("tidyverse", "scales")

# Function to safely check if a package is installed and can be loaded
safe_require <- function(pkg) {
  tryCatch({
    suppressWarnings(suppressMessages(require(pkg, character.only = TRUE, quietly = TRUE)))
  }, error = function(e) FALSE)
}

# Detect OS
get_os <- function() {
  if (.Platform$OS.type == "windows") {
    return("windows")
  } else if (Sys.info()["sysname"] == "Darwin") {
    return("macos")
  } else if (Sys.info()["sysname"] == "Linux") {
    return("linux")
  }
  return("unknown")
}

# Check which packages are missing
cat("Checking R packages...\n")
missing_packages <- required_packages[!sapply(required_packages, safe_require)]

if (length(missing_packages) == 0) {
  cat("✓ All required R packages are already installed.\n")
  quit(status = 0)
}

cat("→ Installing missing packages:", paste(missing_packages, collapse = ", "), "\n")

# Set a reliable CRAN mirror
tryCatch({
  options(repos = c(CRAN = "https://cloud.r-project.org"))
}, error = function(e) {
  cat("Warning: Could not set CRAN mirror\n")
})

# Try installation with different approaches
install_success <- FALSE

# Attempt 1: Try binary packages first (faster, no compilation needed)
cat("\nAttempt 1: Trying binary packages...\n")
tryCatch({
  suppressWarnings(
    suppressMessages(
      install.packages(
        missing_packages,
        dependencies = TRUE,
        quiet = FALSE,
        repos = "https://cloud.r-project.org",
        type = "binary"
      )
    )
  )
  install_success <- TRUE
}, error = function(e) {
  cat("  → Binary installation failed, trying source...\n")
}, warning = function(w) {
  # Warnings are ok, we'll check if packages loaded anyway
})

# Attempt 2: Try source compilation (with system dependencies now available)
if (!install_success) {
  cat("\nAttempt 2: Trying source compilation...\n")
  tryCatch({
    suppressWarnings(
      suppressMessages(
        install.packages(
          missing_packages,
          dependencies = TRUE,
          quiet = FALSE,
          repos = "https://cloud.r-project.org",
          type = "source"
        )
      )
    )
    install_success <- TRUE
  }, error = function(e) {
    cat("  → Source compilation also failed\n")
  })
}

# Verify installation
cat("\nVerifying installation...\n")
verified_packages <- sapply(missing_packages, safe_require)
failed_packages <- names(verified_packages[!verified_packages])

if (length(failed_packages) == 0) {
  cat("✓ All packages successfully installed and verified.\n")
  quit(status = 0)
}

# Installation failed - provide helpful diagnostics
os <- get_os()
cat("\n✗ Failed to install:", paste(failed_packages, collapse = ", "), "\n\n")
cat("This is usually due to missing system dependencies.\n\n")

cat("Troubleshooting:\n\n")

if (os == "linux") {
  cat("On Ubuntu/Debian, install system dependencies:\n")
  cat("  sudo apt-get update\n")
  cat("  sudo apt-get install -y \\\n")
  cat("    r-base-dev \\\n")
  cat("    libcurl4-openssl-dev \\\n")
  cat("    libssl-dev \\\n")
  cat("    libxml2-dev \\\n")
  cat("    libfontconfig1-dev \\\n")
  cat("    libfreetype6-dev\n\n")
  
  cat("On Fedora/RHEL, install system dependencies:\n")
  cat("  sudo dnf install -y \\\n")
  cat("    R-devel \\\n")
  cat("    libcurl-devel \\\n")
  cat("    openssl-devel \\\n")
  cat("    libxml2-devel \\\n")
  cat("    fontconfig-devel \\\n")
  cat("    freetype-devel\n\n")
  
} else if (os == "macos") {
  cat("On macOS with Homebrew:\n")
  cat("  brew install r fontconfig freetype\n\n")
  
} else if (os == "windows") {
  cat("On Windows:\n")
  cat("  1. Ensure R is fully installed with Rtools\n")
  cat("  2. Try: install.packages('tidyverse', type='binary')\n\n")
}

cat("After installing system dependencies, try again:\n")
cat("  make plot\n\n")

cat("Or install packages manually:\n")
cat("  R --slave -e \"install.packages(c('", 
    paste(failed_packages, collapse = "', '"), "'))\"\n\n", sep = "")

cat("For more help, see benchmarks/R_DEPENDENCIES.md\n")

# Exit with non-fatal status (let make continue)
quit(status = 0)


