#!/bin/bash
# Install system dependencies needed for R packages used in MAR benchmarking
# This handles fontconfig, freetype, and other system packages

set -e

OS=$(uname -s)
DISTRO=""

# Detect distribution
if [ "$OS" = "Linux" ]; then
    if [ -f /etc/os-release ]; then
        . /etc/os-release
        DISTRO="$ID"
    fi
fi

echo "=========================================="
echo "MAR Benchmarking - System Setup"
echo "=========================================="
echo ""
echo "OS: $OS"
if [ -n "$DISTRO" ]; then
    echo "Distribution: $DISTRO"
fi
echo ""

case "$OS" in
    Darwin)
        echo "Detected macOS. Installing dependencies via Homebrew..."
        echo ""
        if ! command -v brew &> /dev/null; then
            echo "✗ Homebrew not found. Please install from https://brew.sh"
            exit 1
        fi
        
        echo "Installing: r fontconfig freetype..."
        brew install r fontconfig freetype
        
        echo ""
        echo "✓ macOS system dependencies installed"
        ;;
        
    Linux)
        case "$DISTRO" in
            ubuntu|debian)
                echo "Detected Ubuntu/Debian. Installing dependencies via apt..."
                echo ""
                
                if [ "$EUID" -ne 0 ]; then
                    echo "This requires sudo. Running with sudo..."
                    echo ""
                fi
                
                PACKAGES="r-base-dev libcurl4-openssl-dev libssl-dev libxml2-dev libfontconfig1-dev libfreetype6-dev"
                
                echo "Installing: $PACKAGES"
                echo ""
                
                sudo apt-get update
                sudo apt-get install -y $PACKAGES
                
                echo ""
                echo "✓ Ubuntu/Debian system dependencies installed"
                ;;
                
            fedora|rhel|centos)
                echo "Detected Fedora/RHEL. Installing dependencies via dnf..."
                echo ""
                
                if [ "$EUID" -ne 0 ]; then
                    echo "This requires sudo. Running with sudo..."
                    echo ""
                fi
                
                PACKAGES="R-devel libcurl-devel openssl-devel libxml2-devel fontconfig-devel freetype-devel"
                
                echo "Installing: $PACKAGES"
                echo ""
                
                sudo dnf install -y $PACKAGES
                
                echo ""
                echo "✓ Fedora/RHEL system dependencies installed"
                ;;
                
            arch|manjaro)
                echo "Detected Arch/Manjaro. Installing dependencies via pacman..."
                echo ""
                
                if [ "$EUID" -ne 0 ]; then
                    echo "This requires sudo. Running with sudo..."
                    echo ""
                fi
                
                PACKAGES="r fontconfig freetype2"
                
                echo "Installing: $PACKAGES"
                echo ""
                
                sudo pacman -S --noconfirm $PACKAGES
                
                echo ""
                echo "✓ Arch/Manjaro system dependencies installed"
                ;;
                
            *)
                echo "✗ Unsupported distribution: $DISTRO"
                echo ""
                echo "Please manually install these system packages:"
                echo "  - r/R development libraries"
                echo "  - libcurl development libraries"
                echo "  - OpenSSL development libraries"
                echo "  - libxml2 development libraries"
                echo "  - fontconfig development libraries"
                echo "  - freetype development libraries"
                echo ""
                echo "Then try: make plot"
                exit 1
                ;;
        esac
        ;;
        
    *)
        echo "✗ Unsupported OS: $OS"
        echo ""
        echo "Please manually install system dependencies for your platform"
        exit 1
        ;;
esac

echo ""
echo "=========================================="
echo "✓ Setup complete!"
echo "=========================================="
echo ""
echo "You can now run:"
echo "  cd benchmarks"
echo "  make plot"
echo ""
