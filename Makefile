# MAR Archive Tool - Makefile
# Implements MAR format specification v0.1.0

# Versioning
VERSION_MAJOR ?= 0
VERSION_MINOR ?= 1
VERSION_PATCH ?= 1
VERSION_RELEASE ?= 1
VERSION = $(VERSION_MAJOR).$(VERSION_MINOR).$(VERSION_PATCH)
PKG_VERSION = $(VERSION)-$(VERSION_RELEASE)

# Project Metadata
PROJECT_NAME = mar
PROJECT_DESC = High-performance archival utility for efficient compression, storage, and retrieval of large datasets with multi-format support
PROJECT_HOMEPAGE = https://github.com/earthframe/mar
PROJECT_REPO = https://github.com/earthframe/mar
PROJECT_MAINTAINER = EarthFrame Corporation
PROJECT_LICENSE = MIT

CXX = g++
# -----------------------------------------------------------------------------
# Build configuration knobs
# -----------------------------------------------------------------------------
#
# Examples:
#   make BUILD=release
#   make BUILD=debug SANITIZERS=address,undefined
#   make BUILD=gprof
#   make BUILD=release NATIVE=1 LTO=1
#   make BUILD=pgo-generate && ./mar ... && make BUILD=pgo-use PGO_DIR=./pgo
#
BUILD ?= release        # release|debug|gprof|pgo-generate|pgo-use
NATIVE ?= 0             # 1 => -march=native (host-specific)
LTO ?= 0                # 1 => -flto
SANITIZERS ?=           # e.g. address,undefined,thread
PGO_DIR ?= ./pgo        # profile data dir for PGO

# Detect OS
UNAME_S := $(shell uname -s)

BASE_CXXFLAGS ?= -std=c++17 -Wall -Wextra -Wpedantic
CXXFLAGS += $(BASE_CXXFLAGS)
ifeq ($(UNAME_S),Darwin)
    # macOS clang often needs this for C++17/20 features
    CXXFLAGS += -stdlib=libc++
endif
OPT_CXXFLAGS ?= -O2
DBG_CXXFLAGS ?= -g -fno-omit-frame-pointer
EXTRA_CXXFLAGS ?=
EXTRA_LDFLAGS ?=

# Architecture-specific flags (for distribution builds)
ARCH_FLAGS ?=
TARGET_NAME ?= mar

# Include paths
INCLUDES = -I./include -I./deps/simde

# Source files
SRCS = src/format.cpp src/checksum.cpp src/compression.cpp src/compression_gzip.cpp \
       src/compression_zstd.cpp src/compression_lz4.cpp src/compression_bzip2.cpp \
       src/sections.cpp src/name_index.cpp src/reader.cpp src/writer.cpp \
       src/file_descriptor_manager.cpp src/async_io.cpp src/thread_pool.cpp src/redact.cpp src/diff.cpp \
       src/index_registry.cpp src/index_minhash.cpp
MAIN_SRC = src/main.cpp
TEST_SRC = tests/test_main.cpp

# Object files
BUILD_DIR = build
DIST_DIR = dist
OBJS = $(SRCS:src/%.cpp=$(BUILD_DIR)/%.o)
MAIN_OBJ = $(BUILD_DIR)/main.o
TEST_OBJ = $(BUILD_DIR)/test_main.o

# Output
TARGET = $(TARGET_NAME)
TEST_TARGET = test_mar

# Detect available libraries
STATIC ?= 0
PKG_CONFIG_STATIC := $(if $(filter 1,$(STATIC)),--static,)

ZSTD_FOUND := $(shell pkg-config --exists libzstd 2>/dev/null && echo yes)
ZLIB_FOUND := $(shell pkg-config --exists zlib 2>/dev/null && echo yes)
LZ4_FOUND := $(shell pkg-config --exists liblz4 2>/dev/null && echo yes)
LIBDEFLATE_FOUND := $(shell pkg-config --exists libdeflate 2>/dev/null && echo yes)
BZIP2_FOUND := $(shell ldconfig -p 2>/dev/null | grep -q libbz2 && echo yes || (uname -s | grep -q Darwin && echo yes))
BLAKE3_FOUND := $(shell pkg-config --exists libblake3 2>/dev/null && echo yes)
URING_FOUND := $(shell pkg-config --exists liburing 2>/dev/null && echo yes)
KQUEUE_FOUND := $(shell test "$(UNAME_S)" = "Darwin" && echo yes)

# Library flags
LDFLAGS =
ifeq ($(STATIC),1)
    ifeq ($(shell uname -s),Linux)
        LDFLAGS += -static -static-libgcc -static-libstdc++
    endif
endif

CXXFLAGS += -DMAR_VERSION_MAJOR=$(VERSION_MAJOR) -DMAR_VERSION_MINOR=$(VERSION_MINOR) -DMAR_VERSION_PATCH=$(VERSION_PATCH)

# -----------------------------------------------------------------------------
# Build-mode specific flags
# -----------------------------------------------------------------------------
ifeq ($(BUILD),release)
    CXXFLAGS += $(OPT_CXXFLAGS) -DNDEBUG $(ARCH_FLAGS)
endif

ifeq ($(BUILD),debug)
    CXXFLAGS += -O0 $(DBG_CXXFLAGS) -DMAR_DEBUG
endif

ifeq ($(BUILD),gprof)
    CXXFLAGS += $(DBG_CXXFLAGS) -pg -DMAR_PROFILE
    LDFLAGS  += -pg
endif

ifeq ($(BUILD),pgo-generate)
    CXXFLAGS += $(OPT_CXXFLAGS) -DNDEBUG -fprofile-generate=$(PGO_DIR)
    LDFLAGS  += -fprofile-generate=$(PGO_DIR)
endif

ifeq ($(BUILD),pgo-use)
    CXXFLAGS += $(OPT_CXXFLAGS) -DNDEBUG -fprofile-use=$(PGO_DIR) -fprofile-correction
    LDFLAGS  += -fprofile-use=$(PGO_DIR) -fprofile-correction
endif

ifeq ($(NATIVE),1)
    CXXFLAGS += -march=native
endif

ifeq ($(LTO),1)
    CXXFLAGS += -flto
    LDFLAGS  += -flto
endif

ifneq ($(strip $(SANITIZERS)),)
    CXXFLAGS += -O0 $(DBG_CXXFLAGS) -fsanitize=$(SANITIZERS)
    LDFLAGS  += -fsanitize=$(SANITIZERS)
endif

# Allow last-mile overrides
CXXFLAGS += $(EXTRA_CXXFLAGS)
LDFLAGS  += $(EXTRA_LDFLAGS)

ifeq ($(ZSTD_FOUND),yes)
    CXXFLAGS += $(shell pkg-config --cflags libzstd)
    LDFLAGS += $(shell pkg-config $(PKG_CONFIG_STATIC) --libs libzstd)
else
    # Try direct link
    LDFLAGS += -lzstd 2>/dev/null || true
endif

ifeq ($(LIBDEFLATE_FOUND),yes)
    CXXFLAGS += $(shell pkg-config --cflags libdeflate)
    LDFLAGS += $(shell pkg-config $(PKG_CONFIG_STATIC) --libs libdeflate)
endif

ifeq ($(ZLIB_FOUND),yes)
    CXXFLAGS += $(shell pkg-config --cflags zlib)
    LDFLAGS += $(shell pkg-config $(PKG_CONFIG_STATIC) --libs zlib)
else
    LDFLAGS += -lz 2>/dev/null || true
endif

ifeq ($(LZ4_FOUND),yes)
    CXXFLAGS += $(shell pkg-config --cflags liblz4)
    LDFLAGS += $(shell pkg-config $(PKG_CONFIG_STATIC) --libs liblz4)
else
    LDFLAGS += -llz4 2>/dev/null || true
endif

ifeq ($(BZIP2_FOUND),yes)
    LDFLAGS += -lbz2
endif

ifeq ($(URING_FOUND),yes)
    CXXFLAGS += -DMAR_HAVE_URING=1 -DMAR_HAS_URING=1
    LDFLAGS += -luring
endif

ifeq ($(KQUEUE_FOUND),yes)
    CXXFLAGS += -DMAR_HAS_KQUEUE=1
endif

# Local BLAKE3 detection
LOCAL_BLAKE3_DIR = ./deps/BLAKE3/c
LOCAL_BLAKE3_LIB = $(LOCAL_BLAKE3_DIR)/libblake3.a

# Local libdeflate detection
LOCAL_LIBDEFLATE_DIR = ./deps/libdeflate
LOCAL_LIBDEFLATE_LIB = $(LOCAL_LIBDEFLATE_DIR)/libdeflate.a

ifeq ($(BLAKE3_FOUND),yes)
    CXXFLAGS += $(shell pkg-config --cflags libblake3) -DHAVE_BLAKE3=1
    LDFLAGS += $(shell pkg-config $(PKG_CONFIG_STATIC) --libs libblake3)
else ifneq ($(wildcard $(LOCAL_BLAKE3_LIB)),)
    INCLUDES += -I$(LOCAL_BLAKE3_DIR)
    LDFLAGS += $(LOCAL_BLAKE3_LIB)
    CXXFLAGS += -DHAVE_BLAKE3=1
    BLAKE3_FOUND := yes
else
    # Try to build it if the directory exists
    ifneq ($(wildcard $(LOCAL_BLAKE3_DIR)/Makefile),)
        BLAKE3_DEP = $(LOCAL_BLAKE3_LIB)
        INCLUDES += -I$(LOCAL_BLAKE3_DIR)
        LDFLAGS += $(LOCAL_BLAKE3_LIB)
        CXXFLAGS += -DHAVE_BLAKE3=1
        BLAKE3_FOUND := yes
    endif
endif

ifeq ($(LIBDEFLATE_FOUND),yes)
    CXXFLAGS += $(shell pkg-config --cflags libdeflate)
    LDFLAGS += $(shell pkg-config $(PKG_CONFIG_STATIC) --libs libdeflate)
else ifneq ($(wildcard $(LOCAL_LIBDEFLATE_LIB)),)
    INCLUDES += -I$(LOCAL_LIBDEFLATE_DIR)
    LDFLAGS += $(LOCAL_LIBDEFLATE_LIB)
    LIBDEFLATE_FOUND := yes
else
    # Try to build it if the directory exists
    ifneq ($(wildcard $(LOCAL_LIBDEFLATE_DIR)/CMakeLists.txt),)
        LIBDEFLATE_DEP = $(LOCAL_LIBDEFLATE_LIB)
        INCLUDES += -I$(LOCAL_LIBDEFLATE_DIR)
        LIBDEFLATE_FOUND := yes
    endif
endif

# macOS specific
ifeq ($(UNAME_S),Darwin)
    # Use Homebrew paths if available
    HOMEBREW_PREFIX ?= $(shell brew --prefix 2>/dev/null || echo /opt/homebrew)
    INCLUDES += -I$(HOMEBREW_PREFIX)/include
    LDFLAGS += -L$(HOMEBREW_PREFIX)/lib
    
    # On macOS, if STATIC=1, we try to prefer .a files if they exist
    ifeq ($(STATIC),1)
        # This is a bit of a hack but helps on macOS where -static isn't supported
        # We replace -l<lib> with the full path to the .a file if it exists in Homebrew
        LDFLAGS := $(subst -lzstd,$(wildcard $(HOMEBREW_PREFIX)/lib/libzstd.a),$(LDFLAGS))
        LDFLAGS := $(subst -llz4,$(wildcard $(HOMEBREW_PREFIX)/lib/liblz4.a),$(LDFLAGS))
        LDFLAGS := $(subst -lz,$(wildcard $(HOMEBREW_PREFIX)/lib/libz.a),$(LDFLAGS))
        LDFLAGS := $(subst -lbz2,$(wildcard $(HOMEBREW_PREFIX)/lib/libbz2.a),$(LDFLAGS))
        LDFLAGS := $(subst -ldeflate,$(wildcard $(HOMEBREW_PREFIX)/lib/libdeflate.a),$(LDFLAGS))
        LDFLAGS := $(subst -lblake3,$(wildcard $(HOMEBREW_PREFIX)/lib/libblake3.a),$(LDFLAGS))
    endif

    # Only add these if pkg-config failed to find them
    ifneq ($(ZSTD_FOUND),yes)
        ifneq ($(wildcard $(HOMEBREW_PREFIX)/lib/libzstd.*),)
            LDFLAGS += -lzstd
        endif
    endif
    ifneq ($(LZ4_FOUND),yes)
        ifneq ($(wildcard $(HOMEBREW_PREFIX)/lib/liblz4.*),)
            LDFLAGS += -llz4
        endif
    endif
    ifneq ($(BZIP2_FOUND),yes)
        ifneq ($(wildcard $(HOMEBREW_PREFIX)/lib/libbz2.*),)
            LDFLAGS += -lbz2
        endif
    endif
endif

# Remove duplicate libraries from LDFLAGS to avoid linker warnings
LDFLAGS := $(shell echo "$(LDFLAGS)" | tr ' ' '\n' | awk '!a[$$0]++' | tr '\n' ' ')

# Link zlib if not already handled by pkg-config or libdeflate
ifneq ($(ZLIB_FOUND),yes)
    ifeq ($(LIBDEFLATE_FOUND),)
        LDFLAGS += -lz
    endif
endif

# Phony targets
.PHONY: all clean test install check-deps drop-cache static release \
        deps debug \
        dist-linux-x86_64 \
		dist-linux-x86_64-sse42 \
		dist-linux-x86_64-avx2 \
        dist-linux-arm64 \
		dist-macos-arm64 \
		dist-macos-x86_64 \
		dist-macos-universal \
        dist-all \
		native \
		native-lto \
		help \
		deb \
		brew-formula

# Default target
all-internal: $(BUILD_DIR) $(BLAKE3_DEP) $(LIBDEFLATE_DEP) $(TARGET)

all: all-internal

# Build local BLAKE3
$(LOCAL_BLAKE3_LIB):
	@echo "Building local BLAKE3..."
	$(MAKE) -C $(LOCAL_BLAKE3_DIR)

# Build local libdeflate
$(LOCAL_LIBDEFLATE_LIB):
	@echo "Building local libdeflate..."
	@mkdir -p $(LOCAL_LIBDEFLATE_DIR)/build
	@cd $(LOCAL_LIBDEFLATE_DIR)/build && cmake .. -DLIBDEFLATE_BUILD_SHARED_LIB=OFF && make
	@cp $(LOCAL_LIBDEFLATE_DIR)/build/libdeflate.a $(LOCAL_LIBDEFLATE_LIB)

# Drop system caches (Linux only, requires sudo)
drop-cache:
	@echo "Dropping system caches..."
	sync
	@sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"

# Create build directory
$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

# Compile object files
$(BUILD_DIR)/%.o: src/%.cpp | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

$(BUILD_DIR)/test_main.o: tests/test_main.cpp | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

# Link main binary
$(TARGET): $(OBJS) $(MAIN_OBJ) $(BLAKE3_DEP) $(LIBDEFLATE_DEP)
	$(CXX) $(CXXFLAGS) $(OBJS) $(MAIN_OBJ) $(BLAKE3_DEP) $(LIBDEFLATE_DEP) -o $@ $(LDFLAGS)

# Link test binary
$(TEST_TARGET): $(OBJS) $(TEST_OBJ) $(BLAKE3_DEP) $(LIBDEFLATE_DEP)
	$(CXX) $(CXXFLAGS) $(OBJS) $(TEST_OBJ) $(BLAKE3_DEP) $(LIBDEFLATE_DEP) -o $@ $(LDFLAGS)

# Static build
static:
	$(MAKE) STATIC=1 all-internal

# Release build (optimized, static)
release:
	$(MAKE) STATIC=1 BUILD=release OPT_CXXFLAGS="-O3" all-internal

# Dependency management
deps:
	@echo "Adding dependencies via git subtree..."
	@if [ ! -d "deps/BLAKE3" ]; then \
		git subtree add --prefix deps/BLAKE3 https://github.com/BLAKE3-team/BLAKE3.git master --squash; \
	else \
		echo "deps/BLAKE3 already exists, skipping."; \
	fi
	@if [ ! -d "deps/xxHash" ]; then \
		git subtree add --prefix deps/xxHash https://github.com/Cyan4973/xxHash.git dev --squash; \
	else \
		echo "deps/xxHash already exists, skipping."; \
	fi
	@if [ ! -d "deps/libdeflate" ]; then \
		git subtree add --prefix deps/libdeflate https://github.com/ebiggers/libdeflate.git master --squash; \
	else \
		echo "deps/libdeflate already exists, skipping."; \
	fi

# Debug build
debug:
	$(MAKE) BUILD=debug clean all-internal

# gprof build (compile+link with -pg)
gprof:
	$(MAKE) BUILD=gprof clean all-internal

# PGO build stages
pgo-generate:
	@mkdir -p $(PGO_DIR)
	$(MAKE) BUILD=pgo-generate clean all-internal

pgo-use:
	$(MAKE) BUILD=pgo-use clean all-internal

# Run tests
test: $(TEST_TARGET)
	./$(TEST_TARGET)

# Integration test
integration-test: $(TARGET)
	@echo "Running integration tests..."
	@./tests/integration_test.sh

# Performance smoke test - quick regression detection
# Generates previous_run.txt for performance tracking
# Warns if performance changes by more than 5% (configurable via PERF_THRESHOLD)
perf-smoke: $(TARGET)
	@echo "Running performance smoke test..."
	@PERF_DIR=.perf PERF_THRESHOLD=$(PERF_THRESHOLD) THREADS=$(THREADS) bash scripts/perf_smoke_test.sh

# Check dependencies
check-deps:
	@echo "Checking dependencies..."
	@echo "ZSTD: $(if $(ZSTD_FOUND),found,NOT FOUND - limited functionality)"
	@echo "ZLIB: $(if $(ZLIB_FOUND),found,NOT FOUND - limited functionality)"
	@echo "LIBDEFLATE: $(if $(LIBDEFLATE_FOUND),found,NOT FOUND - using standard zlib)"
	@echo "LZ4: $(if $(LZ4_FOUND),found,NOT FOUND - limited functionality)"
	@echo "BZIP2: $(if $(BZIP2_FOUND),found,NOT FOUND - limited functionality)"
	@echo "BLAKE3: $(if $(BLAKE3_FOUND),found,using builtin reference implementation)"

# Install
install: $(TARGET)
	install -d $(DESTDIR)$(PREFIX)/bin
	install -m 755 $(TARGET) $(DESTDIR)$(PREFIX)/bin/

# Clean
clean:
	rm -rf $(BUILD_DIR) $(DIST_DIR) $(TARGET) $(TEST_TARGET) mar-* 2>/dev/null || true

# Print variables for debugging
print-%:
	@echo '$*=$($*)'

# =============================================================================
# Distribution Build Targets (Portable Binaries)
# =============================================================================
#
# These targets create portable binaries optimized for specific architectures
# while maintaining broad compatibility within that architecture family.
#
# Usage:
#   make dist-linux-x86_64        # Linux, maximum compatibility (SSE2)
#   make dist-linux-x86_64-sse42  # Linux, modern CPUs (2008+)
#   make dist-macos-universal     # macOS, both ARM64 and x86_64
#   make dist-all                 # Build all for current platform

# Linux x86_64 - SSE2 baseline (maximum compatibility)
dist-linux-x86_64:
	@mkdir -p $(DIST_DIR)
	$(MAKE) clean
	$(MAKE) STATIC=1 BUILD=release \
	        ARCH_FLAGS="-march=x86-64 -mtune=generic" \
	        TARGET_NAME=$(DIST_DIR)/mar-linux-x86_64 \
	        all-internal
	@echo ""
	@echo "Built: $(DIST_DIR)/mar-linux-x86_64 (portable x86_64, SSE2 baseline)"
	@ls -lh $(DIST_DIR)/mar-linux-x86_64

# Linux x86_64 - SSE4.2 optimized (recommended for modern systems)
dist-linux-x86_64-sse42:
	@mkdir -p $(DIST_DIR)
	$(MAKE) clean
	$(MAKE) STATIC=1 BUILD=release \
	        ARCH_FLAGS="-march=nehalem -mtune=generic" \
	        TARGET_NAME=$(DIST_DIR)/mar-linux-x86_64-sse42 \
	        all-internal
	@echo ""
	@echo "Built: $(DIST_DIR)/mar-linux-x86_64-sse42 (2008+ Intel, 2011+ AMD)"
	@ls -lh $(DIST_DIR)/mar-linux-x86_64-sse42

# Linux x86_64 - AVX2 optimized (for newer systems)
dist-linux-x86_64-avx2:
	@mkdir -p $(DIST_DIR)
	$(MAKE) clean
	$(MAKE) STATIC=1 BUILD=release \
	        ARCH_FLAGS="-march=haswell -mtune=generic" \
	        TARGET_NAME=$(DIST_DIR)/mar-linux-x86_64-avx2 \
	        all-internal
	@echo ""
	@echo "Built: $(DIST_DIR)/mar-linux-x86_64-avx2 (2013+ Intel, 2015+ AMD)"
	@ls -lh $(DIST_DIR)/mar-linux-x86_64-avx2

# Linux ARM64 (ARMv8-A with NEON)
dist-linux-arm64:
	@mkdir -p $(DIST_DIR)
	$(MAKE) clean
	$(MAKE) STATIC=1 BUILD=release \
	        ARCH_FLAGS="-march=armv8-a+simd -mtune=generic" \
	        TARGET_NAME=$(DIST_DIR)/mar-linux-arm64 \
	        all-internal
	@echo ""
	@echo "Built: $(DIST_DIR)/mar-linux-arm64 (ARMv8-A with NEON)"
	@ls -lh $(DIST_DIR)/mar-linux-arm64

# macOS ARM64 (Apple Silicon: M1, M2, M3, M4)
dist-macos-arm64:
	@mkdir -p $(DIST_DIR)
	$(MAKE) clean
	$(MAKE) BUILD=release \
	        ARCH_FLAGS="-arch arm64" \
	        TARGET_NAME=$(DIST_DIR)/mar-macos-arm64 \
	        all-internal
	@echo ""
	@echo "Built: $(DIST_DIR)/mar-macos-arm64 (Apple Silicon)"
	@file $(DIST_DIR)/mar-macos-arm64
	@ls -lh $(DIST_DIR)/mar-macos-arm64

# macOS x86_64 (Intel Macs)
dist-macos-x86_64:
	@mkdir -p $(DIST_DIR)
	$(MAKE) clean
	$(MAKE) BUILD=release \
	        ARCH_FLAGS="-arch x86_64 -march=nehalem" \
	        TARGET_NAME=$(DIST_DIR)/mar-macos-x86_64 \
	        all-internal
	@echo ""
	@echo "Built: $(DIST_DIR)/mar-macos-x86_64 (Intel Macs, 2008+)"
	@file $(DIST_DIR)/mar-macos-x86_64
	@ls -lh $(DIST_DIR)/mar-macos-x86_64

# macOS Universal Binary (ARM64 + x86_64)
dist-macos-universal:
	@mkdir -p $(DIST_DIR)
	@echo "Building macOS Universal Binary (ARM64 + x86_64)..."
	$(MAKE) dist-macos-arm64
	@mv $(DIST_DIR)/mar-macos-arm64 $(DIST_DIR)/mar-arm64-temp
	$(MAKE) dist-macos-x86_64
	@mv $(DIST_DIR)/mar-macos-x86_64 $(DIST_DIR)/mar-x86_64-temp
	lipo -create -output $(DIST_DIR)/mar-macos-universal $(DIST_DIR)/mar-arm64-temp $(DIST_DIR)/mar-x86_64-temp
	@rm $(DIST_DIR)/mar-arm64-temp $(DIST_DIR)/mar-x86_64-temp
	@echo ""
	@echo "Built: $(DIST_DIR)/mar-macos-universal (ARM64 + x86_64)"
	@lipo -info $(DIST_DIR)/mar-macos-universal
	@ls -lh $(DIST_DIR)/mar-macos-universal

# Build all distribution binaries for current platform
dist-all:
	@mkdir -p $(DIST_DIR)
	@echo "Building all distribution binaries for current platform..."
	@if [ "$(UNAME_S)" = "Linux" ]; then \
	    if [ "$$(uname -m)" = "x86_64" ]; then \
	        echo "==> Building Linux x86_64 variants..."; \
	        $(MAKE) dist-linux-x86_64; \
	        $(MAKE) dist-linux-x86_64-sse42; \
	    elif [ "$$(uname -m)" = "aarch64" ]; then \
	        echo "==> Building Linux ARM64..."; \
	        $(MAKE) dist-linux-arm64; \
	    fi \
	elif [ "$(UNAME_S)" = "Darwin" ]; then \
	    echo "==> Building macOS Universal Binary..."; \
	    $(MAKE) dist-macos-universal; \
	fi
	@echo ""
	@echo "Distribution builds complete!"
	@ls -lh $(DIST_DIR)/mar-* 2>/dev/null || true

# Native optimized build (maximum performance for current CPU)
native:
	$(MAKE) BUILD=release NATIVE=1 all-internal
	@echo ""
	@echo "Built: mar (native optimized for current CPU)"
	@echo "WARNING: This binary may not run on different CPUs!"

# Native + LTO (maximum performance, slower compile)
native-lto:
	$(MAKE) BUILD=release NATIVE=1 LTO=1 all-internal
	@echo ""
	@echo "Built: mar (native + LTO, maximum performance)"
	@echo "WARNING: This binary may not run on different CPUs!"

# Debian package build
deb: release
	@echo "Building Debian package v$(PKG_VERSION)..."
	@mkdir -p $(PROJECT_NAME)-$(PKG_VERSION)/usr/bin
	@mkdir -p $(PROJECT_NAME)-$(PKG_VERSION)/DEBIAN
	@cp $(TARGET) $(PROJECT_NAME)-$(PKG_VERSION)/usr/bin/
	@echo "Package: $(PROJECT_NAME)" > $(PROJECT_NAME)-$(PKG_VERSION)/DEBIAN/control
	@echo "Version: $(PKG_VERSION)" >> $(PROJECT_NAME)-$(PKG_VERSION)/DEBIAN/control
	@echo "Section: utils" >> $(PROJECT_NAME)-$(PKG_VERSION)/DEBIAN/control
	@echo "Priority: optional" >> $(PROJECT_NAME)-$(PKG_VERSION)/DEBIAN/control
	@echo "Architecture: $$(dpkg --print-architecture)" >> $(PROJECT_NAME)-$(PKG_VERSION)/DEBIAN/control
	@echo "Depends: libc6, libstdc++6, libgcc-s1, libzstd1, zlib1g, liblz4-1, libbz2-1.0, libdeflate0" >> $(PROJECT_NAME)-$(PKG_VERSION)/DEBIAN/control
	@echo "Maintainer: $(PROJECT_MAINTAINER)" >> $(PROJECT_NAME)-$(PKG_VERSION)/DEBIAN/control
	@echo "Description: $(PROJECT_DESC)" >> $(PROJECT_NAME)-$(PKG_VERSION)/DEBIAN/control
	@echo " MAR is a high-performance archive format designed for efficient storage" >> $(PROJECT_NAME)-$(PKG_VERSION)/DEBIAN/control
	@echo " and retrieval of large datasets, with support for various compression" >> $(PROJECT_NAME)-$(PKG_VERSION)/DEBIAN/control
	@echo " algorithms and indexing." >> $(PROJECT_NAME)-$(PKG_VERSION)/DEBIAN/control
	@dpkg-deb --build $(PROJECT_NAME)-$(PKG_VERSION)
	@rm -rf $(PROJECT_NAME)-$(PKG_VERSION)
	@echo "Built: $(PROJECT_NAME)-$(PKG_VERSION).deb"

# Homebrew Formula generation
brew-formula:
	@echo "Generating Homebrew formula $(PROJECT_NAME).rb..."
	@echo "class $$(echo $(PROJECT_NAME) | sed 's/\b./\u&/g') < Formula" > $(PROJECT_NAME).rb
	@echo "  desc \"$(PROJECT_DESC)\"" >> $(PROJECT_NAME).rb
	@echo "  homepage \"$(PROJECT_HOMEPAGE)\"" >> $(PROJECT_NAME).rb
	@echo "  url \"$(PROJECT_REPO)/archive/refs/tags/v$(VERSION).tar.gz\"" >> $(PROJECT_NAME).rb
	@echo "  sha256 \"REPLACE_WITH_ACTUAL_SHA256\"" >> $(PROJECT_NAME).rb
	@echo "  license \"$(PROJECT_LICENSE)\"" >> $(PROJECT_NAME).rb
	@echo "" >> $(PROJECT_NAME).rb
	@echo "  depends_on \"pkg-config\" => :build" >> $(PROJECT_NAME).rb
	@echo "  depends_on \"libzstd\"" >> $(PROJECT_NAME).rb
	@echo "  depends_on \"lz4\"" >> $(PROJECT_NAME).rb
	@echo "  depends_on \"libdeflate\"" >> $(PROJECT_NAME).rb
	@echo "" >> $(PROJECT_NAME).rb
	@echo "  def install" >> $(PROJECT_NAME).rb
	@echo "    system \"make\", \"release\"" >> $(PROJECT_NAME).rb
	@echo "    bin.install \"$(PROJECT_NAME)\"" >> $(PROJECT_NAME).rb
	@echo "  end" >> $(PROJECT_NAME).rb
	@echo "" >> $(PROJECT_NAME).rb
	@echo "  test do" >> $(PROJECT_NAME).rb
	@echo "    system \"#{bin}/$(PROJECT_NAME)\", \"--version\"" >> $(PROJECT_NAME).rb
	@echo "  end" >> $(PROJECT_NAME).rb
	@echo "end" >> $(PROJECT_NAME).rb
	@echo "Created $(PROJECT_NAME).rb. Note: You must update the sha256 after tagging a release."


# Help target - show available build targets
help:
	@echo "MAR Archive Tool - Build Targets"
	@echo ""
	@echo "Standard Builds:"
	@echo "  make              - Build release binary (optimized)"
	@echo "  make debug        - Build with debug symbols"
	@echo "  make static       - Build static binary"
	@echo "  make release      - Build optimized static binary"
	@echo "  make test         - Build and run tests"
	@echo ""
	@echo "Distribution Builds (Portable):"
	@echo "  make dist-linux-x86_64        - Linux SSE2 (max compatibility)"
	@echo "  make dist-linux-x86_64-sse42  - Linux SSE4.2 (2008+ CPUs)"
	@echo "  make dist-linux-x86_64-avx2   - Linux AVX2 (2013+ CPUs)"
	@echo "  make dist-linux-arm64         - Linux ARM64 (ARMv8-A)"
	@echo "  make dist-macos-arm64         - macOS Apple Silicon"
	@echo "  make dist-macos-x86_64        - macOS Intel"
	@echo "  make dist-macos-universal     - macOS Universal (ARM64+x86_64)"
	@echo "  make dist-all                 - Build all for current platform"
	@echo ""
	@echo "Performance Builds (Host-Specific, Not Portable):"
	@echo "  make native       - Optimize for current CPU"
	@echo "  make native-lto   - Maximum performance + LTO"
	@echo ""
	@echo "Packaging:"
	@echo "  make deb          - Build Debian package (.deb)"
	@echo "                      (Use VERSION_RELEASE=n to increment patch/release)"
	@echo "  make brew-formula - Generate Homebrew formula (mar.rb)"
	@echo ""
	@echo "Advanced:"
	@echo "  make STATIC=1     - Force static linking"
	@echo "  make NATIVE=1     - Force native optimization"
	@echo "  make LTO=1        - Enable link-time optimization"
	@echo "  make perf-smoke   - Run performance smoke tests"
	@echo ""
	@echo "See docs/BUILD_CONFIGURATIONS.md for details"
