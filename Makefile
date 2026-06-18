# MAR Archive Tool - Makefile
# Implements MAR format specification v0.1.0

# Versioning
VERSION_MAJOR ?= 0
VERSION_MINOR ?= 1
VERSION_PATCH ?= 1
VERSION_RELEASE ?= 1
VERSION = $(VERSION_MAJOR).$(VERSION_MINOR).$(VERSION_PATCH)
PKG_VERSION = $(VERSION)-$(VERSION_RELEASE)

# Installation paths
PREFIX ?= /usr/local
BINDIR ?= $(PREFIX)/bin
DATADIR ?= $(PREFIX)/share
MANDIR ?= $(DATADIR)/man
DOCDIR ?= $(DATADIR)/doc/$(PROJECT_NAME)

# Project Metadata
PROJECT_NAME = mar
PROJECT_DESC = High-performance archival utility for efficient compression, storage, and retrieval of large datasets with multi-format support
PROJECT_HOMEPAGE = https://github.com/earthframe/mar
PROJECT_REPO = https://github.com/earthframe/mar
PROJECT_MAINTAINER = EarthFrame Corporation
PROJECT_LICENSE = MIT

# Developer tool versions
ZIG_VERSION = 0.16.0

# Detect OS
UNAME_S := $(shell uname -s)

# Developer tool paths — override if your tools are not on PATH.
# macOS users: after 'brew install llvm', tools are under $(brew --prefix llvm)/bin/.
ifeq ($(UNAME_S),Darwin)
    LLVM_ROOT := $(shell brew --prefix llvm 2>/dev/null)
    ifneq ($(LLVM_ROOT),)
        LLVM_BIN := $(LLVM_ROOT)/bin
        # macOS needs sysroot to find system headers (<string>, <mutex>, etc.)
        _MACOS_SDK := $(shell xcrun --show-sdk-path 2>/dev/null)
        LINT_FLAGS := --sysroot=$(_MACOS_SDK) \
                      -I$(LLVM_ROOT)/include/c++/v1 \
                      -stdlib=libc++
    endif
endif

CLANG_TIDY   ?= clang-tidy
CLANG_FORMAT ?= clang-format
ZIG          ?= zig

CXX ?= g++
# -----------------------------------------------------------------------------
# Build configuration knobs
# -----------------------------------------------------------------------------
#
# Examples:
#   make BUILD=release
#   make CXX=clang++
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

# Detect OS (already detected at top)

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
INCLUDES = -I./include -isystem ./deps -isystem ./deps/simde -isystem ./deps/hnswlib

# Source files
SRCS = src/format.cpp src/checksum.cpp src/compression.cpp src/compression_gzip.cpp \
       src/compression_zstd.cpp src/compression_lz4.cpp src/compression_bzip2.cpp \
       src/sections.cpp src/name_index.cpp src/reader.cpp src/writer.cpp \
       src/file_descriptor_manager.cpp src/async_io.cpp src/thread_pool.cpp src/redact.cpp src/diff.cpp \
       src/index_registry.cpp src/index_minhash.cpp src/index_vector.cpp src/index_bm25.cpp src/index_genomic.cpp \
       src/index_email.cpp src/index_timeseries.cpp src/embed_server.cpp 
MAIN_SRC = src/main.cpp
TEST_SRC = tests/test_main.cpp

# Source files subject to lint checks.
# Excludes vendored/generated headers (xxhash3.h, deps/).
LINT_SRCS := $(wildcard src/*.cpp) $(wildcard include/mar/*.hpp) tests/test_main.cpp

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
    CXXFLAGS += -DMAR_HAVE_BZIP2=1
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
    ifneq ($(wildcard $(LOCAL_BLAKE3_DIR)/blake3.c),)
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
        # Helper function to find a static lib or fall back to the dynamic flag
        # Usage: $(call find_static,libname,flag)
        find_static = $(if $(wildcard $(HOMEBREW_PREFIX)/lib/$(1).a),$(HOMEBREW_PREFIX)/lib/$(1).a,$(2))

        LDFLAGS := $(subst -lzstd,$(call find_static,libzstd,-lzstd),$(LDFLAGS))
        LDFLAGS := $(subst -llz4,$(call find_static,liblz4,-llz4),$(LDFLAGS))
        LDFLAGS := $(subst -lz,$(call find_static,libz,-lz),$(LDFLAGS))
        LDFLAGS := $(subst -lbz2,$(call find_static,libbz2,-lbz2),$(LDFLAGS))
        LDFLAGS := $(subst -ldeflate,$(call find_static,libdeflate,-ldeflate),$(LDFLAGS))
        
        # Prefer local BLAKE3 static library if it exists, otherwise check Homebrew
        ifneq ($(wildcard $(LOCAL_BLAKE3_LIB)),)
            LDFLAGS := $(subst -lblake3,$(LOCAL_BLAKE3_LIB),$(LDFLAGS))
        else
            LDFLAGS := $(subst -lblake3,$(call find_static,libblake3,-lblake3),$(LDFLAGS))
        endif
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
else
    # On macOS, pkg-config might find zlib but we still need to link it
    ifeq ($(UNAME_S),Darwin)
        LDFLAGS += -lz
    endif
endif

# Phony targets
.PHONY: all clean test install check-deps check-dev-deps drop-cache static release \
        deps debug system-deps dev-deps lint lint-fix format \
        dist-linux-x86_64 \
		dist-linux-x86_64-sse42 \
		dist-linux-x86_64-avx2 \
        dist-linux-arm64 \
		dist-linux-x86_64-musl \
		dist-linux-x86_64-sse42-musl \
		dist-linux-arm64-musl \
		dist-macos-arm64 \
		dist-macos-x86_64 \
		dist-macos-universal \
        dist-all \
		zig-check \
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
	@echo "Building local BLAKE3 (portable)..."
	@cd $(LOCAL_BLAKE3_DIR) && cmake . && make -j

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

# System dependency installation
system-deps:
	@echo "Detecting OS for system dependency installation..."
	@if [ "$(UNAME_S)" = "Darwin" ]; then \
		if command -v brew >/dev/null 2>&1; then \
			echo "Installing dependencies via Homebrew..."; \
			brew install gcc zstd lz4 bzip2 libdeflate; \
		else \
			echo "Error: Homebrew not found. Please install Homebrew or install dependencies manually."; \
			exit 1; \
		fi \
	elif [ "$(UNAME_S)" = "Linux" ]; then \
		if [ -f /etc/debian_version ]; then \
			echo "Detected Debian/Ubuntu. Installing via apt..."; \
			sudo apt update && sudo apt install -y build-essential cmake libzstd-dev liblz4-dev libbz2-dev zlib1g-dev libdeflate-dev; \
		elif [ -f /etc/fedora-release ] || [ -f /etc/redhat-release ]; then \
			echo "Detected Fedora/RHEL. Installing via dnf..."; \
			sudo dnf install -y gcc-c++ cmake zstd-devel lz4-devel bzip2-devel zlib-devel libdeflate-devel; \
		elif [ -f /etc/arch-release ]; then \
			echo "Detected Arch Linux. Installing via pacman..."; \
			sudo pacman -S --noconfirm gcc cmake zstd lz4 bzip2 zlib libdeflate; \
		else \
			echo "Unsupported Linux distribution for automatic installation."; \
			echo "Please install dependencies manually as listed in README.md"; \
			exit 1; \
		fi \
	else \
		echo "Unsupported OS: $(UNAME_S)"; \
		exit 1; \
	fi

# Developer tool installation (bear, clang-tidy, clang-format, zig)
# These are only needed for linting and building musl static binaries.
# See CONTRIBUTING.md for details.
dev-deps:
	@echo "Installing developer tools (bear, clang-tidy, clang-format, zig)..."
	@if [ "$(UNAME_S)" = "Darwin" ]; then \
	    if ! command -v brew >/dev/null 2>&1; then \
	        echo "Error: Homebrew not found. Please install it from https://brew.sh"; \
	        exit 1; \
	    fi; \
	    echo "Installing via Homebrew..."; \
	    brew install bear llvm zig; \
	    echo ""; \
	    echo "NOTE: clang-tidy and clang-format are installed under llvm, not on PATH by default."; \
	    echo "Add the following to your shell profile, or override in make invocations:"; \
	    echo "  export PATH=\"$$(brew --prefix llvm)/bin:\$$PATH\""; \
	    echo "Or pass them explicitly:"; \
	    echo "  make lint CLANG_TIDY=$$(brew --prefix llvm)/bin/clang-tidy CLANG_FORMAT=$$(brew --prefix llvm)/bin/clang-format"; \
	elif [ "$(UNAME_S)" = "Linux" ]; then \
	    if [ -f /etc/debian_version ]; then \
	        echo "Detected Debian/Ubuntu. Installing via apt..."; \
	        sudo apt update && sudo apt install -y bear clang-tidy clang-format; \
	    elif [ -f /etc/fedora-release ] || [ -f /etc/redhat-release ]; then \
	        echo "Detected Fedora/RHEL. Installing via dnf..."; \
	        sudo dnf install -y bear clang-tools-extra; \
	    elif [ -f /etc/arch-release ]; then \
	        echo "Detected Arch Linux. Installing via pacman..."; \
	        sudo pacman -S --noconfirm bear clang; \
	    else \
	        echo "Unsupported Linux distribution."; \
	        echo "Please install bear, clang-tidy, and clang-format manually."; \
	    fi; \
	    echo ""; \
	    echo "Installing zig $(ZIG_VERSION)..."; \
	    ZIG_ARCH=$$(uname -m); \
	    ZIG_DIR="zig-linux-$${ZIG_ARCH}-$(ZIG_VERSION)"; \
	    ZIG_TARBALL="$${ZIG_DIR}.tar.xz"; \
	    ZIG_URL="https://ziglang.org/download/$(ZIG_VERSION)/$${ZIG_TARBALL}"; \
	    ZIG_DEST="$${HOME}/.local/bin"; \
	    mkdir -p "$${ZIG_DEST}"; \
	    echo "Downloading $${ZIG_URL}..."; \
	    curl -L "$${ZIG_URL}" | tar xJ -C /tmp; \
	    cp "/tmp/$${ZIG_DIR}/zig" "$${ZIG_DEST}/zig"; \
	    chmod +x "$${ZIG_DEST}/zig"; \
	    echo "zig installed to $${ZIG_DEST}/zig"; \
	    echo "Make sure $${ZIG_DEST} is in your PATH (add to ~/.bashrc or ~/.profile if needed)."; \
	else \
	    echo "Unsupported OS: $(UNAME_S)"; \
	    exit 1; \
	fi
	@echo ""
	@echo "Done. Run 'make check-dev-deps' to verify all tools are available."

# Verify developer tools are installed and print their versions
check-dev-deps:
	@echo "Checking developer tools..."
	@printf "  bear:         "; \
	    command -v bear >/dev/null 2>&1 \
	    && bear --version 2>&1 | head -1 \
	    || echo "NOT FOUND  (run: make dev-deps)"
	@printf "  clang-tidy:   "; \
	    command -v $(CLANG_TIDY) >/dev/null 2>&1 \
	    && $(CLANG_TIDY) --version 2>&1 | head -1 \
	    || echo "NOT FOUND  (run: make dev-deps)"
	@printf "  clang-format: "; \
	    command -v $(CLANG_FORMAT) >/dev/null 2>&1 \
	    && $(CLANG_FORMAT) --version 2>&1 | head -1 \
	    || echo "NOT FOUND  (run: make dev-deps)"
	@printf "  zig:          "; \
	    command -v $(ZIG) >/dev/null 2>&1 \
	    && $(ZIG) version \
	    || echo "NOT FOUND  (run: make dev-deps)"

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

# =============================================================================
# Lint & Static Analysis
# =============================================================================
#
# Requires: bear, clang-tidy, clang-format  (run: make dev-deps)
#
# Workflow:
#   1. Generate compile_commands.json once:  bear -- make
#   2. Check for issues:                     make lint
#   3. Auto-fix formatting:                  make lint-fix
#
# CLANG_TIDY and CLANG_FORMAT can be overridden on the command line if the
# tools are not on PATH (common on macOS after 'brew install llvm'):
#   make lint CLANG_TIDY=$(brew --prefix llvm)/bin/clang-tidy \
#             CLANG_FORMAT=$(brew --prefix llvm)/bin/clang-format

_LINT_CPP_SRCS := $(filter %.cpp,$(LINT_SRCS))

# Check formatting and run static analysis. Exits non-zero on any issue.
lint:
	@echo "==> clang-format (check)"
	@PATH="$(LLVM_BIN):$(PATH)" $(CLANG_FORMAT) --dry-run --Werror $(LINT_SRCS) || { echo "❌ Formatting issues found. Run 'make format'."; exit 1; }
	@echo "==> clang-tidy"
	@if [ ! -f compile_commands.json ]; then echo "❌ Error: compile_commands.json missing. Run 'bear -- make'."; exit 1; fi
	@if PATH="$(LLVM_BIN):$(PATH)" command -v run-clang-tidy >/dev/null 2>&1; then \
	    TIDY_ARGS=""; \
	    for arg in $(LINT_FLAGS); do TIDY_ARGS="$$TIDY_ARGS -extra-arg=$$arg"; done; \
	    OUTPUT=$$(PATH="$(LLVM_BIN):$(PATH)" run-clang-tidy -p . $$TIDY_ARGS 2>&1); \
	    echo "$$OUTPUT" | python3 scripts/lint_filter.py; \
	    FILTER_EXIT=$$?; \
	    if [ $$FILTER_EXIT -ne 0 ]; then exit $$FILTER_EXIT; fi; \
	else \
	    TIDY_ARGS=""; \
	    for arg in $(LINT_FLAGS); do TIDY_ARGS="$$TIDY_ARGS --extra-arg=$$arg"; done; \
	    OUTPUT=$$(PATH="$(LLVM_BIN):$(PATH)" $(CLANG_TIDY) $(_LINT_CPP_SRCS) -p . $$TIDY_ARGS 2>&1); \
	    echo "$$OUTPUT" | python3 scripts/lint_filter.py; \
	    FILTER_EXIT=$$?; \
	    if [ $$FILTER_EXIT -ne 0 ]; then exit $$FILTER_EXIT; fi; \
	fi
	@echo "✅ lint passed"

# Generate a structured report of linting issues for systematic resolution.
# Filters out system header errors and organizes issues by package.
lint-report:
	@echo "==> Generating lint reports..."
	@if PATH="$(LLVM_BIN):$(PATH)" command -v run-clang-tidy >/dev/null 2>&1; then \
	    TIDY_ARGS=""; \
	    for arg in $(LINT_FLAGS); do TIDY_ARGS="$$TIDY_ARGS -extra-arg=$$arg"; done; \
	    PATH="$(LLVM_BIN):$(PATH)" run-clang-tidy -p . $$TIDY_ARGS 2>&1 | python3 scripts/lint_filter.py --format md > LINT_REPORT.md || true; \
	    PATH="$(LLVM_BIN):$(PATH)" run-clang-tidy -p . $$TIDY_ARGS 2>&1 | python3 scripts/lint_filter.py --format json > LINT_REPORT.json || true; \
	else \
	    TIDY_ARGS=""; \
	    for arg in $(LINT_FLAGS); do TIDY_ARGS="$$TIDY_ARGS --extra-arg=$$arg"; done; \
	    PATH="$(LLVM_BIN):$(PATH)" $(CLANG_TIDY) $(_LINT_CPP_SRCS) -p . $$TIDY_ARGS 2>&1 | python3 scripts/lint_filter.py --format md > LINT_REPORT.md || true; \
	    PATH="$(LLVM_BIN):$(PATH)" $(CLANG_TIDY) $(_LINT_CPP_SRCS) -p . $$TIDY_ARGS 2>&1 | python3 scripts/lint_filter.py --format json > LINT_REPORT.json || true; \
	fi
	@echo "✅ Reports generated:"
	@echo "   - LINT_REPORT.md (Markdown, organized by package)"
	@echo "   - LINT_REPORT.json (JSON, machine-readable)"

# Apply clang-format fixes in place. Does not apply clang-tidy fixes
# automatically since those require careful review.
lint-fix: format

# Alias for lint-fix: reformat all source files in place.
format:
	@echo "==> clang-format (fix)"
	@$(CLANG_FORMAT) -i $(LINT_SRCS)
	@echo "    Done. Review changes with 'git diff' before committing."

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
	install -d $(DESTDIR)$(BINDIR)
	install -m 755 $(TARGET) $(DESTDIR)$(BINDIR)/

uninstall:
	rm -f $(DESTDIR)$(BINDIR)/$(TARGET)

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

# =============================================================================
# Musl Static Builds (truly portable Linux binaries — no glibc dependency)
# =============================================================================
#
# Uses 'zig c++' as a drop-in compiler replacement targeting musl libc.
# The resulting binaries are self-contained and run on any Linux, regardless
# of glibc version (RHEL 7 / glibc 2.17 and later, Alpine, etc).
#
# Requires: zig 0.16.0+ on PATH  (run: make dev-deps)
#
# Note: these targets compile on any OS that has zig, but are intended to
# run on Linux. Build on Linux CI for release artifacts.

# Verify zig is available before attempting musl builds.
zig-check:
	@command -v $(ZIG) >/dev/null 2>&1 || { \
	    echo ""; \
	    echo "  Error: zig not found on PATH."; \
	    echo "  Install with:  make dev-deps"; \
	    echo ""; \
	    exit 1; \
	}
	@echo "zig: $(shell $(ZIG) version)"

_MUSL_LINUX_GUARD = \
	if [ "$(UNAME_S)" != "Linux" ]; then \
	    echo ""; \
	    echo "  Error: musl static builds must run on Linux."; \
	    echo "  The system library .a files on macOS are not linkable into Linux binaries."; \
	    echo "  Run this target on a Linux machine or in CI."; \
	    echo ""; \
	    exit 1; \
	fi

# Linux x86_64 — musl static, SSE2 baseline (maximum portability)
dist-linux-x86_64-musl: zig-check
	@$(_MUSL_LINUX_GUARD)
	@mkdir -p $(DIST_DIR)
	$(MAKE) clean
	$(MAKE) STATIC=1 BUILD=release \
	        CXX="$(ZIG) c++ -target x86_64-linux-musl" \
	        ARCH_FLAGS="-march=x86-64 -mtune=generic" \
	        TARGET_NAME=$(DIST_DIR)/mar-linux-x86_64-musl \
	        all-internal
	@echo ""
	@echo "Built: $(DIST_DIR)/mar-linux-x86_64-musl  (musl static, x86_64 SSE2)"
	@file $(DIST_DIR)/mar-linux-x86_64-musl
	@ls -lh $(DIST_DIR)/mar-linux-x86_64-musl

# Linux x86_64 — musl static, SSE4.2 optimized (2008+ CPUs)
dist-linux-x86_64-sse42-musl: zig-check
	@$(_MUSL_LINUX_GUARD)
	@mkdir -p $(DIST_DIR)
	$(MAKE) clean
	$(MAKE) STATIC=1 BUILD=release \
	        CXX="$(ZIG) c++ -target x86_64-linux-musl" \
	        ARCH_FLAGS="-march=nehalem -mtune=generic" \
	        TARGET_NAME=$(DIST_DIR)/mar-linux-x86_64-sse42-musl \
	        all-internal
	@echo ""
	@echo "Built: $(DIST_DIR)/mar-linux-x86_64-sse42-musl  (musl static, SSE4.2)"
	@file $(DIST_DIR)/mar-linux-x86_64-sse42-musl
	@ls -lh $(DIST_DIR)/mar-linux-x86_64-sse42-musl

# Linux ARM64 — musl static (zig handles cross-compilation from any Linux host)
dist-linux-arm64-musl: zig-check
	@$(_MUSL_LINUX_GUARD)
	@mkdir -p $(DIST_DIR)
	$(MAKE) clean
	$(MAKE) STATIC=1 BUILD=release \
	        CXX="$(ZIG) c++ -target aarch64-linux-musl" \
	        ARCH_FLAGS="-march=armv8-a+simd -mtune=generic" \
	        TARGET_NAME=$(DIST_DIR)/mar-linux-arm64-musl \
	        all-internal
	@echo ""
	@echo "Built: $(DIST_DIR)/mar-linux-arm64-musl  (musl static, ARM64)"
	@file $(DIST_DIR)/mar-linux-arm64-musl
	@ls -lh $(DIST_DIR)/mar-linux-arm64-musl

# Note: macOS builds now use Homebrew for distribution.
# See 'make brew-formula' and RELEASING.md for details.

# Build all distribution binaries for the current platform.
#
# On Linux: builds musl static variants if zig is available (preferred for
# releases), otherwise falls back to glibc static variants.
# On macOS: builds a Universal Binary (ARM64 + x86_64).
dist-all:
	@mkdir -p $(DIST_DIR)
	@echo "Building all distribution binaries for current platform..."
	@if [ "$(UNAME_S)" = "Linux" ]; then \
	    if [ "$$(uname -m)" = "x86_64" ]; then \
	        if command -v $(ZIG) >/dev/null 2>&1; then \
	            echo "==> zig found: building musl static variants (portable, no glibc dependency)"; \
	            $(MAKE) dist-linux-x86_64-musl; \
	            $(MAKE) dist-linux-x86_64-sse42-musl; \
	        else \
	            echo "==> zig not found: building glibc static variants (run 'make dev-deps' to enable musl builds)"; \
	            $(MAKE) dist-linux-x86_64; \
	            $(MAKE) dist-linux-x86_64-sse42; \
	        fi \
	    elif [ "$$(uname -m)" = "aarch64" ]; then \
	        if command -v $(ZIG) >/dev/null 2>&1; then \
	            echo "==> zig found: building musl static ARM64"; \
	            $(MAKE) dist-linux-arm64-musl; \
	        else \
	            echo "==> zig not found: building glibc static ARM64 (run 'make dev-deps' to enable musl builds)"; \
	            $(MAKE) dist-linux-arm64; \
	        fi \
	    fi \
	elif [ "$(UNAME_S)" = "Darwin" ]; then \
	    echo "Note: macOS distribution is handled via Homebrew. See 'make brew-formula'."; \
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
	@echo "  make CXX=clang++  - Build using Clang instead of GCC"
	@echo "  make debug        - Build with debug symbols"
	@echo "  make static       - Build static binary"
	@echo "  make release      - Build optimized static binary"
	@echo "  make test         - Build and run tests"
	@echo ""
	@echo "Distribution Builds (Portable):"
	@echo "  make dist-all                      - Build all for current platform"
	@echo "                                       (prefers musl on Linux if zig is available)"
	@echo "  make dist-linux-x86_64-musl        - Linux musl static, SSE2  (recommended)"
	@echo "  make dist-linux-x86_64-sse42-musl  - Linux musl static, SSE4.2"
	@echo "  make dist-linux-arm64-musl         - Linux musl static, ARM64"
	@echo "  make dist-linux-x86_64             - Linux glibc static, SSE2"
	@echo "  make dist-linux-x86_64-sse42       - Linux glibc static, SSE4.2"
	@echo "  make dist-linux-x86_64-avx2        - Linux glibc static, AVX2"
	@echo "  make dist-linux-arm64              - Linux glibc static, ARM64"
	@echo "  make dist-macos-arm64              - macOS Apple Silicon"
	@echo "  make dist-macos-x86_64             - macOS Intel"
	@echo "  make dist-macos-universal          - macOS Universal (ARM64+x86_64)"
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
	@echo "  make STATIC=1        - Force static linking"
	@echo "  make NATIVE=1        - Force native optimization"
	@echo "  make LTO=1           - Enable link-time optimization"
	@echo "  make perf-smoke      - Run performance smoke tests"
	@echo "  make system-deps     - Install build dependencies (macOS/Linux)"
	@echo ""
	@echo "Lint & Static Analysis:"
	@echo "  bear -- make         - Generate compile_commands.json (run once before lint)"
	@echo "  make format          - Reformat all source files in place (clang-format)"
	@echo "  make lint            - Check formatting + run clang-tidy (requires compile_commands.json)"
	@echo "  make lint-fix        - Alias for 'make format'"
	@echo ""
	@echo "Developer Tools:"
	@echo "  make dev-deps        - Install dev tools: bear, clang-tidy, clang-format, zig"
	@echo "  make check-dev-deps  - Verify dev tools are installed and show versions"
	@echo ""
	@echo "See CONTRIBUTING.md for the full developer setup guide."
