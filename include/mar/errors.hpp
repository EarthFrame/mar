#pragma once

#include <stdexcept>
#include <string>

namespace mar {

// Base exception for all MAR errors
class MarError : public std::runtime_error {
public:
    explicit MarError(const std::string& msg) : std::runtime_error(msg) {}
};

// Archive format or structure errors
class InvalidArchiveError : public MarError {
public:
    explicit InvalidArchiveError(const std::string& msg)
        : MarError("Invalid archive: " + msg) {}
};

// Checksum verification errors
class ChecksumError : public MarError {
public:
    explicit ChecksumError(const std::string& msg)
        : MarError("Checksum error: " + msg) {}
};

// Compression/decompression errors
class CompressionError : public MarError {
public:
    explicit CompressionError(const std::string& msg)
        : MarError("Compression error: " + msg) {}
};

// I/O errors
class IOError : public MarError {
public:
    explicit IOError(const std::string& msg)
        : MarError("I/O error: " + msg) {}
};

// Unsupported feature errors
class UnsupportedError : public MarError {
public:
    explicit UnsupportedError(const std::string& msg)
        : MarError("Unsupported: " + msg) {}
};

} // namespace mar
