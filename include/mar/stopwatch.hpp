/**
 * @file stopwatch.hpp
 * @brief High-resolution stopwatch for performance measurement
 */

#pragma once

#include <chrono>
#include <iostream>
#include <iomanip>
#include <sstream>
#include <string>

namespace mar {

/**
 * @brief High-resolution stopwatch using steady_clock
 * 
 * Uses std::chrono::steady_clock for monotonic timing that isn't affected
 * by system clock adjustments. Provides sub-microsecond resolution on
 * most platforms.
 * 
 * Example:
 *   Stopwatch sw;
 *   // ... do work ...
 *   sw.stop();
 *   std::cerr << sw.report() << std::endl;
 */
class Stopwatch {
public:
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;
    using Duration = Clock::duration;

    Stopwatch() : start_(Clock::now()), stopped_(false) {}

    /// Start or restart the stopwatch
    void start() {
        start_ = Clock::now();
        stopped_ = false;
    }

    /// Stop the stopwatch
    void stop() {
        if (!stopped_) {
            end_ = Clock::now();
            stopped_ = true;
        }
    }

    /// Elapsed time in seconds (as floating point)
    double elapsed_seconds() const {
        auto end = stopped_ ? end_ : Clock::now();
        return std::chrono::duration<double>(end - start_).count();
    }

    /// Elapsed time in milliseconds
    double elapsed_ms() const {
        return elapsed_seconds() * 1000.0;
    }

    /// Elapsed time in microseconds
    double elapsed_us() const {
        return elapsed_seconds() * 1000000.0;
    }

    /// Elapsed time as duration
    Duration elapsed() const {
        auto end = stopped_ ? end_ : Clock::now();
        return end - start_;
    }

    /// Format elapsed time as human-readable string
    std::string format() const {
        double secs = elapsed_seconds();
        
        std::ostringstream oss;
        oss << std::fixed;
        
        if (secs < 0.001) {
            oss << std::setprecision(1) << (secs * 1000000.0) << " µs";
        } else if (secs < 1.0) {
            oss << std::setprecision(2) << (secs * 1000.0) << " ms";
        } else if (secs < 60.0) {
            oss << std::setprecision(3) << secs << " s";
        } else {
            int mins = static_cast<int>(secs) / 60;
            double remaining = secs - (mins * 60);
            oss << mins << "m " << std::setprecision(2) << remaining << "s";
        }
        
        return oss.str();
    }

    /// Generate a report line suitable for stderr
    std::string report(const std::string& label = "Elapsed") const {
        return label + ": " + format();
    }

private:
    TimePoint start_;
    TimePoint end_;
    bool stopped_;
};

/**
 * @brief RAII stopwatch that reports on destruction
 * 
 * Useful for timing a scope:
 *   {
 *       ScopedStopwatch sw("Operation");
 *       // ... do work ...
 *   } // prints "Operation: 1.234 s" to stderr
 */
class ScopedStopwatch {
public:
    explicit ScopedStopwatch(const std::string& label, bool enabled = true)
        : label_(label), enabled_(enabled) {}

    ~ScopedStopwatch() {
        if (enabled_) {
            sw_.stop();
            std::cerr << sw_.report(label_) << std::endl;
        }
    }

    // Non-copyable
    ScopedStopwatch(const ScopedStopwatch&) = delete;
    ScopedStopwatch& operator=(const ScopedStopwatch&) = delete;

    /// Access the underlying stopwatch
    Stopwatch& stopwatch() { return sw_; }
    const Stopwatch& stopwatch() const { return sw_; }

private:
    Stopwatch sw_;
    std::string label_;
    bool enabled_;
};

} // namespace mar
