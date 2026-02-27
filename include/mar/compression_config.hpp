#pragma once

// Centralized build-time feature detection for compression backends.

#if __has_include(<zstd.h>)
    #define HAVE_ZSTD 1
#else
    #define HAVE_ZSTD 0
#endif

#if __has_include(<zlib.h>)
    #define HAVE_ZLIB 1
#else
    #define HAVE_ZLIB 0
#endif

#if __has_include(<lz4.h>)
    #define HAVE_LZ4 1
#else
    #define HAVE_LZ4 0
#endif

#if __has_include(<libdeflate.h>)
    #define HAVE_LIBDEFLATE 1
#else
    #define HAVE_LIBDEFLATE 0
#endif

// BZIP2 requires explicit opt-in since header may exist without library.
#if defined(MAR_HAVE_BZIP2) && __has_include(<bzlib.h>)
    #define HAVE_BZIP2 1
#else
    #define HAVE_BZIP2 0
#endif
