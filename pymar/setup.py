import os
import sys
from setuptools import setup, Extension
from setuptools.command.build_ext import build_ext
import subprocess
import pybind11

class Pybind11BuildExt(build_ext):
    def build_extensions(self):
        # Detect libraries similar to Makefile
        def check_lib(lib):
            try:
                subprocess.check_call(['pkg-config', '--exists', lib])
                return True
            except:
                return False

        opts = []
        libs = []
        
        if check_lib('libzstd'):
            opts.append('-DMAR_HAVE_ZSTD=1')
            libs.append('zstd')
        if check_lib('zlib'):
            libs.append('z')
        if check_lib('liblz4'):
            opts.append('-DMAR_HAVE_LZ4=1')
            libs.append('lz4')
        if check_lib('libdeflate'):
            opts.append('-DMAR_HAVE_LIBDEFLATE=1')
            libs.append('deflate')
        
        # Check for bzip2
        try:
            subprocess.check_call(['ldconfig', '-p'], stdout=subprocess.DEVNULL)
            opts.append('-DMAR_HAVE_BZIP2=1')
            libs.append('bz2')
        except:
            pass

        for ext in self.extensions:
            ext.extra_compile_args.extend(opts)
            ext.libraries.extend(libs)
            if sys.platform == 'darwin':
                ext.extra_compile_args.append('-stdlib=libc++')
                ext.extra_compile_args.append('-mmacosx-version-min=10.15')

        super().build_extensions()

# Define the extension module
mar_root_rel = ".."
src_dir = os.path.join(mar_root_rel, "src")
include_dir = os.path.join(mar_root_rel, "include")
deps_dir = os.path.join(mar_root_rel, "deps")

sources = [
    "pymar/_mar.cpp",
    os.path.join(src_dir, "format.cpp"),
    os.path.join(src_dir, "checksum.cpp"),
    os.path.join(src_dir, "compression.cpp"),
    os.path.join(src_dir, "compression_gzip.cpp"),
    os.path.join(src_dir, "compression_zstd.cpp"),
    os.path.join(src_dir, "compression_lz4.cpp"),
    os.path.join(src_dir, "compression_bzip2.cpp"),
    os.path.join(src_dir, "sections.cpp"),
    os.path.join(src_dir, "name_index.cpp"),
    os.path.join(src_dir, "reader.cpp"),
    os.path.join(src_dir, "writer.cpp"),
    os.path.join(src_dir, "file_descriptor_manager.cpp"),
    os.path.join(src_dir, "async_io.cpp"),
    os.path.join(src_dir, "thread_pool.cpp"),
    os.path.join(src_dir, "redact.cpp"),
    os.path.join(src_dir, "diff.cpp"),
    os.path.join(src_dir, "index_registry.cpp"),
    os.path.join(src_dir, "index_minhash.cpp"),
    os.path.join(src_dir, "index_vector.cpp"),
    os.path.join(src_dir, "index_bm25.cpp"),
    os.path.join(src_dir, "index_genomic.cpp"),
    os.path.join(src_dir, "index_email.cpp"),
    os.path.join(src_dir, "index_timeseries.cpp"),
    os.path.join(src_dir, "embed_server.cpp"),
]

extra_objects = []
# Link against libblake3.a if it exists
libblake3_path = os.path.join(deps_dir, "BLAKE3", "c", "libblake3.a")
if os.path.exists(os.path.join(os.path.dirname(__file__), libblake3_path)):
    extra_objects.append(libblake3_path)

ext_modules = [
    Extension(
        "pymar._mar",
        sources=sources,
        include_dirs=[
            "pymar",
            include_dir,
            deps_dir,
            os.path.join(deps_dir, "simde"),
            os.path.join(deps_dir, "hnswlib"),
            os.path.join(deps_dir, "BLAKE3", "c"),
            pybind11.get_include(),
            pybind11.get_include(user=True),
        ],
        language="c++",
        extra_compile_args=["-std=c++17", "-O3", "-DHAVE_BLAKE3=1"],
        extra_objects=extra_objects,
    ),
]

setup(
    name="pymar",
    ext_modules=ext_modules,
    cmdclass={"build_ext": Pybind11BuildExt},
)
