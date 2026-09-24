pub mod format;
pub mod checksum;
pub mod compression;
pub mod name_index;
pub mod reader;
pub mod writer;
pub mod mai;
pub mod remote;
pub mod async_io;
pub mod diff;
pub mod redact;

#[cfg(feature = "extension-module")]
pub mod py_bindings;

#[cfg(feature = "extension-module")]
use pyo3::prelude::*;

#[cfg(feature = "extension-module")]
#[pymodule]
fn _mar(m: &Bound<'_, PyModule>) -> PyResult<()> {
    py_bindings::register_module(m)?;
    Ok(())
}
