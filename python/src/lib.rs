pub mod py_bindings;

use pyo3::prelude::*;

#[pymodule]
fn _mar(m: &Bound<'_, PyModule>) -> PyResult<()> {
    py_bindings::register_module(m)?;
    Ok(())
}
