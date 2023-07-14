mod h3o_polars_ext_mod;

use pyo3::prelude::*;
use pyo3_polars::PySeries;
use pyo3_polars::error::PyPolarsErr;
use polars::prelude::*;


#[pyfunction]
fn cell_to_parent(pysrs: PySeries, res: u8) -> PyResult<PySeries> {
    let sr: Series = pysrs.into();
    let sr = h3o_polars_ext_mod::cell_to_parent(sr, res).map_err(PyPolarsErr::from)?;
    Ok(PySeries(sr))
}

#[pyfunction]
fn grid_ring(pysrs: PySeries, k: u32) -> PyResult<PySeries> {
    let sr: Series = pysrs.into();
    let sr = h3o_polars_ext_mod::grid_ring(sr, k).map_err(PyPolarsErr::from)?;
    Ok(PySeries(sr))
}

/// A Python module implemented in Rust.
#[pymodule]
fn h3o_polars_ext(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(cell_to_parent, m)?)?;
    m.add_function(wrap_pyfunction!(grid_ring, m)?)?;
    Ok(())
}