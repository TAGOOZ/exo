//! A module for exposing Rust's libp2p datatypes over Pyo3
//!
//! TODO: right now we are coupled to libp2p's identity, but eventually we want to create our own
//!       independent identity type of some kind or another. This may require handshaking.
//!

pub mod ident;
pub mod multiaddr;

use std::sync::Mutex;

use cluster_membership::Peer;
use libp2p::identity::ed25519::Keypair;
use pyo3::{exceptions::PyException, prelude::*, types::PyString};
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

#[gen_stub_pyclass]
#[pyclass]
#[derive(Clone)]
pub struct PyKeypair(Keypair);

#[gen_stub_pymethods]
#[pymethods]
impl PyKeypair {
    #[staticmethod]
    fn generate() -> Self {
        Self(Keypair::generate())
    }
}

#[gen_stub_pyclass]
#[pyclass]
pub struct PyPeer(Mutex<Peer>);

#[gen_stub_pymethods]
#[pymethods]
impl PyPeer {
    #[staticmethod]
    fn init(kp: PyKeypair, namespace: String) -> PyResult<Self> {
        Ok(PyPeer(Mutex::new(
            Peer::new(kp.0.secret(), namespace).map_err(PyError)?.0,
        )))
    }
}

pub trait IntoPyErr {
    type T;
    fn convert(self) -> Self::T;
}
impl<T> IntoPyErr for Result<T, cluster_membership::Error> {
    type T = PyResult<T>;
    fn convert(self) -> Self::T {
        self.map_err(IntoPyErr::convert)
    }
}
impl IntoPyErr for cluster_membership::Error {
    type T = PyErr;
    fn convert(self) -> Self::T {}
}
#[gen_stub_pyclass]
#[pyclass(frozen, extends=PyException, name="AnyhowError")]
pub struct PyError(cluster_membership::Error);

#[gen_stub_pymethods]
#[pymethods]
impl PyError {
    #[new]
    #[pyo3(signature = (*args))]
    #[allow(unused_variables)]
    pub(crate) fn new(args: &Bound<'_, PyTuple>) -> Self {
        Self {}
    }

    fn __repr__(&self) -> String {
        format!("AnyhowError(\"{}\")", self.0)
    }

    fn __str__(&self) -> String {
        self.0.clone()
    }
}
