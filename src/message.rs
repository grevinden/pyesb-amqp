use std::collections::HashMap;

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes};
use pyo3::types::PyAnyMethods;
use serde_json;

use fe2o3_amqp::types::messaging::{MessageId, Properties};

/// AmqpMessage — 1:1 с полями Message<Body<Value>> из fe2o3-amqp.
/// Сложные типы разобраны в плоские HashMap<String, serde_json::Value>.
#[pyclass(name = "AmqpMessage", module = "pyesb_amqp", skip_from_py_object)]
#[derive(Clone, Debug)]
pub(crate) struct PyAmqpMessage {
    // --- Delivery ---
    #[pyo3(get)]
    pub delivery_id: i64,
    pub delivery_tag: Vec<u8>,
    #[pyo3(get)]
    pub message_format: Option<i64>,
    #[pyo3(get)]
    pub rcv_settle_mode: Option<String>,
    #[pyo3(get)]
    pub link_output_handle: u32,

    // --- Message ---
    pub header: Option<HashMap<String, serde_json::Value>>,
    pub delivery_annotations: Option<HashMap<String, serde_json::Value>>,
    pub message_annotations: Option<HashMap<String, serde_json::Value>>,
    pub properties: Option<Properties>,
    pub application_properties: Option<HashMap<String, String>>,
    pub body: Vec<u8>,
    pub footer: Option<HashMap<String, serde_json::Value>>,
}

// Helper methods — not exposed to Python
impl PyAmqpMessage {
    pub(crate) fn json_map<'py>(
        &self,
        py: Python<'py>,
        map: &Option<HashMap<String, serde_json::Value>>,
    ) -> Option<Py<PyAny>> {
        map.as_ref().map(|m| {
            let s = serde_json::to_string(m).unwrap_or_default();
            py.import("json")
                .and_then(|mod_| mod_.call_method1("loads", (s,)))
                .map(|b| b.into())
                .unwrap_or_else(|_| py.None())
        })
    }

    pub(crate) fn str_map<'py>(
        &self,
        py: Python<'py>,
        map: &Option<HashMap<String, String>>,
    ) -> Option<Py<PyAny>> {
        map.as_ref().map(|m| {
            let s = serde_json::to_string(m).unwrap_or_default();
            py.import("json")
                .and_then(|mod_| mod_.call_method1("loads", (s,)))
                .map(|b| b.into())
                .unwrap_or_else(|_| py.None())
        })
    }
}

#[pymethods]
impl PyAmqpMessage {
    #[getter]
    fn delivery_tag<'py>(&self, py: Python<'py>) -> Py<PyBytes> {
        PyBytes::new(py, &self.delivery_tag).into()
    }

    #[getter]
    fn body<'py>(&self, py: Python<'py>) -> Py<PyBytes> {
        PyBytes::new(py, &self.body).into()
    }

    #[getter]
    fn header<'py>(&self, py: Python<'py>) -> Option<Py<PyAny>> {
        self.json_map(py, &self.header)
    }

    #[getter]
    fn delivery_annotations<'py>(&self, py: Python<'py>) -> Option<Py<PyAny>> {
        self.json_map(py, &self.delivery_annotations)
    }

    #[getter]
    fn message_annotations<'py>(&self, py: Python<'py>) -> Option<Py<PyAny>> {
        self.json_map(py, &self.message_annotations)
    }

    #[getter]
    fn properties<'py>(&self, py: Python<'py>) -> Option<Py<PyAny>> {
        self.properties.as_ref().map(|props| {
            let d = pyo3::types::PyDict::new(py);

            // message_id: varies — int/bytes/str
            if let Some(ref v) = props.message_id {
                match v {
                    MessageId::Ulong(n) => d.set_item("message_id", *n).ok(),
                    MessageId::Uuid(uuid) => {
                        d.set_item("message_id", PyBytes::new(py, uuid.as_inner())).ok()
                    }
                    MessageId::Binary(b) => {
                        d.set_item("message_id", PyBytes::new(py, b.as_ref())).ok()
                    }
                    MessageId::String(s) => d.set_item("message_id", s).ok(),
                };
            }

            // user_id: bytes
            if let Some(ref v) = props.user_id {
                d.set_item("user_id", PyBytes::new(py, v.as_ref())).ok();
            }

            // to / reply_to: strings (Address = String)
            if let Some(ref v) = props.to {
                d.set_item("to", v).ok();
            }
            if let Some(ref v) = props.subject {
                d.set_item("subject", v).ok();
            }
            if let Some(ref v) = props.reply_to {
                d.set_item("reply_to", v).ok();
            }

            // correlation_id: same as message_id
            if let Some(ref v) = props.correlation_id {
                match v {
                    MessageId::Ulong(n) => d.set_item("correlation_id", *n).ok(),
                    MessageId::Uuid(uuid) => {
                        d.set_item("correlation_id", PyBytes::new(py, uuid.as_inner())).ok()
                    }
                    MessageId::Binary(b) => {
                        d.set_item("correlation_id", PyBytes::new(py, b.as_ref())).ok()
                    }
                    MessageId::String(s) => d.set_item("correlation_id", s).ok(),
                };
            }

            // content_type / content_encoding: Symbol → str
            if let Some(ref v) = props.content_type {
                d.set_item("content_type", v.to_string()).ok();
            }
            if let Some(ref v) = props.content_encoding {
                d.set_item("content_encoding", v.to_string()).ok();
            }

            // timestamps: ms since epoch as int
            if let Some(ref v) = props.absolute_expiry_time {
                d.set_item("absolute_expiry_time", v.milliseconds()).ok();
            }
            if let Some(ref v) = props.creation_time {
                d.set_item("creation_time", v.milliseconds()).ok();
            }

            // group_id / reply_to_group_id: strings
            if let Some(ref v) = props.group_id {
                d.set_item("group_id", v).ok();
            }
            if let Some(ref v) = props.group_sequence {
                d.set_item("group_sequence", *v).ok();
            }
            if let Some(ref v) = props.reply_to_group_id {
                d.set_item("reply_to_group_id", v).ok();
            }

            d.into()
        })
    }

    #[getter]
    fn application_properties<'py>(&self, py: Python<'py>) -> Option<Py<PyAny>> {
        self.str_map(py, &self.application_properties)
    }

    #[getter]
    fn footer<'py>(&self, py: Python<'py>) -> Option<Py<PyAny>> {
        self.json_map(py, &self.footer)
    }

    fn __repr__(&self) -> String {
        format!(
            "AmqpMessage(delivery_id={}, delivery_tag={}, body_len={})",
            self.delivery_id,
            hex::encode(&self.delivery_tag),
            self.body.len(),
        )
    }
}
