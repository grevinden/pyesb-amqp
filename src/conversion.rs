use std::collections::HashMap;

use fe2o3_amqp::types::{
    messaging::{ApplicationProperties, Body, Header, Properties},
    primitives::{SimpleValue, Value},
};
use fe2o3_amqp::link::delivery::DeliveryInfo;
use fe2o3_amqp::Delivery;

use crate::message::PyAmqpMessage;

/// Plain data struct used before conversion to the Python class.
pub(crate) struct MessageData {
    // Delivery
    pub delivery_id: i64,
    pub delivery_tag: Vec<u8>,
    pub message_format: Option<i64>,
    pub rcv_settle_mode: Option<String>,
    pub link_output_handle: u32,

    // Message
    pub header: Option<HashMap<String, serde_json::Value>>,
    pub delivery_annotations: Option<HashMap<String, serde_json::Value>>,
    pub message_annotations: Option<HashMap<String, serde_json::Value>>,
    pub properties: Option<Properties>,
    pub application_properties: HashMap<String, String>,
    pub body: Vec<u8>,
    pub footer: Option<HashMap<String, serde_json::Value>>,
}

pub(crate) fn delivery_to_data(delivery: &Delivery<Body<Value>>) -> MessageData {
    let message = delivery.message();

    // -- Delivery fields --------------------------------------------
    let delivery_id = *delivery.delivery_id() as i64;
    let delivery_tag = delivery.delivery_tag().as_ref().to_vec();
    let message_format = delivery.message_format().map(|mf| mf as i64);
    let info = DeliveryInfo::from(delivery);
    let rcv_settle_mode = info
        .rcv_settle_mode()
        .as_ref()
        .map(|m| format!("{:?}", m));
    let link_output_handle = delivery.handle().0;

    // -- body -------------------------------------------------------
    let body = body_to_bytes(delivery.body());

    // -- header -----------------------------------------------------
    let header = message.header.as_ref().map(header_to_json);

    // -- annotations (skip complex type conversion) -----------------
    let delivery_annotations = message
        .delivery_annotations
        .as_ref()
        .and_then(|ann| serde_json::to_value(ann).ok())
        .and_then(|v| match v {
            serde_json::Value::Object(obj) if !obj.is_empty() => {
                let mut map = HashMap::new();
                for (k, v) in obj {
                    map.insert(k, v);
                }
                Some(map)
            }
            _ => None,
        });

    let message_annotations = message
        .message_annotations
        .as_ref()
        .and_then(|ann| serde_json::to_value(ann).ok())
        .and_then(|v| match v {
            serde_json::Value::Object(obj) if !obj.is_empty() => {
                let mut map = HashMap::new();
                for (k, v) in obj {
                    map.insert(k, v);
                }
                Some(map)
            }
            _ => None,
        });

    let footer = message
        .footer
        .as_ref()
        .and_then(|ann| serde_json::to_value(ann).ok())
        .and_then(|v| match v {
            serde_json::Value::Object(obj) if !obj.is_empty() => {
                let mut map = HashMap::new();
                for (k, v) in obj {
                    map.insert(k, v);
                }
                Some(map)
            }
            _ => None,
        });

    // -- properties -------------------------------------------------
    let properties = message.properties.clone();

    // -- application_properties -------------------------------------
    let application_properties = message
        .application_properties
        .as_ref()
        .map(|ap| extract_properties(Some(ap)));

    MessageData {
        delivery_id,
        delivery_tag,
        message_format,
        rcv_settle_mode,
        link_output_handle,
        header,
        delivery_annotations,
        message_annotations,
        properties,
        application_properties: application_properties.unwrap_or_default(),
        body,
        footer,
    }
}

impl From<MessageData> for PyAmqpMessage {
    fn from(m: MessageData) -> Self {
        Self {
            delivery_id: m.delivery_id,
            delivery_tag: m.delivery_tag,
            message_format: m.message_format,
            rcv_settle_mode: m.rcv_settle_mode,
            link_output_handle: m.link_output_handle,
            header: m.header,
            delivery_annotations: m.delivery_annotations,
            message_annotations: m.message_annotations,
            properties: m.properties,
            application_properties: Some(m.application_properties),
            body: m.body,
            footer: m.footer,
        }
    }
}

pub(crate) fn header_to_json(hdr: &Header) -> HashMap<String, serde_json::Value> {
    let mut m = HashMap::new();
    m.insert("durable".into(), serde_json::Value::Bool(hdr.durable));
    m.insert("priority".into(), serde_json::json!(hdr.priority.0));
    if let Some(ttl) = &hdr.ttl {
        m.insert("ttl".into(), serde_json::json!(ttl));
    }
    m.insert(
        "first_acquirer".into(),
        serde_json::Value::Bool(hdr.first_acquirer),
    );
    m.insert(
        "delivery_count".into(),
        serde_json::json!(hdr.delivery_count),
    );
    m
}

pub(crate) fn body_to_bytes(body: &Body<Value>) -> Vec<u8> {
    match body {
        Body::Data(batch) => batch
            .iter()
            .flat_map(|data| data.0.as_ref().to_vec())
            .collect(),
        Body::Value(amqp_val) => match &amqp_val.0 {
            Value::Binary(b) => b.as_ref().to_vec(),
            Value::String(s) => s.as_bytes().to_vec(),
            other => serde_json::to_vec(other).unwrap_or_default(),
        },
        Body::Sequence(batch) => serde_json::to_vec(batch).unwrap_or_default(),
        Body::Empty => vec![],
    }
}

pub(crate) fn extract_properties(app_props: Option<&ApplicationProperties>) -> HashMap<String, String> {
    let mut map = HashMap::new();
    if let Some(props) = app_props {
        for (key, val) in props.iter() {
            let s = simple_value_to_string(val);
            map.insert(key.clone(), s);
        }
    }
    map
}

pub(crate) fn simple_value_to_string(val: &SimpleValue) -> String {
    match val {
        SimpleValue::String(s) => s.clone(),
        SimpleValue::Symbol(s) => s.to_string(),
        SimpleValue::Binary(b) => String::from_utf8_lossy(b.as_ref()).to_string(),
        SimpleValue::Null => String::new(),
        other => serde_json::to_string(other).unwrap_or_else(|_| format!("{other:?}")),
    }
}
