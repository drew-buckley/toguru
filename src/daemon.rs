use crate::actuator::mqtt_zigbee_switch::MqttBrokerConnection;

pub async fn run_daemon(config: config::Daemon) -> Result<(), anyhow::Error> {
    let config::Daemon {
        service,
        toggles,
        mqtt_brokers
    };
    let mqtt_brokers = Vec::from_iter(mqtt_brokers.drain(..).map(|b| MqttBrokerConnection::new(b.client_id, b.host, b.port, b.base_topic, set_buffer_capacity, mqtt_client_capacity, switch_timeout)))
    Ok(())
}

pub mod config {
    use std::net::IpAddr;

    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct Daemon {
        pub service: Service,
        #[serde(rename = "toggle")]
        pub toggles: Vec<Toggle>,
        #[serde(rename = "mqtt-broker")]
        pub mqtt_brokers: Vec<MqttBroker>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct Service {
        pub bind_addr: IpAddr,
        pub bind_port: u16,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct Toggle {
        pub id: String,
        pub pretty: Option<String>,
        #[serde(flatten)]
        pub actuator: Actuator,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case", tag = "type")]
    pub enum Actuator {
        PhoneyBaloney,
        Mqtt(MqttActuator),
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct MqttActuator {
        pub broker_name: String,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct MqttBroker {
        pub name: String,
        pub host: String,
        #[serde(default = "defaults::mqtt_broker_port")]
        pub port: u16,
        #[serde(default = "defaults::mqtt_broker_client_id")]
        pub client_id: String,
    }

    mod defaults {
        use std::net::{IpAddr, Ipv4Addr};

        pub(super) fn service_bind_addr() -> IpAddr {
            Ipv4Addr::LOCALHOST.into()
        }

        pub(super) fn service_bind_port() -> u16 {
            8080
        }

        pub(super) fn mqtt_broker_port() -> u16 {
            1883
        }

        pub(super) fn mqtt_broker_client_id() -> String {
            "toguru-client".into()
        }
    }
}
