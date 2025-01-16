use flume::{bounded, Receiver, Sender};
use futures_util::StreamExt;
use tokio::net::{lookup_host, TcpSocket, TcpStream};
use tokio::{select, time};
use tracing::{debug, error};
use types::group::{ClientAtomicMetrics, PacketAtomicMetrics};

use crate::protocol::v3_mini;
use crate::protocol::v3_mini::v4::{ConnAck, Connect, ConnectReturnCode, Packet};
use crate::state::StateError;
use crate::MqttOptions;
use crate::{framed::Network, Transport};
use crate::{Incoming, NetworkOptions};

use crate::framed::AsyncReadWrite;

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

#[cfg(unix)]
use {std::path::Path, tokio::net::UnixStream};

use crate::tls;

use {
    crate::websockets::{split_url, validate_response_headers, UrlError},
    async_tungstenite::tungstenite::client::IntoClientRequest,
    ws_stream_tungstenite::WsStream,
};

#[cfg(feature = "proxy")]
use crate::proxy::ProxyError;

/// Critical errors during eventloop polling
#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    #[error("Mqtt state: {0}")]
    MqttState(#[from] StateError),
    #[error("Network timeout")]
    NetworkTimeout,
    #[error("Flush timeout")]
    FlushTimeout,
    #[error("Websocket: {0}")]
    Websocket(#[from] async_tungstenite::tungstenite::error::Error),
    #[error("Websocket Connect: {0}")]
    WsConnect(#[from] http::Error),
    #[error("TLS: {0}")]
    Tls(#[from] tls::Error),
    #[error("I/O: {0}")]
    Io(#[from] io::Error),
    #[error("Connection refused, return code: `{0:?}`")]
    ConnectionRefused(ConnectReturnCode),
    #[error("Expected ConnAck packet, received: ")]
    NotConnAck,
    #[error("Requests done")]
    RequestsDone,
    #[error("Invalid Url: {0}")]
    InvalidUrl(#[from] UrlError),
    #[cfg(feature = "proxy")]
    #[error("Proxy Connect: {0}")]
    Proxy(#[from] ProxyError),
    #[error("Websocket response validation error: ")]
    ResponseValidation(#[from] crate::websockets::ValidationError),
}

/// Eventloop with all the state of a connection
pub struct EventLoop {
    /// Options of the current mqtt connection
    pub mqtt_options: MqttOptions,
    /// Request stream
    requests_rx: Receiver<Packet>,
    /// Requests handle to send requests
    // pub(crate) requests_tx: Sender<Packet>,
    /// Network connection to the broker
    pub network: Option<Network>,
    pub network_options: NetworkOptions,
}

impl EventLoop {
    /// New MQTT `EventLoop`
    ///
    /// When connection encounters critical errors (like auth failure), user has a choice to
    /// access and update `options`, `state` and `requests`.
    pub async fn start(
        client_metrics: Arc<ClientAtomicMetrics>,
        mqtt_options: MqttOptions,
        cap: usize,
        packet_metrics: Arc<PacketAtomicMetrics>,
    ) -> Sender<Packet> {
        let (requests_tx, requests_rx) = bounded(cap);

        tokio::spawn(async move {
            let mut eventloop = EventLoop {
                mqtt_options,
                requests_rx,
                network: None,
                network_options: NetworkOptions::new(),
            };

            if let Some(local_ip) = &eventloop.mqtt_options.local_ip {
                eventloop.network_options.local_ip = Some(local_ip.clone());
            }

            let network = match eventloop.connect(&packet_metrics).await {
                Ok(network) => {
                    debug!("network connect success");
                    network
                }
                Err(e) => {
                    client_metrics
                        .error_cnt
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    error!("connect error: {:?}", e);
                    return;
                }
            };
            eventloop.run_long_time(network, packet_metrics);
        });

        requests_tx
    }

    pub async fn connect(
        &mut self,
        packet_metrics: &Arc<PacketAtomicMetrics>,
    ) -> Result<Network, ConnectionError> {
        let (network, connack) = match time::timeout(
            Duration::from_secs(self.network_options.connection_timeout()),
            connect(
                &self.mqtt_options,
                self.network_options.clone(),
                packet_metrics,
            ),
        )
        .await
        {
            Ok(inner) => inner?,
            Err(_) => return Err(ConnectionError::NetworkTimeout),
        };
        debug!("conn ack: {:?}", connack);
        packet_metrics
            .conn_ack
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(network)
    }

    fn run_long_time(self, mut network: Network, packet_metrics: Arc<PacketAtomicMetrics>) {
        tokio::spawn(async move {
            loop {
                select! {
                    packet = network.framed.next() => {
                        if !network.handle_incoming_packet(packet, &packet_metrics).await {
                            return
                        }
                    }
                    r = self.requests_rx.recv_async() => {
                        match r {
                            Ok(r) => {
                                if let Err(e) = network.write(r, &packet_metrics).await {
                                    error!("network write error: {:?}", e);
                                    return;
                                }
                                match time::timeout(Duration::from_secs(3), network.flush()).await {
                                    Ok(inner) => {
                                        match inner {
                                        Ok(_) => {}
                                        Err(e) => {
                                            error!("network Flush timeout, err :{:?}", e);
                                            return;
                                        }
                                    }
                                    },
                                    Err(_)=> {
                                        error!("network Flush timeout");
                                        return;
                                    }
                                };
                            }
                            Err(e) => {
                                error!("requests_rx recv error: {:?}", e);
                                return;
                            }
                        }
                    }
                }
            }
        });
    }

    fn handle_incoming_packet(
        packet: Option<Result<Packet, v3_mini::Error>>,
        packet_metrics: &Arc<PacketAtomicMetrics>,
    ) -> bool {
        match packet {
            Some(packet) => match packet {
                Ok(packet) => match packet {
                    Packet::Connect(connect) => todo!(),
                    Packet::ConnAck(conn_ack) => todo!(),
                    Packet::Publish(publish) => todo!(),
                    Packet::PubAck(pub_ack) => todo!(),
                    Packet::PubRec(pub_rec) => todo!(),
                    Packet::PubRel(pub_rel) => todo!(),
                    Packet::PubComp(pub_comp) => todo!(),
                    Packet::Subscribe(subscribe) => todo!(),
                    Packet::SubAck(sub_ack) => todo!(),
                    Packet::Unsubscribe(unsubscribe) => todo!(),
                    Packet::UnsubAck(unsub_ack) => todo!(),
                    Packet::PingReq => todo!(),
                    Packet::PingResp => todo!(),
                    Packet::Disconnect => todo!(),
                },
                Err(_) => todo!(),
            },
            None => false,
        }
    }

    pub fn network_options(&self) -> NetworkOptions {
        self.network_options.clone()
    }

    pub fn set_network_options(&mut self, network_options: NetworkOptions) -> &mut Self {
        self.network_options = network_options;
        self
    }
}

/// This stream internally processes requests from the request stream provided to the eventloop
/// while also consuming byte stream from the network and yielding mqtt packets as the output of
/// the stream.
/// This function (for convenience) includes internal delays for users to perform internal sleeps
/// between re-connections so that cancel semantics can be used during this sleep
async fn connect(
    mqtt_options: &MqttOptions,
    network_options: NetworkOptions,
    packet_metrics: &Arc<PacketAtomicMetrics>,
) -> Result<(Network, ConnAck), ConnectionError> {
    // connect to the broker
    let mut network = network_connect(mqtt_options, network_options).await?;

    // make MQTT connection request (which internally awaits for ack)
    let connack = mqtt_connect(mqtt_options, &mut network, &packet_metrics).await?;

    Ok((network, connack))
}

pub(crate) async fn socket_connect(
    host: String,
    network_options: NetworkOptions,
) -> io::Result<TcpStream> {
    let addrs = lookup_host(host).await?;
    let mut last_err = None;

    for addr in addrs {
        let socket = match addr {
            SocketAddr::V4(_) => TcpSocket::new_v4()?,
            SocketAddr::V6(_) => TcpSocket::new_v6()?,
        };

        socket.set_nodelay(network_options.tcp_nodelay)?;

        if let Some(send_buff_size) = network_options.tcp_send_buffer_size {
            socket.set_send_buffer_size(send_buff_size).unwrap();
        }
        if let Some(recv_buffer_size) = network_options.tcp_recv_buffer_size {
            socket.set_recv_buffer_size(recv_buffer_size).unwrap();
        }

        #[cfg(any(target_os = "android", target_os = "fuchsia", target_os = "linux"))]
        {
            if let Some(bind_device) = &network_options.bind_device {
                // call the bind_device function only if the bind_device network option is defined
                // If binding device is None or an empty string it removes the binding,
                // which is causing PermissionDenied errors in AWS environment (lambda function).
                socket.bind_device(Some(bind_device.as_bytes()))?;
            }
        }

        if let Some(local_ip) = &network_options.local_ip {
            let local_addr = format!("{}:0", local_ip).parse().unwrap();
            socket.bind(local_addr)?;
        }

        match socket.connect(addr).await {
            Ok(s) => return Ok(s),
            Err(e) => {
                last_err = Some(e);
            }
        };
    }

    Err(last_err.unwrap_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "could not resolve to any address",
        )
    }))
}

async fn network_connect(
    options: &MqttOptions,
    network_options: NetworkOptions,
) -> Result<Network, ConnectionError> {
    // Process Unix files early, as proxy is not supported for them.
    #[cfg(unix)]
    if matches!(options.transport(), Transport::Unix) {
        let file = options.broker_addr.as_str();
        let socket = UnixStream::connect(Path::new(file)).await?;
        let network = Network::new(
            socket,
            options.max_incoming_packet_size,
            options.max_outgoing_packet_size,
        );
        return Ok(network);
    }

    // For websockets domain and port are taken directly from `broker_addr` (which is a url).
    let (domain, port) = match options.transport() {
        Transport::Ws => split_url(&options.broker_addr)?,
        Transport::Wss(_) => split_url(&options.broker_addr)?,
        _ => options.broker_address(),
    };

    let tcp_stream: Box<dyn AsyncReadWrite> = {
        #[cfg(feature = "proxy")]
        match options.proxy() {
            Some(proxy) => proxy.connect(&domain, port, network_options).await?,
            None => {
                let addr = format!("{domain}:{port}");
                let tcp = socket_connect(addr, network_options).await?;
                Box::new(tcp)
            }
        }
        #[cfg(not(feature = "proxy"))]
        {
            let addr = format!("{domain}:{port}");
            let tcp = socket_connect(addr, network_options).await?;
            Box::new(tcp)
        }
    };

    let network = match options.transport() {
        Transport::Tcp => Network::new(
            tcp_stream,
            options.max_incoming_packet_size,
            options.max_outgoing_packet_size,
        ),
        Transport::Tls(tls_config) => {
            let socket =
                tls::tls_connect(&options.broker_addr, options.port, &tls_config, tcp_stream)
                    .await?;
            Network::new(
                socket,
                options.max_incoming_packet_size,
                options.max_outgoing_packet_size,
            )
        }
        #[cfg(unix)]
        Transport::Unix => unreachable!(),
        Transport::Ws => todo!(),
        Transport::Wss(tls_configuration) => todo!(),
        // Transport::Ws => {
        //     let mut request = options.broker_addr.as_str().into_client_request()?;
        //     request
        //         .headers_mut()
        //         .insert("Sec-WebSocket-Protocol", "mqtt".parse().unwrap());

        //     if let Some(request_modifier) = options.request_modifier() {
        //         request = request_modifier(request).await;
        //     }

        //     let (socket, response) =
        //         async_tungstenite::tokio::client_async(request, tcp_stream).await?;
        //     validate_response_headers(response)?;

        //     Network::new(
        //         WsStream::new(socket),
        //         options.max_incoming_packet_size,
        //         options.max_outgoing_packet_size,
        //     )
        // }
        // Transport::Wss(tls_config) => {
        //     let mut request = options.broker_addr.as_str().into_client_request()?;
        //     request
        //         .headers_mut()
        //         .insert("Sec-WebSocket-Protocol", "mqtt".parse().unwrap());

        //     if let Some(request_modifier) = options.request_modifier() {
        //         request = request_modifier(request).await;
        //     }

        //     let connector = tls::rustls_connector(&tls_config).await?;

        //     let (socket, response) = async_tungstenite::tokio::client_async_tls_with_connector(
        //         request,
        //         tcp_stream,
        //         Some(connector),
        //     )
        //     .await?;
        //     validate_response_headers(response)?;

        //     Network::new(
        //         WsStream::new(socket),
        //         options.max_incoming_packet_size,
        //         options.max_outgoing_packet_size,
        //     )
        // }
    };

    Ok(network)
}

async fn mqtt_connect(
    options: &MqttOptions,
    network: &mut Network,
    packet_metrics: &Arc<PacketAtomicMetrics>,
) -> Result<ConnAck, ConnectionError> {
    let mut connect = Connect::new(options.client_id());
    connect.keep_alive = options.keep_alive();
    connect.clean_session = options.clean_session();
    // connect.last_will = options.last_will();
    connect.login = options.credentials();

    // send mqtt connect packet
    network
        .write(Packet::Connect(connect), &packet_metrics)
        .await?;
    network.flush().await?;

    // validate connack
    match network.read().await? {
        Incoming::ConnAck(connack) if connack.code == ConnectReturnCode::Success => Ok(connack),
        Incoming::ConnAck(connack) => Err(ConnectionError::ConnectionRefused(connack.code)),
        _packet => Err(ConnectionError::NotConnAck),
    }
}
