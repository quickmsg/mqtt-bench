use std::{fmt, marker::PhantomData, rc::Rc};

/// Mqtt client
pub struct Client {
    // io: IoBoxed,
    // shared: Rc<MqttShared>,
    // keepalive: Seconds,
    session_present: bool,
    // config: DispatcherConfig,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("v3::Client")
            .field("keepalive", &self.keepalive)
            .field("session_present", &self.session_present)
            .field("max_receive", &self.max_receive)
            .field("config", &self.config)
            .finish()
    }
}

impl Client {
    /// Construct new `Dispatcher` instance with outgoing messages stream.
    pub(super) fn new(
        // io: IoBoxed,
        // shared: Rc<MqttShared>,
        session_present: bool,
        // keepalive_timeout: Seconds,
        // config: DispatcherConfig,
    ) -> Self {
        Client {
            io,
            shared,
            session_present,
            max_receive,
            config,
            keepalive: keepalive_timeout,
        }
    }
}

impl Client {
    #[inline]
    /// Get client sink
    pub fn sink(&self) -> MqttSink {
        MqttSink::new(self.shared.clone())
    }

    #[inline]
    /// Indicates whether there is already stored Session state
    pub fn session_present(&self) -> bool {
        self.session_present
    }

    /// Configure mqtt resource for a specific topic
    pub fn resource<T, F, U>(self, address: T, service: F) -> ClientRouter<U::Error, U::Error>
    where
        T: IntoPattern,
        F: IntoService<U, Publish>,
        U: Service<Publish, Response = ()> + 'static,
    {
        let mut builder = Router::build();
        builder.path(address, 0);
        let handlers = vec![Pipeline::new(boxed::service(service.into_service()))];

        ClientRouter {
            builder,
            handlers,
            io: self.io,
            shared: self.shared,
            keepalive: self.keepalive,
            config: self.config,
            max_receive: self.max_receive,
            _t: PhantomData,
        }
    }

    /// Run client with default control messages handler.
    ///
    /// Default handler closes connection on any control message.
    pub async fn start_default(self) {
        if self.keepalive.non_zero() {
            let _ =
                ntex_util::spawn(keepalive(MqttSink::new(self.shared.clone()), self.keepalive));
        }

        let dispatcher = create_dispatcher(
            self.shared.clone(),
            self.max_receive,
            fn_service(|pkt| Ready::Ok(Either::Right(pkt))),
            fn_service(|msg: Control<()>| Ready::<_, ()>::Ok(msg.disconnect())),
        );

        let _ = Dispatcher::new(self.io, self.shared.clone(), dispatcher, &self.config).await;
    }

    /// Run client with provided control messages handler
    pub async fn start<F, S, E>(self, service: F) -> Result<(), MqttError<E>>
    where
        E: 'static,
        F: IntoService<S, Control<E>> + 'static,
        S: Service<Control<E>, Response = ControlAck, Error = E> + 'static,
    {
        if self.keepalive.non_zero() {
            let _ =
                ntex_util::spawn(keepalive(MqttSink::new(self.shared.clone()), self.keepalive));
        }

        let dispatcher = create_dispatcher(
            self.shared.clone(),
            self.max_receive,
            fn_service(|pkt| Ready::Ok(Either::Right(pkt))),
            service.into_service(),
        );

        Dispatcher::new(self.io, self.shared.clone(), dispatcher, &self.config).await
    }

    /// Get negotiated io stream and codec
    pub fn into_inner(self) -> (IoBoxed, codec::Codec) {
        (self.io, self.shared.codec.clone())
    }
}