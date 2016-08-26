use super::{multiplex, Message, Error, RequestId, ServerService, Transport};
use reactor::{Task, Tick};
use std::io;

/// A server `Task` that dispatches `Transport` messages to a `Service` using
/// protocol multiplexing.
pub struct Server<S, T>
    where S: ServerService,
          T: Transport,
{
    inner: multiplex::Multiplex<Dispatch<S>, T>,
}

struct Dispatch<S: ServerService> {
    // The service handling the connection
    service: S,
    // Don't look at this, it is terrible
    in_flight: (),
}

impl<T, S, E> Server<S, T>
    where T: Transport<Error = E>,
          S: ServerService<Req = T::Out, Resp = T::In, Body = T::BodyIn, Error = E>,
          E: From<Error<E>> + Send + 'static,
{
    /// Create a new pipeline `Server` dispatcher with the given service and
    /// transport
    pub fn new(service: S, transport: T) -> io::Result<Server<S, T>> {
        let dispatch = Dispatch {
            service: service,
            in_flight: (),
        };

        // Create the pipeline dispatcher
        let pipeline = try!(multiplex::Multiplex::new(dispatch, transport));

        // Return the server task
        Ok(Server { inner: pipeline })
    }
}

impl<S> multiplex::Dispatch for Dispatch<S>
    where S: ServerService,
{
    type InMsg = S::Resp;
    type InBody = S::Body;
    type InBodyStream = S::BodyStream;
    type OutMsg = S::Req;
    type Error = S::Error;

    /// Process an out message
    fn dispatch(&mut self, request_id: RequestId, message: Self::OutMsg) -> io::Result<()> {
        unimplemented!();
    }

    /// Poll the next completed message
    fn poll(&mut self) -> Option<(RequestId, Message<Self::InMsg, Self::InBodyStream>)> {
        unimplemented!();
    }

    /// RPC currently in flight
    fn has_in_flight(&self) -> bool {
        unimplemented!();
    }
}

impl<T, S, E> Task for Server<S, T>
    where T: Transport<Error = E>,
          S: ServerService<Req = T::Out, Resp = T::In, Body = T::BodyIn, Error = E>,
          E: From<Error<E>> + Send + 'static,
{
    fn tick(&mut self) -> io::Result<Tick> {
        unimplemented!();
        // self.inner.tick()
    }
}
