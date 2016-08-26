use super::{Frame, Message, Error, RequestId, Transport};
use reactor::{Task, Tick};
use util::future::{Await, AwaitStream, BusySender, Sender};
use futures::stream::Stream;
use std::io;
use std::collections::HashMap;

/*
 * TODO:
 *
 * - Cap the max number of in-flight connections
 *
 */

/// Configuration values for `Multiplex`
pub struct Config {
    max_out_body_chunks: usize,
}

/// Provides protocol multiplexing functionality in a generic way over clients
/// and servers. Used internally by `multiplex::Client` and
/// `multiplex::Server`.
pub struct Multiplex<S, T>
    where T: Transport,
          S: Dispatch,
{
    // True as long as the connection has more request frames to read.
    run: bool,
    // The transport wrapping the connection.
    transport: T,
    // The `Sender` for the in-flight request body streams
    out_bodies: HashMap<RequestId, BodySender<T::BodyOut, S::Error>>,
    // The in-flight response body streams
    in_bodies: HashMap<RequestId, AwaitStream<S::InBodyStream>>,
    // True when the transport is fully flushed
    is_flushed: bool,
    // Glues the service with the pipeline task
    dispatch: S,
}

/// Dispatch messages from the transport to the service
pub trait Dispatch {
    /// Message written to transport
    type InMsg: Send + 'static;

    /// Body written to transport
    type InBody: Send + 'static;

    /// Body stream written to transport
    type InBodyStream: Stream<Item = Self::InBody, Error = Self::Error>;

    type OutMsg: Send + 'static;

    type Error: Send + 'static;

    /// Process an out message
    fn dispatch(&mut self, request_id: RequestId, message: Self::OutMsg) -> io::Result<()>;

    /// Poll the next completed message
    fn poll(&mut self) -> Option<(RequestId, Message<Self::InMsg, Self::InBodyStream>)>;

    /// RPC currently in flight
    fn has_in_flight(&self) -> bool;
}

enum BodySender<B, E>
    where B: Send + 'static,
          E: Send + 'static,
{
    Ready(Sender<B, E>),
    Busy(Await<BusySender<B, E>>),
}

/*
 *
 * ===== impl Config =====
 *
 */

impl Config {
    /// Return a default `Config` value
    pub fn new() -> Config {
        Config::default()
    }

    /// Given that frames for all in-flight RPC transactions arrive via the
    /// same `Transport`, a body frame may be received while the destination
    /// `Sender` is not ready to receive another value. In this case, the body
    /// chunk must be stored until the sender becomes ready.
    ///
    /// `max_out_body_chunks` sets the max number of body chunks that may be
    /// stored across all active RPC transactions.
    pub fn max_out_body_chunks(self, val: usize) -> Self {
        self.max_out_body_chunks = val;
        self
    }
}

impl Default for Config {
    fn default() -> Config {
    // These default values haven't been thought through that much. There are
    // most likely better default values.
        Config {
            max_in_flight_threads: 100,
            max_out_body_chunks: 400,
        }
    }
}

/*
 *
 * ===== impl Multiplex =====
 *
 */

impl<S, T, E> Multiplex<S, T>
    where T: Transport<Error = E>,
          S: Dispatch<InMsg = T::In, InBody = T::BodyIn, OutMsg = T::Out, Error = E>,
          E: From<Error<E>> + Send + 'static,
{
    /// Create a new pipeline `Multiplex` dispatcher with the given service and
    /// transport
    pub fn new(dispatch: S, transport: T) -> io::Result<Multiplex<S, T>> {
        Ok(Multiplex {
            run: true,
            transport: transport,
            out_bodies: HashMap::new(),
            in_bodies: HashMap::new(),
            is_flushed: true,
            dispatch: dispatch,
        })
    }

    /// Returns true if the pipeline server dispatch has nothing left to do
    fn is_done(&self) -> bool {
        !self.run && self.is_flushed && !self.dispatch.has_in_flight()
    }

    fn read_out_frames(&mut self) -> io::Result<()> {
        while self.run {
            if !self.check_out_body_stream() {
                break;
            }

            if let Some(frame) = try!(self.transport.read()) {
                try!(self.process_out_frame(frame));
            } else {
                break;
            }
        }

        Ok(())
    }

    fn check_out_body_stream(&mut self) -> bool {
        let sender = match self.out_body {
            Some(BodySender::Ready(..)) => {
                // The body sender is ready
                return true;
            }
            Some(BodySender::Busy(ref mut busy)) => {
                match busy.poll() {
                    Some(Ok(sender)) => sender,
                    Some(Err(_)) => unimplemented!(),
                    None => {
                        // Not ready
                        return false;
                    }
                }
            }
            None => return true,
        };

        self.out_body = Some(BodySender::Ready(sender));
        true
    }

    fn process_out_frame(&mut self, frame: Frame<T::Out, E, T::BodyOut>) -> io::Result<()> {
        // At this point, the service & transport are ready to process the
        // frame, no matter what it is.
        match frame {
            Frame::Message(id, out_message) => {
                trace!("read out message; id={:?}", id);
                // There is no streaming body. Set `out_body` to `None` so that
                // the previous body stream is dropped.
                self.out_body = None;

                if let Err(_) = self.dispatch.dispatch(id, out_message) {
                    // TODO: Should dispatch be infalliable
                    unimplemented!();
                }
            }
            Frame::MessageWithBody(id, out_message, body_sender) => {
                trace!("read out message with body; id={:?}", id);
                // Track the out body sender. If `self.out_body`
                // currently holds a sender for the previous out body, it
                // will get dropped. This terminates the stream.
                self.out_body = Some(BodySender::Ready(body_sender));

                if let Err(_) = self.dispatch.dispatch(id, out_message) {
                    // TODO: Should dispatch be infalliable
                    unimplemented!();
                }
            }
            Frame::Body(id, Some(chunk)) => {
                trace!("read out body chunk; id={:?}", id);
                try!(self.process_out_body_chunk(id, chunk));
            }
            Frame::Body(id, None) => {
                trace!("read out body EOF; id={:?}", id);
                // Drop the sender.
                // TODO: Ensure a sender exists
                let _ = self.out_body.take();
            }
            Frame::Done => {
                trace!("read Frame::Done");
                // At this point, we just return. This works
                // because tick() will be called again and go
                // through the read-cycle again.
                self.run = false;
            }
            Frame::Error(_, _) => {
                // At this point, the transport is toast, there
                // isn't much else that we can do. Killing the task
                // will cause all in-flight requests to abort, but
                // they can't be written to the transport anyway...
                return Err(io::Error::new(io::ErrorKind::BrokenPipe, "An error occurred."));
            }
        }

        Ok(())
    }

    fn process_out_body_chunk(&mut self, id: RequestId, chunk: T::BodyOut) -> io::Result<()> {
        match self.out_body.take() {
            Some(BodySender::Ready(sender)) => {
                // Try sending the out body chunk
                let busy = match sender.send(Ok(chunk)) {
                    Ok(busy) => busy,
                    Err(_) => {
                        // The rx half is gone. There is no longer any interest
                        // in the out body.
                        return Ok(());
                    }
                };

                let await = try!(Await::new(busy));
                self.out_body = Some(BodySender::Busy(await));
            }
            Some(BodySender::Busy(..)) => {
                // This case should never happen but it may be better to fail a
                // bit more gracefully in the event of an internal bug than to
                // panic.
                unimplemented!();
            }
            None => {
                // The rx half canceled interest, there is nothing else to do
            }
        }

        Ok(())
    }

    fn write_in_frames(&mut self) -> io::Result<()> {
        while self.transport.is_writable() {
            // Ensure the current in body is fully written
            if !try!(self.write_in_body()) {
                break;
            }

            // Write the next in-flight in message
            match self.dispatch.poll() {
                Some((id, msg)) => try!(self.write_in_message(id, msg)),
                None => break,
            }
        }

        Ok(())
    }

    fn write_in_message(&mut self, id: RequestId, message: Message<S::InMsg, S::InBodyStream>) -> io::Result<()> {
        match message {
            Message::WithoutBody(val) => {
                trace!("got in_flight value with body");
                try!(self.transport.write(Frame::Message(id, val)));

                // TODO: don't panic maybe if this isn't true?
                assert!(self.in_body.is_none());

                // Track the response body
                self.in_body = None;
            }
            Message::WithBody(val, body) => {
                trace!("got in_flight value with body");
                try!(self.transport.write(Frame::Message(id, val)));

                // TODO: don't panic maybe if this isn't true?
                assert!(self.in_body.is_none());

                // Track the response body
                self.in_body = Some(try!(AwaitStream::new(body)));
            }
        }

        Ok(())
    }

    // Returns true if the response body is fully written
    fn write_in_body(&mut self) -> io::Result<bool> {
        if let Some(ref mut body) = self.in_body {
            match body.poll() {
                Some(Ok(Some(chunk))) => {
                    try!(self.transport.write(Frame::Body(Some(chunk))));
                    return Ok(false);
                }
                Some(Ok(None)) => {
                    try!(self.transport.write(Frame::Body(None)));
                    // Response body flushed, let fall through
                }
                Some(Err(_)) => {
                    unimplemented!();
                }
                None => {
                    return Ok(false);
                }
            }
        }

        self.in_body = None;
        Ok(true)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.is_flushed = try!(self.transport.flush()).is_some();
        Ok(())
    }
}

impl<S, T, E> Task for Multiplex<S, T>
    where T: Transport<Error = E>,
          S: Dispatch<InMsg = T::In, InBody = T::BodyIn, OutMsg = T::Out, Error = E>,
          E: From<Error<E>> + Send + 'static,
{
    // Tick the pipeline state machine
    fn tick(&mut self) -> io::Result<Tick> {
        trace!("Multiplex::tick");

        // Always flush the transport first
        try!(self.flush());

        // First read off data from the socket
        try!(self.read_out_frames());

        // Handle completed responses
        try!(self.write_in_frames());

        // Try flushing buffered writes
        try!(self.flush());

        // Clean shutdown of the pipeline server can happen when
        //
        // 1. The server is done running, this is signaled by Transport::read()
        //    returning Frame::Done.
        //
        // 2. The transport is done writing all data to the socket, this is
        //    signaled by Transport::flush() returning Ok(Some(())).
        //
        // 3. There are no further responses to write to the transport.
        //
        // It is necessary to perfom these three checks in order to handle the
        // case where the client shuts down half the socket.
        //
        if self.is_done() {
            return Ok(Tick::Final);
        }

        // Tick again later
        Ok(Tick::WouldBlock)
    }
}
