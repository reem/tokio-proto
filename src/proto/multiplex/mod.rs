//! A dispatcher for multiplexed protocols

#![allow(warnings)]

use {Service};
use io::Readiness;
use util::future::{Empty, Sender};
use futures::Future;
use futures::stream::Stream;
use take::Take;
use std::{fmt, cmp, io, ops};

mod client;
mod multiplex;
mod server;

/// Identifies a request / response thread
pub type RequestId = u64;

/// A multiplexed protocol frame
pub enum Frame<T, E, B = ()>
    where E: Send + 'static,
          B: Send + 'static,
{
    /// Either a request or a response
    Message(RequestId, T),
    /// Returned by `Transport::read` when a streaming body will follow.
    /// Subsequent body frames with a matching `RequestId` will be proxied to
    /// the provided `Sender`.
    ///
    /// Calling `Transport::write` with Frame::MessageWithBody is an error.
    MessageWithBody(RequestId, T, Sender<B, E>),
    /// Body frame. None indicates that the body is done streaming.
    Body(RequestId, Option<B>),
    /// Error
    Error(RequestId, E),
    /// Final frame sent in each transport direction
    Done,
}

/// Message sent and received from a multiplexed service
pub enum Message<T, B = Empty<(), ()>> {
    /// Has no associated streaming body
    WithoutBody(T),
    /// Has associated streaming body
    WithBody(T, B),
}

/// Error returned as an Error frame or an `io::Error` that occurerred during
/// normal processing of the Transport
pub enum Error<E> {
    /// Transport frame level error
    Transport(E),
    /// I/O level error
    Io(io::Error),
}

/// A specialization of `Service` supporting the requirements of server
/// pipelined services
///
/// `Service` should be implemented instead of this trait.
pub trait ServerService: Send + 'static {
    /// Requests handled by the service.
    type Req: Send + 'static;

    /// Responses given by the service.
    type Resp: Send + 'static;

    /// Response body chunk
    type Body: Send + 'static;

    /// Response body stream
    type BodyStream: Stream<Item = Self::Body, Error = Self::Error>;

    /// Errors produced by the service.
    type Error: Send + 'static;

    /// The future response value.
    type Fut: Future<Item = Message<Self::Resp, Self::BodyStream>, Error = Self::Error>;

    /// Process the request and return the response asynchronously.
    fn call(&self, req: Self::Req) -> Self::Fut;
}

/// A specialization of `io::Transport` supporting the requirements of
/// pipeline based protocols.
///
/// `io::Transport` should be implemented instead of this trait.
pub trait Transport: Readiness {
    /// Messages written to the transport
    type In: Send + 'static;

    /// Inbound body frame
    type BodyIn: Send + 'static;

    /// Messages read from the transport
    type Out: Send + 'static;

    /// Outbound body frame
    type BodyOut: Send + 'static;

    /// Transport error
    type Error: Send + 'static; // TODO: rename

    /// Read a message from the `Transport`
    fn read(&mut self) -> io::Result<Option<Frame<Self::Out, Self::Error, Self::BodyOut>>>;

    /// Write a message to the `Transport`
    fn write(&mut self, req: Frame<Self::In, Self::Error, Self::BodyIn>) -> io::Result<Option<()>>;

    /// Flush pending writes to the socket
    fn flush(&mut self) -> io::Result<Option<()>>;
}

/// A specialization of `io::NewTransport` supporting the requirements of
/// pipeline based protocols.
///
/// `io::NewTransport` should be implemented instead of this trait.
pub trait NewTransport: Send + 'static {
    /// Messages written to the transport
    type In: Send + 'static;

    /// Inbound streaming body
    type BodyIn: Send + 'static;

    /// Messages read from the transport
    type Out: Send + 'static;

    /// Outbound streaming body
    type BodyOut: Send + 'static;

    /// Errors
    type Error: Send + 'static;

    /// Transport returned
    type Item: Transport<In = Self::In,
                     BodyIn = Self::BodyIn,
                        Out = Self::Out,
                    BodyOut = Self::BodyOut,
                      Error = Self::Error>;

    /// Create and return a new `Transport`
    fn new_transport(&self) -> io::Result<Self::Item>;
}

/*
 *
 * ===== impl Frame =====
 *
 */

impl<T, E, B> Frame<T, E, B>
    where E: Send + 'static,
          B: Send + 'static,
{
    /// Unwraps a frame, yielding the content of the `Message`.
    pub fn unwrap_msg(self) -> T {
        match self {
            Frame::Message(_, v) => v,
            Frame::MessageWithBody(_, v, _) => v,
            Frame::Body(..) => panic!("called `Frame::unwrap_msg()` on a `Body` value"),
            Frame::Error(..) => panic!("called `Frame::unwrap_msg()` on an `Error` value"),
            Frame::Done => panic!("called `Frame::unwrap_msg()` on a `Done` value"),
        }
    }

    /// Unwraps a frame, yielding the content of the `Body`.
    pub fn unwrap_body(self) -> Option<B> {
        match self {
            Frame::Body(_, v) => v,
            Frame::Message(..) => panic!("called `Frame::unwrap_body()` on a `Message` value"),
            Frame::MessageWithBody(..) => panic!("called `Frame::unwrap_body()` on a `MessageWithBody` value"),
            Frame::Error(..) => panic!("called `Frame::unwrap_body()` on an `Error` value"),
            Frame::Done => panic!("called `Frame::unwrap_body()` on a `Done` value"),
        }
    }

    /// Unwraps a frame, yielding the content of the `Error`.
    pub fn unwrap_err(self) -> E {
        match self {
            Frame::Error(_, e) => e,
            Frame::Body(..) => panic!("called `Frame::unwrap_err()` on a `Body` value"),
            Frame::Message(..) => panic!("called `Frame::unwrap_err()` on a `Message` value"),
            Frame::MessageWithBody(..) => panic!("called `Frame::unwrap_err()` on a `MessageWithBody` value"),
            Frame::Done => panic!("called `Frame::unwrap_message()` on a `Done` value"),
        }
    }

    /// Returns true if the frame is `Frame::Done`
    pub fn is_done(&self) -> bool {
        match *self {
            Frame::Done => true,
            _ => false,
        }
    }
}

impl<T, E, B> fmt::Debug for Frame<T, E, B>
    where T: fmt::Debug,
          E: fmt::Debug + Send + 'static,
          B: fmt::Debug + Send + 'static,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Frame::Message(ref id, ref v) => write!(fmt, "Frame::Message({:?}, {:?})", id, v),
            Frame::MessageWithBody(ref id, ref v, _) => write!(fmt, "Frame::MessageWithBody({:?}, {:?}, Sender)", id, v),
            Frame::Body(ref id, ref v) => write!(fmt, "Frame::Body({:?}, {:?})", id, v),
            Frame::Error(ref id, ref v) => write!(fmt, "Frame::Error({:?}, {:?})", id, v),
            Frame::Done => write!(fmt, "Frame::Done"),
        }
    }
}

/*
 *
 * ===== impl Message =====
 *
 */

impl<T, B> Message<T, B> {
    /// If the `Message` value has an associated body stream, return it. The
    /// original `Message` value will then become a `WithoutBody` variant.
    pub fn take_body(&mut self) -> Option<B> {
        use std::ptr;

        // unfortunate that this is unsafe, but I think it is preferable to add
        // a little bit of unsafe code instead of adding a useless variant to
        // Message.
        unsafe {
            match ptr::read(self as *const Message<T, B>) {
                m @ Message::WithoutBody(..) => {
                    ptr::write(self as *mut Message<T, B>, m);
                    None
                }
                Message::WithBody(m, b) => {
                    ptr::write(self as *mut Message<T, B>, Message::WithoutBody(m));
                    Some(b)
                }
            }
        }
    }
}

impl<T, B> cmp::PartialEq<T> for Message<T, B>
    where T: cmp::PartialEq
{
    fn eq(&self, other: &T) -> bool {
        (**self).eq(other)
    }
}

impl<T, B> ops::Deref for Message<T, B> {
    type Target = T;

    fn deref(&self) -> &T {
        match *self {
            Message::WithoutBody(ref v) => v,
            Message::WithBody(ref v, _) => v,
        }
    }
}

impl<T, B> ops::DerefMut for Message<T, B> {
    fn deref_mut(&mut self) -> &mut T {
        match *self {
            Message::WithoutBody(ref mut v) => v,
            Message::WithBody(ref mut v, _) => v,
        }
    }
}

impl<T, B> fmt::Debug for Message<T, B>
    where T: fmt::Debug
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Message::WithoutBody(ref v) => write!(fmt, "Message::WithoutBody({:?})", v),
            Message::WithBody(ref v, _) => write!(fmt, "Message::WithBody({:?}, ...)", v),
        }
    }
}

/*
 *
 * ===== impl ServerService =====
 *
 */

impl<S, Resp, Body, BodyStream> ServerService for S
    where S: Service<Resp = Message<Resp, BodyStream>>,
          Resp: Send + 'static,
          Body: Send + 'static,
          BodyStream: Stream<Item = Body, Error = S::Error>,
{
    type Req = S::Req;
    type Resp = Resp;
    type Body = Body;
    type BodyStream = BodyStream;
    type Error = S::Error;
    type Fut = S::Fut;

    fn call(&self, req: Self::Req) -> Self::Fut {
        Service::call(self, req)
    }
}

/*
 *
 * ===== impl Transport =====
 *
 */

impl<T, M1, M2, B1, B2, E> Transport for T
    where T: ::io::Transport<In = Frame<M1, E, B1>, Out = Frame<M2, E, B2>>,
          M1: Send + 'static,
          B1: Send + 'static,
          M2: Send + 'static,
          B2: Send + 'static,
          E: Send + 'static,
{
    type In = M1;
    type BodyIn = B1;
    type Out = M2;
    type BodyOut = B2;
    type Error = E;

    fn read(&mut self) -> io::Result<Option<Frame<M2, E, B2>>> {
        ::io::Transport::read(self)
    }

    fn write(&mut self, req: Frame<M1, E, B1>) -> io::Result<Option<()>> {
        ::io::Transport::write(self, req)
    }

    fn flush(&mut self) -> io::Result<Option<()>> {
        ::io::Transport::flush(self)
    }
}

/*
 *
 * ===== impl NewTransport =====
 *
 */

impl<F, T> NewTransport for F
    where F: Fn() -> io::Result<T> + Send + 'static,
          T: Transport,
{
    type In = T::In;
    type BodyIn = T::BodyIn;
    type Out = T::Out;
    type BodyOut = T::BodyOut;
    type Error = T::Error;
    type Item = T;

    fn new_transport(&self) -> io::Result<T> {
        self()
    }
}

impl<F, T> NewTransport for Take<F>
    where F: FnOnce() -> io::Result<T> + Send + 'static,
          T: Transport,
{
    type In = T::In;
    type BodyIn = T::BodyIn;
    type Out = T::Out;
    type BodyOut = T::BodyOut;
    type Error = T::Error;
    type Item = T;

    fn new_transport(&self) -> io::Result<T> {
        self.take()()
    }
}

impl From<Error<io::Error>> for io::Error {
    fn from(err: Error<io::Error>) -> Self {
        match err {
            Error::Transport(e) |
            Error::Io(e) => e,
        }
    }
}
