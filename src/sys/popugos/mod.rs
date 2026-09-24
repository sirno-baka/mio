use std::io;
use std::os::popugos::io::RawFd;
#[cfg(debug_assertions)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::{Interest, Registry, Token};

mod abi;

cfg_net! {
    pub(crate) mod tcp;
    pub(crate) mod udp;
}

const POLLIN: i16 = 0x001;
const POLLPRI: i16 = 0x002;
const POLLOUT: i16 = 0x004;
const POLLERR: i16 = 0x008;
const POLLHUP: i16 = 0x010;
const POLLNVAL: i16 = 0x020;

#[derive(Debug, Clone)]
pub struct Event {
    token: Token,
    events: i16,
}

pub type Events = Vec<Event>;

pub mod event {
    use std::fmt;

    use super::{Event, POLLERR, POLLHUP, POLLIN, POLLNVAL, POLLOUT, POLLPRI};
    use crate::Token;

    pub fn token(event: &Event) -> Token { event.token }
    pub fn is_readable(event: &Event) -> bool { event.events & (POLLIN | POLLPRI) != 0 }
    pub fn is_writable(event: &Event) -> bool { event.events & POLLOUT != 0 }
    pub fn is_error(event: &Event) -> bool { event.events & (POLLERR | POLLNVAL) != 0 }
    pub fn is_read_closed(event: &Event) -> bool { event.events & POLLHUP != 0 }
    pub fn is_write_closed(event: &Event) -> bool {
        event.events & POLLHUP != 0 || event.events & (POLLOUT | POLLERR) == (POLLOUT | POLLERR)
    }
    pub fn is_priority(event: &Event) -> bool { event.events & POLLPRI != 0 }
    pub fn is_aio(_: &Event) -> bool { false }
    pub fn is_lio(_: &Event) -> bool { false }

    pub fn debug_details(f: &mut fmt::Formatter<'_>, event: &Event) -> fmt::Result {
        f.debug_struct("poll_event")
            .field("token", &event.token)
            .field("events", &event.events)
            .finish()
    }
}

#[derive(Debug, Clone)]
struct Registration {
    fd: RawFd,
    token: Token,
    interests: Interest,
    // PopugOS exposes level-triggered poll(2), while Mio's socket users are
    // written for edge/one-shot style readiness. Keep only readiness classes
    // that are currently armed; a delivered class stays disarmed until an I/O
    // attempt returns WouldBlock and IoSourceState rearms the registration.
    armed: i16,
    generation: u64,
}

#[derive(Debug)]
struct SelectorState {
    registrations: Mutex<Vec<Registration>>,
    pending_wake: Mutex<Option<Token>>,
    wake_reader: RawFd,
    wake_writer: RawFd,
    #[cfg(debug_assertions)]
    id: usize,
}

#[cfg(debug_assertions)]
static NEXT_SELECTOR_ID: AtomicUsize = AtomicUsize::new(1);

#[derive(Debug)]
pub struct Selector {
    state: Arc<SelectorState>,
}

impl Selector {
    pub fn new() -> io::Result<Self> {
        let [wake_reader, wake_writer] = abi::pipe()?;
        if let Err(error) = abi::set_nonblocking(wake_reader) {
            abi::close(wake_reader);
            abi::close(wake_writer);
            return Err(error);
        }
        if let Err(error) = abi::set_nonblocking(wake_writer) {
            abi::close(wake_reader);
            abi::close(wake_writer);
            return Err(error);
        }
        Ok(Self {
            state: Arc::new(SelectorState {
                registrations: Mutex::new(Vec::new()),
                pending_wake: Mutex::new(None),
                wake_reader,
                wake_writer,
                #[cfg(debug_assertions)]
                id: NEXT_SELECTOR_ID.fetch_add(1, Ordering::Relaxed),
            }),
        })
    }

    pub fn try_clone(&self) -> io::Result<Self> {
        Ok(Self { state: self.state.clone() })
    }

    pub fn select(&self, events: &mut Events, timeout: Option<Duration>) -> io::Result<()> {
        events.clear();
        let registrations = self.state.registrations.lock().unwrap().clone();
        let active_registrations: Vec<_> = registrations
            .iter()
            .filter(|registration| registration.armed != 0)
            .cloned()
            .collect();
        let mut poll_fds = Vec::with_capacity(active_registrations.len() + 1);
        poll_fds.push(abi::PollFd {
            fd: self.state.wake_reader,
            events: POLLIN,
            revents: 0,
        });
        poll_fds.extend(active_registrations.iter().map(|registration| abi::PollFd {
            fd: registration.fd,
            events: registration.armed,
            revents: 0,
        }));

        let woke = self.state.pending_wake.lock().unwrap().is_some();
        let timeout = if woke { Some(Duration::ZERO) } else { timeout };
        abi::poll(&mut poll_fds, timeout)?;

        if poll_fds[0].revents & POLLIN != 0 {
            let mut buffer = [0u8; 64];
            while matches!(abi::read(self.state.wake_reader, &mut buffer), Ok(n) if n != 0) {}
        }

        if let Some(token) = self.state.pending_wake.lock().unwrap().take() {
            events.push(Event { token, events: POLLIN });
        }

        let mut delivered = Vec::new();
        for (registration, poll_fd) in active_registrations.iter().zip(poll_fds[1..].iter()) {
            if poll_fd.revents != 0 {
                events.push(Event { token: registration.token, events: poll_fd.revents });
                delivered.push((registration.fd, registration.generation, poll_fd.revents));
            }
        }

        if !delivered.is_empty() {
            let mut current = self.state.registrations.lock().unwrap();
            for (fd, generation, revents) in delivered {
                let Some(registration) = current
                    .iter_mut()
                    .find(|registration| registration.fd == fd && registration.generation == generation)
                else {
                    continue;
                };

                if revents & (POLLERR | POLLHUP | POLLNVAL) != 0 {
                    // Error/closure is itself an edge. Deliver it once; the
                    // consumer will observe the error/EOF and deregister.
                    registration.armed = 0;
                } else {
                    registration.armed &= !(revents & (POLLIN | POLLPRI | POLLOUT));
                }
            }
        }
        Ok(())
    }

    pub fn register(&self, fd: RawFd, token: Token, interests: Interest) -> io::Result<()> {
        {
            let mut registrations = self.state.registrations.lock().unwrap();
            if registrations.iter().any(|registration| registration.fd == fd) {
                return Err(io::ErrorKind::AlreadyExists.into());
            }
            registrations.push(Registration {
                fd,
                token,
                interests,
                armed: interests_to_poll(interests),
                generation: 1,
            });
        }
        self.kick()
    }

    pub fn reregister(&self, fd: RawFd, token: Token, interests: Interest) -> io::Result<()> {
        {
            let mut registrations = self.state.registrations.lock().unwrap();
            let registration = registrations
                .iter_mut()
                .find(|registration| registration.fd == fd)
                .ok_or(io::ErrorKind::NotFound)?;
            registration.token = token;
            registration.interests = interests;
            registration.armed = interests_to_poll(interests);
            registration.generation = registration.generation.wrapping_add(1);
        }
        self.kick()
    }

    pub fn deregister(&self, fd: RawFd) -> io::Result<()> {
        {
            let mut registrations = self.state.registrations.lock().unwrap();
            let index = registrations
                .iter()
                .position(|registration| registration.fd == fd)
                .ok_or(io::ErrorKind::NotFound)?;
            registrations.swap_remove(index);
        }
        self.kick()
    }

    fn kick(&self) -> io::Result<()> {
        match abi::write(self.state.wake_writer, &[1]) {
            Ok(_) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(()),
            Err(error) => Err(error),
        }
    }

    fn wake(&self, token: Token) -> io::Result<()> {
        let mut pending_wake = self.state.pending_wake.lock().unwrap();
        if pending_wake.replace(token).is_none() {
            abi::write(self.state.wake_writer, &[1])?;
        }
        Ok(())
    }

    #[cfg(debug_assertions)]
    pub fn id(&self) -> usize { self.state.id }
}

impl Drop for SelectorState {
    fn drop(&mut self) {
        abi::close(self.wake_reader);
        abi::close(self.wake_writer);
    }
}

fn interests_to_poll(interests: Interest) -> i16 {
    let mut events = 0;
    if interests.is_readable() { events |= POLLIN; }
    if interests.is_writable() { events |= POLLOUT; }
    if interests.is_priority() { events |= POLLPRI; }
    events
}

pub(crate) struct IoSourceState {
    inner: Option<IoSourceRegistration>,
}

struct IoSourceRegistration {
    selector: Selector,
    fd: RawFd,
    token: Token,
    interests: Interest,
    registered: bool,
}

impl IoSourceState {
    pub(crate) fn new() -> Self { Self { inner: None } }

    pub(crate) fn do_io<T, F, R>(&self, f: F, io: &T) -> io::Result<R>
    where
        F: FnOnce(&T) -> io::Result<R>,
    {
        let result = f(io);

        // poll(2) is level-triggered. The selector disarms readiness after it
        // has delivered an event so a permanently writable socket cannot keep
        // Tokio in a hot reactor loop. Once an actual I/O operation drains the
        // readiness and reaches WouldBlock, rearm the original interests just
        // like Mio's registered_io_source backend does.
        if matches!(&result, Err(error) if error.kind() == io::ErrorKind::WouldBlock) {
            if let Some(state) = self.inner.as_ref() {
                state
                    .selector
                    .reregister(state.fd, state.token, state.interests)?;
            }
        }

        result
    }

    pub(crate) fn register(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
        fd: RawFd,
    ) -> io::Result<()> {
        if self.inner.is_some() {
            return Err(io::ErrorKind::AlreadyExists.into());
        }
        let selector = registry.selector().try_clone()?;
        selector.register(fd, token, interests)?;
        self.inner = Some(IoSourceRegistration {
            selector,
            fd,
            token,
            interests,
            registered: true,
        });
        Ok(())
    }

    pub(crate) fn reregister(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
        fd: RawFd,
    ) -> io::Result<()> {
        registry.selector().reregister(fd, token, interests).map(|()| {
            if let Some(state) = self.inner.as_mut() {
                state.token = token;
                state.interests = interests;
            }
        })
    }

    pub(crate) fn deregister(&mut self, registry: &Registry, fd: RawFd) -> io::Result<()> {
        if let Some(state) = self.inner.as_mut() {
            state.registered = false;
        }
        self.inner = None;
        registry.selector().deregister(fd)
    }
}

impl Drop for IoSourceRegistration {
    fn drop(&mut self) {
        if self.registered {
            let _ = self.selector.deregister(self.fd);
        }
    }
}

#[derive(Debug)]
pub(crate) struct Waker {
    selector: Selector,
    token: Token,
}

impl Waker {
    pub(crate) fn new(selector: &Selector, token: Token) -> io::Result<Self> {
        Ok(Self { selector: selector.try_clone()?, token })
    }

    pub(crate) fn wake(&self) -> io::Result<()> { self.selector.wake(self.token) }
}
