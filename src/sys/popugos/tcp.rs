use std::io;
use std::mem::size_of;
use std::net::{self, SocketAddr, SocketAddrV4};
use std::os::popugos::io::AsRawFd;

use super::abi;

const AF_INET: u32 = 2;
const SOCK_STREAM: u32 = 1;
const IPPROTO_TCP: u32 = 6;
const EINPROGRESS: i32 = 115;

#[repr(C)]
struct SockAddrIn {
    family: u16,
    port: u16,
    address: u32,
    zero: [u8; 8],
}

impl SockAddrIn {
    fn new(address: &SocketAddrV4) -> Self {
        Self {
            family: AF_INET as u16,
            port: address.port().to_be(),
            // sockaddr_in.sin_addr is stored in network byte order. On i686,
            // from_ne_bytes() reversed IPv4 addresses (e.g. 10.0.2.2 became
            // 2.2.0.10 from the kernel/smoltcp point of view), which made
            // non-blocking Tokio connects sit in SYN-SENT until timeout.
            address: u32::from_be_bytes(address.ip().octets()),
            zero: [0; 8],
        }
    }
}

fn with_address<T>(address: SocketAddr, call: impl FnOnce(*const u8, u32) -> io::Result<T>) -> io::Result<T> {
    match address {
        SocketAddr::V4(address) => {
            let raw = SockAddrIn::new(&address);
            call((&raw as *const SockAddrIn).cast(), size_of::<SockAddrIn>() as u32)
        }
        SocketAddr::V6(_) => Err(io::Error::new(io::ErrorKind::Unsupported, "PopugOS currently supports IPv4 only")),
    }
}

pub(crate) fn new_for_addr(address: SocketAddr) -> io::Result<i32> {
    if address.is_ipv6() {
        return Err(io::Error::new(io::ErrorKind::Unsupported, "PopugOS currently supports IPv4 only"));
    }
    let fd = abi::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)?;
    if let Err(error) = abi::set_nonblocking(fd) {
        abi::close(fd);
        return Err(error);
    }
    Ok(fd)
}

pub(crate) fn bind(listener: &net::TcpListener, address: SocketAddr) -> io::Result<()> {
    with_address(address, |raw, length| abi::bind(listener.as_raw_fd(), raw, length))
}

pub(crate) fn connect(stream: &net::TcpStream, address: SocketAddr) -> io::Result<()> {
    match with_address(address, |raw, length| abi::connect(stream.as_raw_fd(), raw, length)) {
        Err(error) if error.raw_os_error() == Some(EINPROGRESS) => Ok(()),
        result => result,
    }
}

pub(crate) fn listen(listener: &net::TcpListener, backlog: i32) -> io::Result<()> {
    abi::listen(listener.as_raw_fd(), backlog)
}

pub(crate) fn set_reuseaddr(_listener: &net::TcpListener, _reuseaddr: bool) -> io::Result<()> {
    Ok(())
}

pub(crate) fn accept(listener: &net::TcpListener) -> io::Result<(net::TcpStream, SocketAddr)> {
    let (stream, address) = listener.accept()?;
    stream.set_nonblocking(true)?;
    Ok((stream, address))
}
