use core::arch::asm;
use std::cmp;
use std::io;
use std::time::Duration;

const SYS_POLL: u32 = 168;
const SYS_READ: u32 = 3;
const SYS_WRITE: u32 = 4;
const SYS_PIPE: u32 = 42;
const SYS_CLOSE: u32 = 6;
const SYS_FCNTL: u32 = 55;
const SYS_SOCKET: u32 = 359;
const SYS_BIND: u32 = 361;
const SYS_CONNECT: u32 = 362;
const SYS_LISTEN: u32 = 363;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub(crate) struct PollFd {
    pub(crate) fd: i32,
    pub(crate) events: i16,
    pub(crate) revents: i16,
}

pub(crate) fn poll(fds: &mut [PollFd], timeout: Option<Duration>) -> io::Result<usize> {
    let timeout_ms = timeout.map_or(-1, |duration| {
        let rounded = duration
            .checked_add(Duration::from_nanos(999_999))
            .unwrap_or(duration)
            .as_millis();
        cmp::min(rounded, i32::MAX as u128) as i32
    });

    let result = unsafe {
        syscall3(
            SYS_POLL,
            fds.as_mut_ptr() as usize as u32,
            fds.len() as u32,
            timeout_ms as u32,
        )
    };
    if result < 0 {
        Err(io::Error::from_raw_os_error(-result))
    } else {
        Ok(result as usize)
    }
}

pub(crate) fn pipe() -> io::Result<[i32; 2]> {
    let mut fds = [0u32; 2];
    cvt(unsafe { syscall1(SYS_PIPE, fds.as_mut_ptr() as usize as u32) })?;
    Ok([fds[0] as i32, fds[1] as i32])
}

pub(crate) fn read(fd: i32, buffer: &mut [u8]) -> io::Result<usize> {
    cvt(unsafe {
        syscall3(
            SYS_READ,
            fd as u32,
            buffer.as_mut_ptr() as usize as u32,
            buffer.len() as u32,
        )
    })
    .map(|result| result as usize)
}

pub(crate) fn write(fd: i32, buffer: &[u8]) -> io::Result<usize> {
    cvt(unsafe {
        syscall3(
            SYS_WRITE,
            fd as u32,
            buffer.as_ptr() as usize as u32,
            buffer.len() as u32,
        )
    })
    .map(|result| result as usize)
}

pub(crate) fn socket(domain: u32, ty: u32, protocol: u32) -> io::Result<i32> {
    cvt(unsafe { syscall3(SYS_SOCKET, domain, ty, protocol) })
}

pub(crate) fn bind(fd: i32, address: *const u8, length: u32) -> io::Result<()> {
    cvt(unsafe { syscall3(SYS_BIND, fd as u32, address as usize as u32, length) }).map(drop)
}

pub(crate) fn connect(fd: i32, address: *const u8, length: u32) -> io::Result<()> {
    cvt(unsafe { syscall3(SYS_CONNECT, fd as u32, address as usize as u32, length) }).map(drop)
}

pub(crate) fn listen(fd: i32, backlog: i32) -> io::Result<()> {
    cvt(unsafe { syscall2(SYS_LISTEN, fd as u32, backlog as u32) }).map(drop)
}

pub(crate) fn set_nonblocking(fd: i32) -> io::Result<()> {
    const F_GETFL: u32 = 3;
    const F_SETFL: u32 = 4;
    const O_NONBLOCK: u32 = 0x800;

    let flags = cvt(unsafe { syscall3(SYS_FCNTL, fd as u32, F_GETFL, 0) })? as u32;
    cvt(unsafe { syscall3(SYS_FCNTL, fd as u32, F_SETFL, flags | O_NONBLOCK) }).map(drop)
}

pub(crate) fn close(fd: i32) {
    let _ = unsafe { syscall1(SYS_CLOSE, fd as u32) };
}

fn cvt(result: i32) -> io::Result<i32> {
    if result < 0 {
        Err(io::Error::from_raw_os_error(-result))
    } else {
        Ok(result)
    }
}

#[inline]
unsafe fn syscall1(number: u32, arg1: u32) -> i32 {
    let result: i32;
    unsafe {
        asm!(
            "int 0x80",
            inlateout("eax") number => result,
            in("ebx") arg1,
            options(nostack),
        );
    }
    result
}

#[inline]
unsafe fn syscall2(number: u32, arg1: u32, arg2: u32) -> i32 {
    let result: i32;
    unsafe {
        asm!(
            "int 0x80",
            inlateout("eax") number => result,
            in("ebx") arg1,
            in("ecx") arg2,
            options(nostack),
        );
    }
    result
}

#[inline]
unsafe fn syscall3(number: u32, arg1: u32, arg2: u32, arg3: u32) -> i32 {
    let result: i32;
    unsafe {
        asm!(
            "int 0x80",
            inlateout("eax") number => result,
            in("ebx") arg1,
            in("ecx") arg2,
            in("edx") arg3,
            options(nostack),
        );
    }
    result
}
