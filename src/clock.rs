use std::{
    io,
    os::unix::fs::MetadataExt,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use rustix::{
    fd::OwnedFd,
    time::{
        ClockId, Itimerspec, TimerfdClockId, TimerfdFlags, TimerfdTimerFlags, Timespec,
        clock_gettime, timerfd_create, timerfd_settime,
    },
};
use tokio::io::{Interest, unix::AsyncFd};

pub fn boot_id() -> Result<String> {
    let id = std::fs::read_to_string("/proc/sys/kernel/random/boot_id")?;
    let id = id.trim();
    uuid::Uuid::parse_str(id).context("invalid Linux boot id")?;
    let namespace = std::fs::metadata("/proc/self/ns/time")?.ino();
    let offsets = std::fs::read_to_string("/proc/self/timens_offsets")?;
    let mut fields = offsets
        .lines()
        .find(|line| line.split_whitespace().next() == Some("boottime"))
        .context("missing boot-clock namespace offset")?
        .split_whitespace()
        .skip(1);
    let seconds: i64 = fields
        .next()
        .context("missing boot-clock offset seconds")?
        .parse()?;
    let nanos: i64 = fields
        .next()
        .context("missing boot-clock offset nanoseconds")?
        .parse()?;
    Ok(format!("{id}:{namespace}:{seconds}:{nanos}"))
}

pub fn now() -> u64 {
    u64::try_from(clock_gettime(ClockId::Boottime).tv_sec).unwrap_or_default()
}

pub fn centiseconds() -> u64 {
    let time = clock_gettime(ClockId::Boottime);
    u64::try_from(time.tv_sec)
        .unwrap_or_default()
        .saturating_mul(100)
        + u64::try_from(time.tv_nsec / 10_000_000).unwrap_or_default()
}

pub fn wall_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub struct Timer(AsyncFd<OwnedFd>);

impl Timer {
    pub fn new() -> Result<Self> {
        let fd = timerfd_create(
            TimerfdClockId::Boottime,
            TimerfdFlags::NONBLOCK | TimerfdFlags::CLOEXEC,
        )?;
        // OwnedFd keeps the timer descriptor valid and unchanged while registered.
        Ok(Self(unsafe {
            AsyncFd::register_with_interest(fd, Interest::READABLE)
        }?))
    }

    pub fn set(&self, deadline: Option<u64>) -> Result<()> {
        let seconds = deadline
            .map(|value| i64::try_from(value.max(1)))
            .transpose()?
            .unwrap_or(0);
        timerfd_settime(
            self.0.get_ref(),
            TimerfdTimerFlags::ABSTIME,
            &Itimerspec {
                it_interval: Timespec {
                    tv_sec: 0,
                    tv_nsec: 0,
                },
                it_value: Timespec {
                    tv_sec: seconds,
                    tv_nsec: 0,
                },
            },
        )?;
        Ok(())
    }

    pub async fn wait(&self) -> Result<()> {
        loop {
            let mut ready = self.0.readable().await?;
            match ready.try_io(|fd| {
                let mut bytes = [0u8; 8];
                let count = rustix::io::read(fd.get_ref(), &mut bytes).map_err(io::Error::from)?;
                if count != bytes.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "short timerfd read",
                    ));
                }
                Ok(())
            }) {
                Ok(result) => return result.map_err(Into::into),
                Err(_) => continue,
            }
        }
    }
}
