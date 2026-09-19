use crate::{BridgeError, Output};
use std::{future::Future, sync::OnceLock};
use tokio::runtime::Runtime;

fn runtime() -> Result<&'static Runtime, BridgeError> {
    // Shared for the process lifetime; clients do not create their own threads.
    static RUNTIME: OnceLock<Result<Runtime, String>> = OnceLock::new();
    RUNTIME
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_name("gocodex")
                .on_thread_start(initialize_thread)
                .enable_all()
                .build()
                .map_err(|error| error.to_string())
        })
        .as_ref()
        .map_err(|error| BridgeError::new("internal", error.clone()))
}

fn initialize_thread() {
    // Rust binaries ignore SIGPIPE at startup, but an embedded library has no Rust main.
    // Socket writev (including TLS/HTTP2 shutdown) can raise SIGPIPE. Mask it
    // only on our own Tokio threads so writes return EPIPE. Go's signal handlers
    // and thread masks must remain untouched.
    #[cfg(unix)]
    unsafe {
        let mut signals = std::mem::zeroed();
        assert_eq!(libc::sigemptyset(&mut signals), 0);
        assert_eq!(libc::sigaddset(&mut signals, libc::SIGPIPE), 0);
        assert_eq!(
            libc::pthread_sigmask(libc::SIG_BLOCK, &signals, std::ptr::null_mut()),
            0
        );
    }
}

pub(crate) fn run(
    future: impl Future<Output = Result<Output, BridgeError>> + Send + 'static,
) -> Result<Output, BridgeError> {
    let runtime = runtime()?;
    // Only wait for a task on the cgo thread. Poll all network futures on our
    // configured Tokio threads, including their very first poll.
    runtime.block_on(runtime.spawn(future)).map_err(|error| {
        BridgeError::new(
            if error.is_panic() {
                "panic"
            } else {
                "internal"
            },
            "Rust request task terminated",
        )
    })?
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    #[test]
    fn network_tasks_mask_sigpipe_without_changing_calling_thread_mask() {
        fn sigpipe_blocked() -> bool {
            // SAFETY: Both libc calls receive a valid sigset_t output buffer.
            unsafe {
                let mut mask = std::mem::zeroed();
                assert_eq!(
                    libc::pthread_sigmask(libc::SIG_BLOCK, std::ptr::null(), &mut mask),
                    0
                );
                libc::sigismember(&mask, libc::SIGPIPE) == 1
            }
        }
        let original = sigpipe_blocked();
        assert!(
            run(async {
                assert!(sigpipe_blocked());
                Ok(Output::handle(0))
            })
            .is_ok()
        );
        assert_eq!(sigpipe_blocked(), original);
    }
}
