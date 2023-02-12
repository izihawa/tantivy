use crate::TantivyError;

/// `FutureResult` is a handle that makes it possible to wait for the completion
/// of an ongoing task.
///
/// Contrary to some `Future`, it does not need to be polled for the task to
/// progress. Dropping the `FutureResult` does not cancel the task being executed
/// either.
///
/// - In a sync context, you can call `FutureResult::wait()`. The function
/// does not rely on `block_on`.
/// - In an async context, you can call simply use `FutureResult` as a future.
pub struct FutureResult<T> {
    inner: Inner<T>,
}

enum Inner<T> {
    FailedBeforeStart(Option<TantivyError>),
    InProgress {
        receiver: kanal::OneshotReceiver<crate::Result<T>>,
        error_msg_if_failure: &'static str,
    },
}

impl<T> From<TantivyError> for FutureResult<T> {
    fn from(err: TantivyError) -> Self {
        FutureResult {
            inner: Inner::FailedBeforeStart(Some(err)),
        }
    }
}

impl<T> FutureResult<T> {
    pub(crate) fn create(
        error_msg_if_failure: &'static str,
    ) -> (Self, kanal::OneshotSender<crate::Result<T>>) {
        let (sender, receiver) = kanal::oneshot();
        let inner: Inner<T> = Inner::InProgress {
            receiver,
            error_msg_if_failure,
        };
        (FutureResult { inner }, sender)
    }

    /// Blocks until the scheduled result is available.
    ///
    /// In an async context, you should simply use `ScheduledResult` as a future.
    pub fn wait(self) -> crate::Result<T> {
        match self.inner {
            Inner::FailedBeforeStart(err) => Err(err.unwrap()),
            Inner::InProgress {
                receiver,
                error_msg_if_failure,
            } => receiver.recv().unwrap_or_else(|_| {
                Err(crate::TantivyError::SystemError(
                    error_msg_if_failure.to_string(),
                ))
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::FutureResult;
    use crate::TantivyError;

    #[test]
    fn test_scheduled_result_failed_to_schedule() {
        let scheduled_result: FutureResult<()> = FutureResult::from(TantivyError::Poisoned);
        let res = scheduled_result.wait();
        assert!(matches!(res, Err(TantivyError::Poisoned)));
    }

    #[test]

    fn test_scheduled_result_error() {
        let (scheduled_result, tx): (FutureResult<()>, _) = FutureResult::create("failed");
        drop(tx);
        let res = scheduled_result.wait();
        assert!(matches!(res, Err(TantivyError::SystemError(_))));
    }

    #[test]
    fn test_scheduled_result_sent_success() {
        let (scheduled_result, tx): (FutureResult<u64>, _) = FutureResult::create("failed");
        tx.send(Ok(2u64)).unwrap();
        assert_eq!(scheduled_result.wait().unwrap(), 2u64);
    }

    #[test]
    fn test_scheduled_result_sent_error() {
        let (scheduled_result, tx): (FutureResult<u64>, _) = FutureResult::create("failed");
        tx.send(Err(TantivyError::Poisoned)).unwrap();
        let res = scheduled_result.wait();
        assert!(matches!(res, Err(TantivyError::Poisoned)));
    }
}
