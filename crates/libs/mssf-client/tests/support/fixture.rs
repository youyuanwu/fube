use std::future::Future;

use super::oracle::Result;

pub async fn with_cleanup<T: Send + 'static>(
    scenario: impl Future<Output = Result<T>> + Send + 'static,
    cleanup: impl Future<Output = Result<()>>,
) -> Result<T> {
    // A task boundary catches assertion panics without skipping async restoration.
    let result = tokio::spawn(scenario).await;
    if let Err(cleanup_error) = cleanup.await {
        let primary = match result {
            Ok(Ok(_)) => "scenario succeeded".to_string(),
            Ok(Err(error)) => format!("scenario failed: {error}"),
            Err(error) => format!("scenario task failed: {error}"),
        };
        return Err(format!("{primary}; fixture cleanup failed: {cleanup_error}").into());
    }
    match result {
        Ok(result) => result,
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };

    use super::*;

    #[tokio::test]
    async fn cleanup_runs_after_success_and_preserves_value() {
        let cleaned = AtomicBool::new(false);
        let value = with_cleanup(async { Ok(42) }, async {
            cleaned.store(true, Ordering::SeqCst);
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(value, 42);
        assert!(cleaned.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn cleanup_runs_after_preparation_or_native_error() {
        for message in ["preparation failed", "native query failed"] {
            let cleaned = AtomicBool::new(false);
            let error = with_cleanup::<()>(async move { Err(message.into()) }, async {
                cleaned.store(true, Ordering::SeqCst);
                Ok(())
            })
            .await
            .unwrap_err();
            assert_eq!(error.to_string(), message);
            assert!(cleaned.load(Ordering::SeqCst));
        }
    }

    #[tokio::test]
    async fn cleanup_runs_after_assertion_panic_and_preserves_panic() {
        async fn panicking() -> Result<()> {
            panic!("native assertion failed");
        }
        let cleaned = Arc::new(AtomicBool::new(false));
        let observed = cleaned.clone();
        let error = tokio::spawn(with_cleanup(panicking(), async move {
            cleaned.store(true, Ordering::SeqCst);
            Ok(())
        }))
        .await
        .unwrap_err();
        assert!(error.is_panic());
        assert!(observed.load(Ordering::SeqCst));
        assert_eq!(
            error.into_panic().downcast_ref::<&str>(),
            Some(&"native assertion failed")
        );
    }

    #[tokio::test]
    async fn cleanup_error_fails_an_otherwise_successful_scenario() {
        let error = with_cleanup(async { Ok(()) }, async { Err("restore failed".into()) })
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("fixture cleanup failed: restore failed"));
    }

    #[tokio::test]
    async fn cleanup_error_preserves_the_primary_failure() {
        let error = with_cleanup::<()>(async { Err("native query failed".into()) }, async {
            Err("restore failed".into())
        })
        .await
        .unwrap_err();
        let message = error.to_string();
        assert!(message.contains("scenario failed: native query failed"));
        assert!(message.contains("fixture cleanup failed: restore failed"));
    }
}
