use barkeeper::kv::apply_notifier::ApplyNotifier;

#[tokio::test]
async fn test_already_applied_returns_immediately() {
    let notifier = ApplyNotifier::new(10);
    // Should return immediately since 5 <= 10
    notifier.wait_for(5).await;
}

#[tokio::test]
async fn test_wait_for_future_index() {
    let notifier = ApplyNotifier::new(0);
    let n = notifier.clone();
    let handle = tokio::spawn(async move {
        n.wait_for(5).await;
    });
    // Not yet applied — task should be pending
    tokio::task::yield_now().await;
    assert!(!handle.is_finished());
    // Advance to 5
    notifier.advance(5);
    handle.await.unwrap();
}

#[tokio::test]
async fn test_advance_wakes_multiple_waiters() {
    let notifier = ApplyNotifier::new(0);
    let n1 = notifier.clone();
    let n2 = notifier.clone();
    let h1 = tokio::spawn(async move { n1.wait_for(3).await; });
    let h2 = tokio::spawn(async move { n2.wait_for(5).await; });
    tokio::task::yield_now().await;
    notifier.advance(5);
    h1.await.unwrap();
    h2.await.unwrap();
}

#[tokio::test]
async fn test_current_returns_applied_index() {
    let notifier = ApplyNotifier::new(42);
    assert_eq!(notifier.current(), 42);
    notifier.advance(100);
    assert_eq!(notifier.current(), 100);
}
