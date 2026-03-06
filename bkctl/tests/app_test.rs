//! State/model layer tests — pure functions, no I/O, no server.

use bkctl::app::{App, DialogField, DialogKind, PendingAction, Tab, WatchEvent, EventType};

#[test]
fn test_initial_state() {
    let app = App::new();
    assert_eq!(app.active_tab(), Tab::Dashboard);
    assert_eq!(app.current_prefix(), "/");
    assert!(app.watch_events().is_empty());
}

#[test]
fn test_tab_switching() {
    let mut app = App::new();
    app.toggle_tab();
    assert_eq!(app.active_tab(), Tab::Keys);
    app.toggle_tab();
    assert_eq!(app.active_tab(), Tab::Dashboard);
}

#[test]
fn test_prefix_navigation_descend() {
    let mut app = App::new();
    app.set_keys(vec![
        b"/a/1".to_vec(),
        b"/a/2".to_vec(),
        b"/b/1".to_vec(),
    ]);
    assert_eq!(app.visible_prefixes(), vec!["a/", "b/"]);
    app.descend("a/");
    assert_eq!(app.current_prefix(), "/a/");
    assert_eq!(app.visible_keys(), vec!["1", "2"]);
}

#[test]
fn test_prefix_navigation_ascend() {
    let mut app = App::new();
    app.set_keys(vec![b"/a/b/c".to_vec()]);
    app.descend("a/");
    app.descend("b/");
    assert_eq!(app.current_prefix(), "/a/b/");
    app.ascend();
    assert_eq!(app.current_prefix(), "/a/");
    app.ascend();
    assert_eq!(app.current_prefix(), "/");
}

#[test]
fn test_ascend_at_root_is_noop() {
    let mut app = App::new();
    app.ascend();
    assert_eq!(app.current_prefix(), "/");
}

#[test]
fn test_key_selection() {
    let mut app = App::new();
    app.set_keys(vec![b"/a".to_vec(), b"/b".to_vec(), b"/c".to_vec()]);
    assert_eq!(app.selected_index(), 0);
    app.move_down();
    assert_eq!(app.selected_index(), 1);
    app.move_down();
    assert_eq!(app.selected_index(), 2);
    app.move_down(); // should clamp
    assert_eq!(app.selected_index(), 2);
    app.move_up();
    assert_eq!(app.selected_index(), 1);
}

#[test]
fn test_watch_event_buffer() {
    let mut app = App::new();
    for i in 0..1100 {
        app.push_watch_event(WatchEvent {
            key: format!("/key/{}", i).into_bytes(),
            event_type: EventType::Put,
            value_size: 100,
        });
    }
    // Should cap at 1000.
    assert_eq!(app.watch_events().len(), 1000);
    // Most recent event should be last.
    assert_eq!(
        app.watch_events().last().unwrap().key,
        b"/key/1099"
    );
}

#[test]
fn test_search_filter() {
    let mut app = App::new();
    app.set_keys(vec![
        b"/app/nginx".to_vec(),
        b"/app/redis".to_vec(),
        b"/sys/kube".to_vec(),
    ]);
    app.set_search("nginx");
    let visible = app.filtered_keys();
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0], b"/app/nginx");
    app.clear_search();
    assert_eq!(app.filtered_keys().len(), 3);
}

#[test]
fn test_set_members() {
    let mut app = App::new();
    app.set_members(vec![
        ("node-0".to_string(), 1, true),
        ("node-1".to_string(), 2, false),
    ]);
    assert_eq!(app.members().len(), 2);
    assert!(app.members()[0].is_leader);
}

#[test]
fn test_put_dialog() {
    let mut app = App::new();
    assert!(!app.is_dialog_open());
    app.open_put_dialog();
    assert!(app.is_dialog_open());
    app.set_dialog_key("test/key");
    app.set_dialog_value("hello");
    assert_eq!(app.dialog_key(), "test/key");
    assert_eq!(app.dialog_value(), "hello");
    app.close_dialog();
    assert!(!app.is_dialog_open());
}

#[test]
fn test_delete_confirmation() {
    let mut app = App::new();
    app.set_keys(vec![b"/a".to_vec()]);
    app.open_delete_confirm();
    assert!(app.is_delete_confirm_open());
    app.close_dialog();
    assert!(!app.is_delete_confirm_open());
}

#[test]
fn test_dialog_type_char_put() {
    let mut app = App::new();
    app.open_put_dialog();
    assert_eq!(app.dialog_field(), DialogField::Key);

    app.dialog_type_char('h');
    app.dialog_type_char('i');
    assert_eq!(app.dialog_key(), "hi");
    assert_eq!(app.dialog_value(), "");

    app.toggle_dialog_field();
    assert_eq!(app.dialog_field(), DialogField::Value);
    app.dialog_type_char('v');
    assert_eq!(app.dialog_value(), "v");
}

#[test]
fn test_dialog_backspace() {
    let mut app = App::new();
    app.open_put_dialog();
    app.dialog_type_char('a');
    app.dialog_type_char('b');
    app.dialog_backspace();
    assert_eq!(app.dialog_key(), "a");
}

#[test]
fn test_confirm_put_creates_pending_action() {
    let mut app = App::new();
    app.open_put_dialog();
    app.dialog_type_char('k');
    app.toggle_dialog_field();
    app.dialog_type_char('v');
    let action = app.confirm_put();
    assert_eq!(
        action,
        Some(PendingAction::Put {
            key: "k".to_string(),
            value: "v".to_string()
        })
    );
    assert!(!app.is_dialog_open());
    // Should also be in pending_action.
    let taken = app.take_pending_action();
    assert_eq!(
        taken,
        Some(PendingAction::Put {
            key: "k".to_string(),
            value: "v".to_string()
        })
    );
    // Second take returns None.
    assert!(app.take_pending_action().is_none());
}

#[test]
fn test_confirm_put_empty_key_rejected() {
    let mut app = App::new();
    app.open_put_dialog();
    // Don't type anything — key is empty.
    let action = app.confirm_put();
    assert!(action.is_none());
}

#[test]
fn test_confirm_delete_creates_pending_action() {
    let mut app = App::new();
    app.set_keys(vec![b"/mykey".to_vec()]);
    // Select the leaf key (no prefixes at root for "/mykey").
    assert_eq!(app.selected_full_key(), Some("/mykey".to_string()));
    app.open_delete_confirm();
    let action = app.confirm_delete();
    assert_eq!(
        action,
        Some(PendingAction::Delete {
            key: "/mykey".to_string()
        })
    );
    assert!(!app.is_dialog_open());
}

#[test]
fn test_search_dialog_flow() {
    let mut app = App::new();
    app.set_keys(vec![
        b"/app/nginx".to_vec(),
        b"/app/redis".to_vec(),
        b"/sys/kube".to_vec(),
    ]);

    app.open_search_dialog();
    assert_eq!(app.dialog_kind(), Some(DialogKind::Search));

    app.dialog_type_char('n');
    app.dialog_type_char('g');
    app.dialog_type_char('i');
    app.dialog_type_char('n');
    app.dialog_type_char('x');
    assert_eq!(app.search_input(), "nginx");

    app.confirm_search();
    assert!(!app.is_dialog_open());
    assert_eq!(app.search_query(), Some("nginx"));

    let filtered = app.filtered_keys();
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0], b"/app/nginx");
}

#[test]
fn test_search_dialog_empty_clears_search() {
    let mut app = App::new();
    app.set_search("old");
    assert!(app.search_query().is_some());

    app.open_search_dialog();
    // Don't type anything — confirm empty clears search.
    app.confirm_search();
    assert!(app.search_query().is_none());
}
