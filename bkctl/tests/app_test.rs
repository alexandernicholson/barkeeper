//! State/model layer tests — pure functions, no I/O, no server.

use bkctl::app::{App, Tab, WatchEvent, EventType};

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
