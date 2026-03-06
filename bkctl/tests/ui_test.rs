//! Rendering layer tests — use TestBackend to verify TUI output.

use ratatui::backend::TestBackend;
use ratatui::Terminal;

use bkctl::app::{App, ClusterStatus, EventType, WatchEvent};
use bkctl::ui;

fn buffer_to_string(buf: &ratatui::buffer::Buffer) -> String {
    let mut s = String::new();
    for y in 0..buf.area.height {
        for x in 0..buf.area.width {
            s.push(buf.cell((x, y)).unwrap().symbol().chars().next().unwrap_or(' '));
        }
        s.push('\n');
    }
    s
}

#[test]
fn test_dashboard_renders_members() {
    let mut app = App::new();
    app.set_members(vec![
        ("node-0".to_string(), 1, true),
        ("node-1".to_string(), 2, false),
    ]);
    app.set_cluster_status(ClusterStatus {
        leader_id: 1,
        raft_term: 42,
        member_count: 2,
    });

    let mut terminal = Terminal::new(TestBackend::new(80, 24)).unwrap();
    terminal
        .draw(|f| ui::draw(f, &app))
        .unwrap();
    let buf = terminal.backend().buffer().clone();
    let content = buffer_to_string(&buf);

    assert!(content.contains("node-0"), "should show member name");
    assert!(content.contains("Leader"), "should show leader status");
    assert!(content.contains("node-1"), "should show second member");
}

#[test]
fn test_dashboard_renders_watch_events() {
    let mut app = App::new();
    app.push_watch_event(WatchEvent {
        key: b"/test/key".to_vec(),
        event_type: EventType::Put,
        value_size: 1024,
    });

    let mut terminal = Terminal::new(TestBackend::new(80, 24)).unwrap();
    terminal.draw(|f| ui::draw(f, &app)).unwrap();
    let buf = terminal.backend().buffer().clone();
    let content = buffer_to_string(&buf);

    assert!(content.contains("/test/key"), "should show watch event key");
    assert!(content.contains("PUT"), "should show event type");
}

#[test]
fn test_keys_tab_renders_prefixes_and_keys() {
    let mut app = App::new();
    app.toggle_tab(); // Switch to Keys tab
    app.set_keys(vec![
        b"/app/web".to_vec(),
        b"/app/api".to_vec(),
        b"/sys/core".to_vec(),
    ]);

    let mut terminal = Terminal::new(TestBackend::new(80, 24)).unwrap();
    terminal.draw(|f| ui::draw(f, &app)).unwrap();
    let buf = terminal.backend().buffer().clone();
    let content = buffer_to_string(&buf);

    assert!(content.contains("app/"), "should show app/ prefix");
    assert!(content.contains("sys/"), "should show sys/ prefix");
}

#[test]
fn test_tab_indicator_shows_active() {
    let app = App::new();
    let mut terminal = Terminal::new(TestBackend::new(80, 24)).unwrap();
    terminal.draw(|f| ui::draw(f, &app)).unwrap();
    let buf = terminal.backend().buffer().clone();
    let content = buffer_to_string(&buf);
    assert!(content.contains("Dashboard"), "should show Dashboard tab");
    assert!(content.contains("Keys"), "should show Keys tab");
}

#[test]
fn test_status_line_shows_endpoint() {
    let mut app = App::new();
    app.endpoint = "http://127.0.0.1:2379".to_string();

    let mut terminal = Terminal::new(TestBackend::new(80, 24)).unwrap();
    terminal.draw(|f| ui::draw(f, &app)).unwrap();
    let buf = terminal.backend().buffer().clone();
    let content = buffer_to_string(&buf);

    assert!(
        content.contains("127.0.0.1:2379"),
        "should show endpoint in status line"
    );
}
