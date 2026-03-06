//! Stateless rendering functions for the TUI.
//!
//! All functions take an immutable `&App` reference and draw to the frame.

use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Block, Borders, Cell, List, ListItem, Paragraph, Row, Table, Tabs, Wrap,
};
use ratatui::Frame;

use crate::app::{App, DialogKind, EventType, Tab};

pub fn draw(f: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),  // tab bar
            Constraint::Min(10),   // main content
            Constraint::Length(1), // status line
        ])
        .split(f.area());

    draw_tab_bar(f, app, chunks[0]);

    match app.active_tab() {
        Tab::Dashboard => draw_dashboard(f, app, chunks[1]),
        Tab::Keys => draw_keys(f, app, chunks[1]),
    }

    draw_status_line(f, app, chunks[2]);

    // Draw dialog overlay if open.
    if let Some(kind) = app.dialog_kind() {
        draw_dialog(f, app, kind);
    }
}

fn draw_tab_bar(f: &mut Frame, app: &App, area: Rect) {
    let titles = vec!["Dashboard", "Keys"];
    let selected = match app.active_tab() {
        Tab::Dashboard => 0,
        Tab::Keys => 1,
    };
    let tabs = Tabs::new(titles)
        .select(selected)
        .block(Block::default().borders(Borders::ALL).title("bkctl"))
        .highlight_style(Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD));
    f.render_widget(tabs, area);
}

fn draw_dashboard(f: &mut Frame, app: &App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),  // cluster summary
            Constraint::Min(5),    // members table
            Constraint::Min(5),    // watch events
        ])
        .split(area);

    // Cluster summary
    let status_text = if let Some(status) = app.cluster_status() {
        format!(
            " Cluster: {} nodes | Leader: {} | Raft term: {}",
            status.member_count, status.leader_id, status.raft_term
        )
    } else {
        " Connecting...".to_string()
    };
    let summary = Paragraph::new(status_text)
        .block(Block::default().borders(Borders::ALL).title("Status"));
    f.render_widget(summary, chunks[0]);

    // Members table
    let header = Row::new(vec!["ID", "Name", "Status"])
        .style(Style::default().add_modifier(Modifier::BOLD));
    let rows: Vec<Row> = app
        .members()
        .iter()
        .map(|m| {
            let status = if m.is_leader { "Leader" } else { "Follower" };
            Row::new(vec![
                Cell::from(m.id.to_string()),
                Cell::from(m.name.clone()),
                Cell::from(status),
            ])
        })
        .collect();
    let table = Table::new(rows, [Constraint::Length(6), Constraint::Min(20), Constraint::Length(10)])
        .header(header)
        .block(Block::default().borders(Borders::ALL).title("Members"));
    f.render_widget(table, chunks[1]);

    // Watch events
    let items: Vec<ListItem> = app
        .watch_events()
        .iter()
        .rev()
        .take(chunks[2].height.saturating_sub(2) as usize)
        .map(|e| {
            let type_str = match e.event_type {
                EventType::Put => "PUT",
                EventType::Delete => "DEL",
            };
            let key = String::from_utf8_lossy(&e.key);
            let line = format!(" {} {:40} ({}B)", type_str, key, e.value_size);
            ListItem::new(line)
        })
        .collect();
    let events_list = List::new(items).block(
        Block::default()
            .borders(Borders::ALL)
            .title(format!("Watch Events [prefix: {}]", app.watch_prefix)),
    );
    f.render_widget(events_list, chunks[2]);
}

fn draw_keys(f: &mut Frame, app: &App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(5),    // split pane
            Constraint::Length(1), // keybindings bar
        ])
        .split(area);

    let panes = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(30), Constraint::Percentage(70)])
        .split(chunks[0]);

    // Left pane: prefix tree
    let mut items = Vec::new();
    let prefixes = app.visible_prefixes();
    let keys = app.visible_keys();

    for (i, p) in prefixes.iter().enumerate() {
        let style = if i == app.selected_index() {
            Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(Color::Cyan)
        };
        items.push(ListItem::new(format!("  {}", p)).style(style));
    }
    for (i, k) in keys.iter().enumerate() {
        let idx = prefixes.len() + i;
        let style = if idx == app.selected_index() {
            Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
        } else {
            Style::default()
        };
        let marker = if idx == app.selected_index() { "> " } else { "  " };
        items.push(ListItem::new(format!("{}{}", marker, k)).style(style));
    }

    let left = List::new(items).block(
        Block::default()
            .borders(Borders::ALL)
            .title(format!("Path: {}", app.current_prefix())),
    );
    f.render_widget(left, panes[0]);

    // Right pane: value display
    let value_content = if let Some(value) = app.selected_value() {
        // Try pretty-printing as JSON.
        if let Ok(json) = serde_json::from_slice::<serde_json::Value>(value) {
            serde_json::to_string_pretty(&json).unwrap_or_else(|_| {
                String::from_utf8_lossy(value).to_string()
            })
        } else {
            String::from_utf8_lossy(value).to_string()
        }
    } else {
        "Select a key to view its value".to_string()
    };

    let mut right_lines = Vec::new();
    if let Some(key) = app.selected_full_key() {
        right_lines.push(Line::from(vec![
            Span::styled("Key: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(key),
        ]));
    }
    if let Some(meta) = app.selected_key_meta() {
        right_lines.push(Line::from(format!(
            "Rev: {}  Mod: {}  Ver: {}  Lease: {}",
            meta.revision, meta.mod_revision, meta.version, meta.lease
        )));
        right_lines.push(Line::from(""));
    }
    for line in value_content.lines() {
        right_lines.push(Line::from(line.to_string()));
    }

    let right = Paragraph::new(right_lines)
        .block(Block::default().borders(Borders::ALL).title("Value"))
        .wrap(Wrap { trim: false });
    f.render_widget(right, panes[1]);

    // Keybindings bar
    let bindings = Paragraph::new(Line::from(vec![
        Span::styled("[p]", Style::default().fg(Color::Yellow)),
        Span::raw("ut "),
        Span::styled("[d]", Style::default().fg(Color::Yellow)),
        Span::raw("elete "),
        Span::styled("[/]", Style::default().fg(Color::Yellow)),
        Span::raw("search "),
        Span::styled("[r]", Style::default().fg(Color::Yellow)),
        Span::raw("efresh "),
        Span::styled("[q]", Style::default().fg(Color::Yellow)),
        Span::raw("uit"),
    ]));
    f.render_widget(bindings, chunks[1]);
}

fn draw_status_line(f: &mut Frame, app: &App, area: Rect) {
    let text = if app.endpoint.is_empty() {
        " bkctl".to_string()
    } else {
        format!(" bkctl | {}", app.endpoint)
    };
    let status = Paragraph::new(text).style(
        Style::default()
            .bg(Color::DarkGray)
            .fg(Color::White),
    );
    f.render_widget(status, area);
}

fn draw_dialog(f: &mut Frame, app: &App, kind: DialogKind) {
    let area = centered_rect(60, 20, f.area());

    // Clear the area.
    let clear = Block::default().style(Style::default().bg(Color::Black));
    f.render_widget(clear, area);

    match kind {
        DialogKind::Put => {
            let text = format!(
                "Key:   {}\nValue: {}\n\n[Enter] confirm  [Esc] cancel",
                app.dialog_key(),
                app.dialog_value()
            );
            let dialog = Paragraph::new(text)
                .block(Block::default().borders(Borders::ALL).title("Put Key"))
                .wrap(Wrap { trim: false });
            f.render_widget(dialog, area);
        }
        DialogKind::DeleteConfirm => {
            let key_name = app.selected_full_key().unwrap_or_default();
            let text = format!(
                "Delete key: {}?\n\n[y] yes  [n/Esc] cancel",
                key_name
            );
            let dialog = Paragraph::new(text)
                .block(Block::default().borders(Borders::ALL).title("Confirm Delete"))
                .wrap(Wrap { trim: false });
            f.render_widget(dialog, area);
        }
    }
}

/// Helper to create a centered rectangle.
fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}
