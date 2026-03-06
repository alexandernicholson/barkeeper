//! Terminal event handling — maps crossterm events to app state transitions.

use crossterm::event::KeyCode;
use crossterm::event::KeyModifiers;

use crate::app::{App, DialogKind, PendingAction, Tab};

/// Handle a key event, mutating app state. Returns true if the event was consumed.
pub fn handle_key_event(app: &mut App, code: KeyCode, modifiers: KeyModifiers) -> bool {
    // Dialog mode takes priority.
    if app.is_dialog_open() {
        return handle_dialog_key(app, code);
    }

    match code {
        KeyCode::Char('q') | KeyCode::Esc => {
            app.should_quit = true;
            true
        }
        KeyCode::Tab => {
            app.toggle_tab();
            true
        }
        KeyCode::Char('j') | KeyCode::Down => {
            app.move_down();
            true
        }
        KeyCode::Char('k') | KeyCode::Up => {
            app.move_up();
            true
        }
        KeyCode::Enter => {
            if app.active_tab() == Tab::Keys {
                // If selected item is a prefix, descend.
                let prefixes = app.visible_prefixes();
                if app.selected_index() < prefixes.len() {
                    let p = prefixes[app.selected_index()].clone();
                    app.descend(&p);
                }
                // Otherwise it's a leaf key — triggers value fetch (handled by main loop).
            }
            true
        }
        KeyCode::Backspace => {
            if app.active_tab() == Tab::Keys {
                app.ascend();
            }
            true
        }
        KeyCode::Char('p') => {
            if app.active_tab() == Tab::Keys {
                app.open_put_dialog();
            }
            true
        }
        KeyCode::Char('d') => {
            if app.active_tab() == Tab::Keys {
                app.open_delete_confirm();
            }
            true
        }
        KeyCode::Char('/') => {
            if app.active_tab() == Tab::Keys {
                app.open_search_dialog();
            }
            true
        }
        KeyCode::Char('r') => {
            app.pending_action = Some(PendingAction::Refresh);
            true
        }
        KeyCode::Char('c') if modifiers.contains(KeyModifiers::CONTROL) => {
            app.should_quit = true;
            true
        }
        _ => false,
    }
}

fn handle_dialog_key(app: &mut App, code: KeyCode) -> bool {
    match app.dialog_kind() {
        Some(DialogKind::Put) => handle_put_dialog_key(app, code),
        Some(DialogKind::DeleteConfirm) => handle_delete_dialog_key(app, code),
        Some(DialogKind::Search) => handle_search_dialog_key(app, code),
        None => false,
    }
}

fn handle_put_dialog_key(app: &mut App, code: KeyCode) -> bool {
    match code {
        KeyCode::Esc => {
            app.close_dialog();
            true
        }
        KeyCode::Enter => {
            app.confirm_put();
            true
        }
        KeyCode::Tab => {
            app.toggle_dialog_field();
            true
        }
        KeyCode::Backspace => {
            app.dialog_backspace();
            true
        }
        KeyCode::Char(c) => {
            app.dialog_type_char(c);
            true
        }
        _ => true,
    }
}

fn handle_delete_dialog_key(app: &mut App, code: KeyCode) -> bool {
    match code {
        KeyCode::Char('y') => {
            app.confirm_delete();
            true
        }
        KeyCode::Char('n') | KeyCode::Esc => {
            app.close_dialog();
            true
        }
        _ => true,
    }
}

fn handle_search_dialog_key(app: &mut App, code: KeyCode) -> bool {
    match code {
        KeyCode::Esc => {
            app.close_dialog();
            true
        }
        KeyCode::Enter => {
            app.confirm_search();
            true
        }
        KeyCode::Backspace => {
            app.dialog_backspace();
            true
        }
        KeyCode::Char(c) => {
            app.dialog_type_char(c);
            true
        }
        _ => true,
    }
}
