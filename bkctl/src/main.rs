use std::io;
use std::time::Duration;

use clap::Parser;
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;

use bkctl::app::{App, ClusterStatus, EventType, PendingAction, Tab, WatchEvent};
use bkctl::client::BkClient;
use bkctl::event::handle_key_event;
use bkctl::ui;

#[derive(Parser)]
#[command(name = "bkctl", about = "TUI for exploring barkeeper clusters")]
struct Cli {
    /// Comma-separated gRPC endpoints
    #[arg(long, default_value = "http://127.0.0.1:2379")]
    endpoints: String,

    /// Initial watch/browse prefix
    #[arg(long, default_value = "/")]
    prefix: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    // Connect to barkeeper.
    let client = BkClient::connect(&cli.endpoints).await?;

    // Set up terminal.
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Initialize app state.
    let mut app = App::new();
    app.endpoint = cli.endpoints.clone();
    app.watch_prefix = cli.prefix.clone();

    // Start watch stream.
    let mut watch_rx = client.watch(cli.prefix.as_bytes()).await.ok();

    // Periodic refresh ticker.
    let mut last_refresh = std::time::Instant::now();
    let refresh_interval = Duration::from_secs(2);

    // Initial data load.
    refresh_dashboard(&client, &mut app).await;
    refresh_keys(&client, &mut app).await;

    loop {
        // Draw.
        terminal.draw(|f| ui::draw(f, &app))?;

        // Poll watch events (non-blocking).
        if let Some(ref mut rx) = watch_rx {
            while let Ok(evt) = rx.try_recv() {
                let event_type = if evt.r#type == 0 {
                    EventType::Put
                } else {
                    EventType::Delete
                };
                let kv = evt.kv.as_ref();
                app.push_watch_event(WatchEvent {
                    key: kv.map(|k| k.key.clone()).unwrap_or_default(),
                    event_type,
                    value_size: kv.map(|k| k.value.len()).unwrap_or(0),
                });
            }
        }

        // Periodic refresh.
        if last_refresh.elapsed() >= refresh_interval {
            refresh_dashboard(&client, &mut app).await;
            if app.active_tab() == Tab::Keys {
                refresh_keys(&client, &mut app).await;
            }
            last_refresh = std::time::Instant::now();
        }

        // Handle input.
        if event::poll(Duration::from_millis(50))? {
            if let Event::Key(key) = event::read()? {
                handle_key_event(&mut app, key.code, key.modifiers);

                // Fetch value when Enter is pressed on a leaf key.
                if key.code == KeyCode::Enter && app.active_tab() == Tab::Keys {
                    if let Some(full_key) = app.selected_full_key() {
                        if let Ok(Some(kv)) = client.get(full_key.as_bytes()).await {
                            app.set_selected_value(Some(kv.value.clone()));
                            app.set_selected_key_meta(Some(bkctl::app::KeyMeta {
                                revision: kv.create_revision,
                                mod_revision: kv.mod_revision,
                                version: kv.version,
                                lease: kv.lease,
                            }));
                        }
                    }
                }

                // Handle pending actions from dialogs/keybindings.
                if let Some(action) = app.take_pending_action() {
                    match action {
                        PendingAction::Put { key, value } => {
                            let _ = client.put(key.as_bytes(), value.as_bytes()).await;
                            refresh_keys(&client, &mut app).await;
                        }
                        PendingAction::Delete { key } => {
                            let _ = client.delete(key.as_bytes()).await;
                            refresh_keys(&client, &mut app).await;
                        }
                        PendingAction::Refresh => {
                            refresh_dashboard(&client, &mut app).await;
                            refresh_keys(&client, &mut app).await;
                        }
                    }
                }
            }
        }

        if app.should_quit {
            break;
        }
    }

    // Restore terminal.
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    Ok(())
}

async fn refresh_dashboard(client: &BkClient, app: &mut App) {
    if let Ok(members) = client.member_list().await {
        if let Ok(status) = client.status().await {
            let member_data: Vec<(String, u64, bool)> = members
                .iter()
                .map(|m| (m.name.clone(), m.id, m.id == status.leader))
                .collect();
            app.set_cluster_status(ClusterStatus {
                leader_id: status.leader,
                raft_term: status.raft_term,
                member_count: members.len(),
            });
            app.set_members(member_data);
        }
    }
}

async fn refresh_keys(client: &BkClient, app: &mut App) {
    if let Ok(kvs) = client.get_prefix(app.current_prefix().as_bytes()).await {
        let keys: Vec<Vec<u8>> = kvs.into_iter().map(|kv| kv.key).collect();
        app.set_keys(keys);
    }
}
