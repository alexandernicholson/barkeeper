//! Rebar actor for authentication management.
//!
//! Converts the shared `AuthManager` into a message-passing actor that
//! serializes all access through a `tokio::sync::mpsc` command channel,
//! eliminating the need for `Arc<Mutex<...>>`.

use std::collections::HashMap;

use jsonwebtoken::{encode, decode, Header, Algorithm, Validation, EncodingKey, DecodingKey};
use serde::{Serialize, Deserialize};
use tokio::sync::{mpsc, oneshot};

use rebar_core::runtime::Runtime;

use crate::actors::commands::AuthCmd;
use crate::auth::manager::{Permission, Role, User};

#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    sub: String,
    exp: u64,
    iat: u64,
}

/// Spawn the authentication actor on the Rebar runtime.
///
/// The actor owns all auth state internally and processes commands
/// sequentially from the `cmd_rx` channel. Returns a lightweight handle
/// for sending commands.
pub async fn spawn_auth_actor(runtime: &Runtime) -> AuthActorHandle {
    let (cmd_tx, mut cmd_rx) = mpsc::channel::<AuthCmd>(256);

    runtime.spawn(move |mut ctx| async move {
        let mut enabled: bool = false;
        let mut users: HashMap<String, User> = HashMap::new();
        let mut roles: HashMap<String, Role> = HashMap::new();
        let jwt_secret: Vec<u8> = {
            use rand::Rng;
            let mut key = vec![0u8; 32];
            rand::thread_rng().fill(&mut key[..]);
            key
        };

        loop {
            tokio::select! {
                Some(cmd) = cmd_rx.recv() => {
                    match cmd {
                        AuthCmd::AuthEnable { reply } => {
                            enabled = true;
                            let _ = reply.send(());
                        }
                        AuthCmd::AuthDisable { reply } => {
                            enabled = false;
                            let _ = reply.send(());
                        }
                        AuthCmd::IsEnabled { reply } => {
                            let _ = reply.send(enabled);
                        }
                        AuthCmd::Authenticate { name, password, reply } => {
                            let user_data = users.get(&name).map(|u| {
                                (u.password_hash.clone(), name.clone())
                            });

                            match user_data {
                                Some((password_hash, user_name)) => {
                                    let secret = jwt_secret.clone();
                                    let result = tokio::task::spawn_blocking(move || {
                                        if bcrypt::verify(&password, &password_hash).unwrap_or(false) {
                                            let now = std::time::SystemTime::now()
                                                .duration_since(std::time::UNIX_EPOCH)
                                                .unwrap()
                                                .as_secs();
                                            let claims = Claims {
                                                sub: user_name.clone(),
                                                iat: now,
                                                exp: now + 300,
                                            };
                                            let token = encode(
                                                &Header::default(),
                                                &claims,
                                                &EncodingKey::from_secret(&secret),
                                            ).expect("JWT encoding should not fail");
                                            Some(token)
                                        } else {
                                            None
                                        }
                                    }).await.expect("bcrypt verify task panicked");

                                    let _ = reply.send(result);
                                }
                                None => {
                                    let _ = reply.send(None);
                                }
                            }
                        }
                        AuthCmd::ValidateToken { token, reply } => {
                            let mut validation = Validation::new(Algorithm::HS256);
                            validation.validate_exp = true;
                            match decode::<Claims>(&token, &DecodingKey::from_secret(&jwt_secret), &validation) {
                                Ok(token_data) => {
                                    let username = token_data.claims.sub;
                                    if users.contains_key(&username) {
                                        let _ = reply.send(Some(username));
                                    } else {
                                        let _ = reply.send(None);
                                    }
                                }
                                Err(_) => {
                                    let _ = reply.send(None);
                                }
                            }
                        }
                        AuthCmd::UserAdd { name, password, reply } => {
                            if users.contains_key(&name) {
                                let _ = reply.send(false);
                            } else {
                                // bcrypt::hash is also CPU-intensive.
                                let user_name = name.clone();
                                let hash = tokio::task::spawn_blocking(move || {
                                    bcrypt::hash(&password, 4).expect("bcrypt hash should not fail")
                                }).await.expect("bcrypt hash task panicked");

                                users.insert(
                                    user_name.clone(),
                                    User {
                                        name: user_name,
                                        password_hash: hash,
                                        roles: Vec::new(),
                                    },
                                );
                                let _ = reply.send(true);
                            }
                        }
                        AuthCmd::UserDelete { name, reply } => {
                            let _ = reply.send(users.remove(&name).is_some());
                        }
                        AuthCmd::UserGet { name, reply } => {
                            let _ = reply.send(users.get(&name).cloned());
                        }
                        AuthCmd::UserList { reply } => {
                            let mut names: Vec<String> = users.keys().cloned().collect();
                            names.sort();
                            let _ = reply.send(names);
                        }
                        AuthCmd::UserChangePassword { name, password, reply } => {
                            match users.get_mut(&name) {
                                Some(user) => {
                                    let hash = tokio::task::spawn_blocking(move || {
                                        bcrypt::hash(&password, 4).expect("bcrypt hash should not fail")
                                    }).await.expect("bcrypt hash task panicked");
                                    user.password_hash = hash;
                                    let _ = reply.send(true);
                                }
                                None => {
                                    let _ = reply.send(false);
                                }
                            }
                        }
                        AuthCmd::UserGrantRole { name, role, reply } => {
                            match users.get_mut(&name) {
                                Some(u) => {
                                    if u.roles.contains(&role) {
                                        let _ = reply.send(false);
                                    } else {
                                        u.roles.push(role);
                                        let _ = reply.send(true);
                                    }
                                }
                                None => {
                                    let _ = reply.send(false);
                                }
                            }
                        }
                        AuthCmd::UserRevokeRole { name, role, reply } => {
                            match users.get_mut(&name) {
                                Some(u) => {
                                    let before = u.roles.len();
                                    u.roles.retain(|r| r != &role);
                                    let _ = reply.send(u.roles.len() < before);
                                }
                                None => {
                                    let _ = reply.send(false);
                                }
                            }
                        }
                        AuthCmd::RoleAdd { name, reply } => {
                            if roles.contains_key(&name) {
                                let _ = reply.send(false);
                            } else {
                                roles.insert(
                                    name.clone(),
                                    Role {
                                        name,
                                        permissions: Vec::new(),
                                    },
                                );
                                let _ = reply.send(true);
                            }
                        }
                        AuthCmd::RoleDelete { name, reply } => {
                            let _ = reply.send(roles.remove(&name).is_some());
                        }
                        AuthCmd::RoleGet { name, reply } => {
                            let _ = reply.send(roles.get(&name).cloned());
                        }
                        AuthCmd::RoleList { reply } => {
                            let mut names: Vec<String> = roles.keys().cloned().collect();
                            names.sort();
                            let _ = reply.send(names);
                        }
                        AuthCmd::RoleGrantPermission { name, permission, reply } => {
                            match roles.get_mut(&name) {
                                Some(r) => {
                                    r.permissions.push(permission);
                                    let _ = reply.send(true);
                                }
                                None => {
                                    let _ = reply.send(false);
                                }
                            }
                        }
                        AuthCmd::RoleRevokePermission { name, key, range_end, reply } => {
                            match roles.get_mut(&name) {
                                Some(r) => {
                                    let before = r.permissions.len();
                                    r.permissions.retain(|p| p.key != key || p.range_end != range_end);
                                    let _ = reply.send(r.permissions.len() < before);
                                }
                                None => {
                                    let _ = reply.send(false);
                                }
                            }
                        }
                    }
                }
                // Also listen on Rebar mailbox for future distributed messages.
                Some(_msg) = ctx.recv() => {
                    // Reserved for future distributed auth coordination.
                }
                else => break,
            }
        }
    }).await;

    AuthActorHandle { cmd_tx }
}

/// Lightweight handle for communicating with the auth actor.
///
/// This replaces `Arc<AuthManager>` throughout the codebase. It is
/// cheaply cloneable and exposes the same async API as the old manager.
#[derive(Clone)]
pub struct AuthActorHandle {
    cmd_tx: mpsc::Sender<AuthCmd>,
}

impl AuthActorHandle {
    /// Enable authentication.
    pub async fn auth_enable(&self) {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx.send(AuthCmd::AuthEnable { reply }).await.expect("auth actor dead");
        rx.await.expect("auth actor dropped");
    }

    /// Disable authentication.
    pub async fn auth_disable(&self) {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx.send(AuthCmd::AuthDisable { reply }).await.expect("auth actor dead");
        rx.await.expect("auth actor dropped");
    }

    /// Check whether authentication is enabled.
    pub async fn is_enabled(&self) -> bool {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx.send(AuthCmd::IsEnabled { reply }).await.expect("auth actor dead");
        rx.await.expect("auth actor dropped")
    }

    /// Authenticate a user by name and password. Returns a token on success.
    pub async fn authenticate(&self, name: &str, password: &str) -> Option<String> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx.send(AuthCmd::Authenticate { name: name.to_string(), password: password.to_string(), reply }).await.expect("auth actor dead");
        rx.await.expect("auth actor dropped")
    }

    /// Validate a token. Returns the username if valid.
    pub async fn validate_token(&self, token: &str) -> Option<String> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx.send(AuthCmd::ValidateToken { token: token.to_string(), reply }).await.expect("auth actor dead");
        rx.await.expect("auth actor dropped")
    }

    /// Add a new user. Returns `true` if the user was created.
    pub async fn user_add(&self, name: String, password: String) -> bool {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx.send(AuthCmd::UserAdd { name, password, reply }).await.expect("auth actor dead");
        rx.await.expect("auth actor dropped")
    }

    /// Delete a user. Returns `true` if the user existed.
    pub async fn user_delete(&self, name: &str) -> bool {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx.send(AuthCmd::UserDelete { name: name.to_string(), reply }).await.expect("auth actor dead");
        rx.await.expect("auth actor dropped")
    }

    /// Get a user by name.
    pub async fn user_get(&self, name: &str) -> Option<User> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx.send(AuthCmd::UserGet { name: name.to_string(), reply }).await.expect("auth actor dead");
        rx.await.expect("auth actor dropped")
    }

    /// List all user names (sorted).
    pub async fn user_list(&self) -> Vec<String> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx.send(AuthCmd::UserList { reply }).await.expect("auth actor dead");
        rx.await.expect("auth actor dropped")
    }

    /// Change a user's password. Returns `true` if the user exists.
    pub async fn user_change_password(&self, name: &str, password: String) -> bool {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx.send(AuthCmd::UserChangePassword { name: name.to_string(), password, reply }).await.expect("auth actor dead");
        rx.await.expect("auth actor dropped")
    }

    /// Grant a role to a user. Returns `true` if the grant succeeded.
    pub async fn user_grant_role(&self, user: &str, role: &str) -> bool {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx.send(AuthCmd::UserGrantRole { name: user.to_string(), role: role.to_string(), reply }).await.expect("auth actor dead");
        rx.await.expect("auth actor dropped")
    }

    /// Revoke a role from a user. Returns `true` if the role was removed.
    pub async fn user_revoke_role(&self, user: &str, role: &str) -> bool {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx.send(AuthCmd::UserRevokeRole { name: user.to_string(), role: role.to_string(), reply }).await.expect("auth actor dead");
        rx.await.expect("auth actor dropped")
    }

    /// Add a new role. Returns `true` if the role was created.
    pub async fn role_add(&self, name: String) -> bool {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx.send(AuthCmd::RoleAdd { name, reply }).await.expect("auth actor dead");
        rx.await.expect("auth actor dropped")
    }

    /// Delete a role. Returns `true` if the role existed.
    pub async fn role_delete(&self, name: &str) -> bool {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx.send(AuthCmd::RoleDelete { name: name.to_string(), reply }).await.expect("auth actor dead");
        rx.await.expect("auth actor dropped")
    }

    /// Get a role by name.
    pub async fn role_get(&self, name: &str) -> Option<Role> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx.send(AuthCmd::RoleGet { name: name.to_string(), reply }).await.expect("auth actor dead");
        rx.await.expect("auth actor dropped")
    }

    /// List all role names (sorted).
    pub async fn role_list(&self) -> Vec<String> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx.send(AuthCmd::RoleList { reply }).await.expect("auth actor dead");
        rx.await.expect("auth actor dropped")
    }

    /// Grant a permission to a role. Returns `true` if the role exists.
    pub async fn role_grant_permission(&self, role: &str, permission: Permission) -> bool {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx.send(AuthCmd::RoleGrantPermission { name: role.to_string(), permission, reply }).await.expect("auth actor dead");
        rx.await.expect("auth actor dropped")
    }

    /// Revoke a permission from a role. Returns `true` if a matching
    /// permission was found and removed.
    pub async fn role_revoke_permission(&self, role: &str, key: &[u8], range_end: &[u8]) -> bool {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx.send(AuthCmd::RoleRevokePermission { name: role.to_string(), key: key.to_vec(), range_end: range_end.to_vec(), reply }).await.expect("auth actor dead");
        rx.await.expect("auth actor dropped")
    }
}
