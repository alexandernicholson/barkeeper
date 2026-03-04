use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::Mutex;

/// A user in the auth system.
#[derive(Debug, Clone)]
pub struct User {
    pub name: String,
    pub password_hash: Vec<u8>,
    pub roles: Vec<String>,
}

/// A role in the auth system.
#[derive(Debug, Clone)]
pub struct Role {
    pub name: String,
    pub permissions: Vec<Permission>,
}

/// A permission granting access to a key range.
#[derive(Debug, Clone)]
pub struct Permission {
    /// 0 = READ, 1 = WRITE, 2 = READWRITE
    pub perm_type: i32,
    pub key: Vec<u8>,
    pub range_end: Vec<u8>,
}

/// In-memory RBAC auth manager.
///
/// Manages users, roles, and permissions for the barkeeper auth system.
/// All operations are async and use interior mutability via `Arc<Mutex<_>>`.
pub struct AuthManager {
    enabled: Arc<Mutex<bool>>,
    users: Arc<Mutex<HashMap<String, User>>>,
    roles: Arc<Mutex<HashMap<String, Role>>>,
}

impl AuthManager {
    pub fn new() -> Self {
        AuthManager {
            enabled: Arc::new(Mutex::new(false)),
            users: Arc::new(Mutex::new(HashMap::new())),
            roles: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Enable authentication.
    pub async fn auth_enable(&self) {
        let mut enabled = self.enabled.lock().await;
        *enabled = true;
    }

    /// Disable authentication.
    pub async fn auth_disable(&self) {
        let mut enabled = self.enabled.lock().await;
        *enabled = false;
    }

    /// Check whether authentication is enabled.
    pub async fn is_enabled(&self) -> bool {
        let enabled = self.enabled.lock().await;
        *enabled
    }

    /// Authenticate a user by name and password.
    ///
    /// Returns a simple token string on success, or `None` if the user does
    /// not exist or the password does not match.
    pub async fn authenticate(&self, name: &str, password: &str) -> Option<String> {
        let users = self.users.lock().await;
        let user = users.get(name)?;
        let password_hash = simple_hash(password);
        if user.password_hash == password_hash {
            // Return a simple token: "name.simple-token"
            Some(format!("{}.{}", name, hex::encode(&password_hash[..8])))
        } else {
            None
        }
    }

    /// Add a new user. Returns `true` if the user was created, `false` if it
    /// already exists.
    pub async fn user_add(&self, name: String, password: String) -> bool {
        let mut users = self.users.lock().await;
        if users.contains_key(&name) {
            return false;
        }
        let password_hash = simple_hash(&password);
        users.insert(
            name.clone(),
            User {
                name,
                password_hash,
                roles: Vec::new(),
            },
        );
        true
    }

    /// Delete a user. Returns `true` if the user existed and was removed.
    pub async fn user_delete(&self, name: &str) -> bool {
        let mut users = self.users.lock().await;
        users.remove(name).is_some()
    }

    /// Get a user by name.
    pub async fn user_get(&self, name: &str) -> Option<User> {
        let users = self.users.lock().await;
        users.get(name).cloned()
    }

    /// List all user names.
    pub async fn user_list(&self) -> Vec<String> {
        let users = self.users.lock().await;
        let mut names: Vec<String> = users.keys().cloned().collect();
        names.sort();
        names
    }

    /// Change a user's password. Returns `true` if the user exists.
    pub async fn user_change_password(&self, name: &str, password: String) -> bool {
        let mut users = self.users.lock().await;
        match users.get_mut(name) {
            Some(user) => {
                user.password_hash = simple_hash(&password);
                true
            }
            None => false,
        }
    }

    /// Grant a role to a user. Returns `true` if the grant succeeded (user
    /// exists and did not already have the role).
    pub async fn user_grant_role(&self, user: &str, role: &str) -> bool {
        let mut users = self.users.lock().await;
        match users.get_mut(user) {
            Some(u) => {
                if u.roles.contains(&role.to_string()) {
                    return false;
                }
                u.roles.push(role.to_string());
                true
            }
            None => false,
        }
    }

    /// Revoke a role from a user. Returns `true` if the role was removed.
    pub async fn user_revoke_role(&self, user: &str, role: &str) -> bool {
        let mut users = self.users.lock().await;
        match users.get_mut(user) {
            Some(u) => {
                let before = u.roles.len();
                u.roles.retain(|r| r != role);
                u.roles.len() < before
            }
            None => false,
        }
    }

    /// Add a new role. Returns `true` if the role was created.
    pub async fn role_add(&self, name: String) -> bool {
        let mut roles = self.roles.lock().await;
        if roles.contains_key(&name) {
            return false;
        }
        roles.insert(
            name.clone(),
            Role {
                name,
                permissions: Vec::new(),
            },
        );
        true
    }

    /// Delete a role. Returns `true` if the role existed.
    pub async fn role_delete(&self, name: &str) -> bool {
        let mut roles = self.roles.lock().await;
        roles.remove(name).is_some()
    }

    /// Get a role by name.
    pub async fn role_get(&self, name: &str) -> Option<Role> {
        let roles = self.roles.lock().await;
        roles.get(name).cloned()
    }

    /// List all role names.
    pub async fn role_list(&self) -> Vec<String> {
        let roles = self.roles.lock().await;
        let mut names: Vec<String> = roles.keys().cloned().collect();
        names.sort();
        names
    }

    /// Grant a permission to a role. Returns `true` if the role exists.
    pub async fn role_grant_permission(&self, role: &str, perm: Permission) -> bool {
        let mut roles = self.roles.lock().await;
        match roles.get_mut(role) {
            Some(r) => {
                r.permissions.push(perm);
                true
            }
            None => false,
        }
    }

    /// Revoke a permission from a role by matching key and range_end.
    /// Returns `true` if a matching permission was found and removed.
    pub async fn role_revoke_permission(&self, role: &str, key: &[u8], range_end: &[u8]) -> bool {
        let mut roles = self.roles.lock().await;
        match roles.get_mut(role) {
            Some(r) => {
                let before = r.permissions.len();
                r.permissions
                    .retain(|p| p.key != key || p.range_end != range_end);
                r.permissions.len() < before
            }
            None => false,
        }
    }
}

/// Simple hash function for passwords (not cryptographically secure --
/// production would use bcrypt/scrypt/argon2).
fn simple_hash(input: &str) -> Vec<u8> {
    // Simple FNV-1a-inspired hash producing 32 bytes for demo purposes.
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in input.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    let mut result = Vec::with_capacity(32);
    // Expand to 32 bytes by hashing iterations.
    for i in 0u64..4 {
        let h = hash.wrapping_add(i).wrapping_mul(0x517cc1b727220a95);
        result.extend_from_slice(&h.to_le_bytes());
    }
    result
}

/// Simple hex encoding (avoids adding a dependency).
mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{:02x}", b)).collect()
    }
}
