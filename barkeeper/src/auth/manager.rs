/// A user in the auth system.
#[derive(Debug, Clone)]
pub struct User {
    pub name: String,
    pub password_hash: String,
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
