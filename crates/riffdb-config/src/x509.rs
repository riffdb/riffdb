//! Deterministic pure-Rust ECDSA P-256 (`secp256r1`) PKCS#8 and X.509 v3
//! certificate provisioning for direct-TLS and mutual-TLS (mTLS) bundles.

use std::fmt::Write as _;
use std::fs;
use std::io::Write as _;
use std::net::IpAddr;
#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::{CanonicalHttpsEndpoint, RemoteConfigError, TlsServerIdentity};

/// One complete PEM-encoded certificate and PKCS#8 v1 private-key pair.
#[derive(Clone, Eq, PartialEq)]
pub struct IssuedTlsIdentityPem {
    certificate_pem: String,
    private_key_pem: String,
}

impl IssuedTlsIdentityPem {
    /// Returns the PEM-encoded X.509 v3 certificate (or chain).
    #[must_use]
    pub fn certificate_pem(&self) -> &str {
        &self.certificate_pem
    }

    /// Returns the PEM-encoded PKCS#8 v1 ECDSA P-256 private key.
    #[must_use]
    pub fn private_key_pem(&self) -> &str {
        &self.private_key_pem
    }
}

impl std::fmt::Debug for IssuedTlsIdentityPem {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IssuedTlsIdentityPem")
            .field("certificate_pem", &"[PEM CERTIFICATE]")
            .field("private_key_pem", &"[REDACTED PRIVATE KEY]")
            .finish()
    }
}

/// Complete provisioned X.509 v3 bundle for direct-TLS and optional mTLS.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IssuedTlsBundle {
    ca: IssuedTlsIdentityPem,
    server: IssuedTlsIdentityPem,
    client: Option<IssuedTlsIdentityPem>,
}

impl IssuedTlsBundle {
    /// Returns the CA root certificate and private key.
    #[must_use]
    pub const fn ca(&self) -> &IssuedTlsIdentityPem {
        &self.ca
    }

    /// Returns the SAN-bound server leaf certificate and private key.
    #[must_use]
    pub const fn server(&self) -> &IssuedTlsIdentityPem {
        &self.server
    }

    /// Returns the optional mTLS client leaf certificate and private key.
    #[must_use]
    pub const fn client(&self) -> Option<&IssuedTlsIdentityPem> {
        self.client.as_ref()
    }
}

/// Maximum Subject Alternative Name entries allowed in one server certificate.
pub const MAX_TLS_SUBJECT_ALT_NAMES: usize = 32;

/// Issues a complete X.509 v3 bundle (CA, SAN-bound server leaf, and optional mTLS client leaf)
/// from a 32-byte entropy seed for one or more canonical server identities.
pub fn issue_tls_bundle_for_identities_from_seed(
    seed: [u8; 32],
    identities: &[TlsServerIdentity],
    now_unix_seconds: u64,
    validity_days: u16,
    issue_client: bool,
) -> Result<IssuedTlsBundle, RemoteConfigError> {
    if validity_days == 0
        || validity_days > 3_650
        || identities.is_empty()
        || identities.len() > MAX_TLS_SUBJECT_ALT_NAMES
    {
        return Err(RemoteConfigError::InvalidBound);
    }
    let mut unique_identities: Vec<TlsServerIdentity> = Vec::with_capacity(identities.len());
    for identity in identities {
        if !unique_identities.contains(identity) {
            unique_identities.push(identity.clone());
        }
    }
    let not_before = now_unix_seconds.saturating_sub(300);
    let not_after = now_unix_seconds.saturating_add(u64::from(validity_days) * 86_400);

    let ca_scalar = derive_nonzero_scalar(&seed, b"riffdb.x509.ca.key.v1");
    let ca_pub = p256_PublicKey::from_scalar(&ca_scalar);
    let ca_ski = key_identifier(&ca_pub);
    let ca_serial = derive_serial(&seed, b"riffdb.x509.ca.serial.v1");
    let ca_tbs = encode_tbs_certificate(
        &ca_serial,
        "RiffDB Local CA",
        "RiffDB Local CA",
        not_before,
        not_after,
        &ca_pub,
        &encode_ca_extensions(&ca_ski),
    );
    let ca_sig = ecdsa_p256_sign_deterministic(&ca_scalar, &ca_tbs, b"riffdb.x509.ca.sig.v1");
    let ca_cert_der = encode_signed_certificate(&ca_tbs, &ca_sig);
    let ca_key_der = encode_pkcs8_p256_private_key(&ca_scalar, &ca_pub);

    let server_scalar = derive_nonzero_scalar(&seed, b"riffdb.x509.server.key.v1");
    let server_pub = p256_PublicKey::from_scalar(&server_scalar);
    let server_ski = key_identifier(&server_pub);
    let server_serial = derive_serial(&seed, b"riffdb.x509.server.serial.v1");
    let server_cn = unique_identities[0].as_str();
    let server_tbs = encode_tbs_certificate(
        &server_serial,
        "RiffDB Local CA",
        &server_cn,
        not_before,
        not_after,
        &server_pub,
        &encode_server_extensions(&server_ski, &ca_ski, &unique_identities),
    );
    let server_sig =
        ecdsa_p256_sign_deterministic(&ca_scalar, &server_tbs, b"riffdb.x509.server.sig.v1");
    let server_cert_der = encode_signed_certificate(&server_tbs, &server_sig);
    let server_key_der = encode_pkcs8_p256_private_key(&server_scalar, &server_pub);

    let client = if issue_client {
        let client_scalar = derive_nonzero_scalar(&seed, b"riffdb.x509.client.key.v1");
        let client_pub = p256_PublicKey::from_scalar(&client_scalar);
        let client_ski = key_identifier(&client_pub);
        let client_serial = derive_serial(&seed, b"riffdb.x509.client.serial.v1");
        let client_tbs = encode_tbs_certificate(
            &client_serial,
            "RiffDB Local CA",
            "riffdb-mtls-client",
            not_before,
            not_after,
            &client_pub,
            &encode_client_extensions(&client_ski, &ca_ski),
        );
        let client_sig =
            ecdsa_p256_sign_deterministic(&ca_scalar, &client_tbs, b"riffdb.x509.client.sig.v1");
        let client_cert_der = encode_signed_certificate(&client_tbs, &client_sig);
        let client_key_der = encode_pkcs8_p256_private_key(&client_scalar, &client_pub);
        Some(IssuedTlsIdentityPem {
            certificate_pem: encode_pem("CERTIFICATE", &client_cert_der),
            private_key_pem: encode_pem("PRIVATE KEY", &client_key_der),
        })
    } else {
        None
    };

    Ok(IssuedTlsBundle {
        ca: IssuedTlsIdentityPem {
            certificate_pem: encode_pem("CERTIFICATE", &ca_cert_der),
            private_key_pem: encode_pem("PRIVATE KEY", &ca_key_der),
        },
        server: IssuedTlsIdentityPem {
            certificate_pem: encode_pem("CERTIFICATE", &server_cert_der),
            private_key_pem: encode_pem("PRIVATE KEY", &server_key_der),
        },
        client,
    })
}

/// Issues a complete X.509 v3 bundle (CA, SAN-bound server leaf, and optional mTLS client leaf)
/// from a 32-byte entropy seed.
pub fn issue_tls_bundle_from_seed(
    seed: [u8; 32],
    endpoint: &CanonicalHttpsEndpoint,
    now_unix_seconds: u64,
    validity_days: u16,
    issue_client: bool,
) -> Result<IssuedTlsBundle, RemoteConfigError> {
    issue_tls_bundle_for_identities_from_seed(
        seed,
        std::slice::from_ref(endpoint.identity()),
        now_unix_seconds,
        validity_days,
        issue_client,
    )
}

/// Generates a 32-byte seed from OS randomness (`/dev/urandom` on Unix, mixed with
/// process/timestamp entropy) and issues a complete X.509 v3 bundle for one or more server identities.
pub fn issue_tls_bundle_for_identities(
    identities: &[TlsServerIdentity],
    validity_days: u16,
    issue_client: bool,
) -> Result<IssuedTlsBundle, RemoteConfigError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(1_767_225_600))
        .as_secs();
    let seed = read_os_entropy_seed(now)?;
    issue_tls_bundle_for_identities_from_seed(seed, identities, now, validity_days, issue_client)
}

/// Generates a 32-byte seed from OS randomness (`/dev/urandom` on Unix, mixed with
/// process/timestamp entropy) and issues a complete X.509 v3 bundle.
pub fn issue_tls_bundle(
    endpoint: &CanonicalHttpsEndpoint,
    validity_days: u16,
    issue_client: bool,
) -> Result<IssuedTlsBundle, RemoteConfigError> {
    issue_tls_bundle_for_identities(
        std::slice::from_ref(endpoint.identity()),
        validity_days,
        issue_client,
    )
}

/// Atomically writes a PEM file with mode `0600` (when `private` is true) or `0644` (when false).
pub fn write_protected_pem_file(path: &Path, contents: &str, private: bool) -> std::io::Result<()> {
    let parent = path.parent().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "missing parent directory")
    })?;
    fs::create_dir_all(parent)?;
    #[cfg(unix)]
    {
        if let Ok(meta) = fs::symlink_metadata(parent) {
            let mode = meta.permissions().mode() & 0o777;
            if mode & 0o022 != 0 {
                let _ = fs::set_permissions(parent, fs::Permissions::from_mode(mode & !0o022));
            }
        }
    }
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("tls.pem");
    let staging = parent.join(format!(".{file_name}.tmp.{}", std::process::id()));
    let _ = fs::remove_file(&staging);
    #[cfg(unix)]
    let mode = if private { 0o600 } else { 0o644 };
    {
        let mut options = fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(mode);
        let mut file = options.open(&staging)?;
        file.write_all(contents.as_bytes())?;
        file.sync_all()?;
    }
    #[cfg(unix)]
    fs::set_permissions(&staging, fs::Permissions::from_mode(mode))?;
    fs::rename(&staging, path)?;
    Ok(())
}

fn read_os_entropy_seed(now_unix_seconds: u64) -> Result<[u8; 32], RemoteConfigError> {
    use std::io::Read as _;
    let mut raw = [0_u8; 32];
    let mut file = fs::File::open("/dev/urandom").map_err(|_| RemoteConfigError::InvalidBound)?;
    file.read_exact(&mut raw)
        .map_err(|_| RemoteConfigError::InvalidBound)?;
    let mut material = Vec::with_capacity(48);
    material.extend_from_slice(&raw);
    material.extend_from_slice(&now_unix_seconds.to_be_bytes());
    material.extend_from_slice(&std::process::id().to_be_bytes());
    Ok(sha256(&material))
}

// ============================================================================
// Pure-Rust SHA-256
// ============================================================================

const SHA256_K: [u32; 64] = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];

fn sha256(data: &[u8]) -> [u8; 32] {
    let mut state: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];
    let bit_len = (data.len() as u64).wrapping_mul(8);
    let mut padded = Vec::with_capacity((data.len() + 72) & !63);
    padded.extend_from_slice(data);
    padded.push(0x80);
    while (padded.len() % 64) != 56 {
        padded.push(0);
    }
    padded.extend_from_slice(&bit_len.to_be_bytes());

    for chunk in padded.chunks_exact(64) {
        let mut w = [0_u32; 64];
        for (i, word) in chunk.chunks_exact(4).enumerate() {
            w[i] = u32::from_be_bytes([word[0], word[1], word[2], word[3]]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }
        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut h] = state;
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let temp1 = h
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(SHA256_K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = s0.wrapping_add(maj);

            h = g;
            g = f;
            f = e;
            e = d.wrapping_add(temp1);
            d = c;
            c = b;
            b = a;
            a = temp1.wrapping_add(temp2);
        }
        state[0] = state[0].wrapping_add(a);
        state[1] = state[1].wrapping_add(b);
        state[2] = state[2].wrapping_add(c);
        state[3] = state[3].wrapping_add(d);
        state[4] = state[4].wrapping_add(e);
        state[5] = state[5].wrapping_add(f);
        state[6] = state[6].wrapping_add(g);
        state[7] = state[7].wrapping_add(h);
    }

    let mut out = [0_u8; 32];
    for (i, word) in state.iter().enumerate() {
        out[i * 4..(i + 1) * 4].copy_from_slice(&word.to_be_bytes());
    }
    out
}

// ============================================================================
// Pure-Rust 256-bit arithmetic & NIST P-256 (secp256r1)
// ============================================================================

/// Little-endian 4x64-bit unsigned integer (`limbs[0]` is least significant).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct U256([u64; 4]);

const P256_P: U256 = U256([
    0xffff_ffff_ffff_ffff,
    0x0000_0000_ffff_ffff,
    0x0000_0000_0000_0000,
    0xffff_ffff_0000_0001,
]);

const P256_P_MINUS_2: U256 = U256([
    0xffff_ffff_ffff_fffd,
    0x0000_0000_ffff_ffff,
    0x0000_0000_0000_0000,
    0xffff_ffff_0000_0001,
]);

const P256_N: U256 = U256([
    0xf3b9_cac2_fc63_2551,
    0xbce6_faad_a717_9e84,
    0xffff_ffff_ffff_ffff,
    0xffff_ffff_0000_0000,
]);

const P256_N_MINUS_2: U256 = U256([
    0xf3b9_cac2_fc63_254f,
    0xbce6_faad_a717_9e84,
    0xffff_ffff_ffff_ffff,
    0xffff_ffff_0000_0000,
]);

const P256_GX: U256 = U256([
    0xf4a1_3945_d898_c296,
    0x7703_7d81_2deb_33a0,
    0xf8bc_e6e5_63a4_40f2,
    0x6b17_d1f2_e12c_4247,
]);

const P256_GY: U256 = U256([
    0xcbb6_4068_37bf_51f5,
    0x2bce_3357_6b31_5ece,
    0x8ee7_eb4a_7c0f_9e16,
    0x4fe3_42e2_fe1a_7f9b,
]);

impl U256 {
    const ZERO: Self = Self([0, 0, 0, 0]);
    const ONE: Self = Self([1, 0, 0, 0]);

    fn from_be_bytes(bytes: &[u8; 32]) -> Self {
        let mut limbs = [0_u64; 4];
        for (i, limb) in limbs.iter_mut().enumerate() {
            let offset = (3 - i) * 8;
            let mut chunk = [0_u8; 8];
            chunk.copy_from_slice(&bytes[offset..offset + 8]);
            *limb = u64::from_be_bytes(chunk);
        }
        Self(limbs)
    }

    fn to_be_bytes(self) -> [u8; 32] {
        let mut out = [0_u8; 32];
        for i in 0..4 {
            let offset = (3 - i) * 8;
            out[offset..offset + 8].copy_from_slice(&self.0[i].to_be_bytes());
        }
        out
    }

    fn is_zero(self) -> bool {
        self.0 == [0, 0, 0, 0]
    }

    fn bit(self, index: usize) -> bool {
        let limb = index / 64;
        let shift = index % 64;
        ((self.0[limb] >> shift) & 1) == 1
    }

    fn ge(self, other: Self) -> bool {
        for i in (0..4).rev() {
            if self.0[i] != other.0[i] {
                return self.0[i] > other.0[i];
            }
        }
        true
    }

    fn add_with_carry(self, other: Self) -> (Self, bool) {
        let mut out = [0_u64; 4];
        let mut carry = 0_u128;
        for (i, slot) in out.iter_mut().enumerate() {
            let sum = u128::from(self.0[i]) + u128::from(other.0[i]) + carry;
            *slot = sum as u64;
            carry = sum >> 64;
        }
        (Self(out), carry != 0)
    }

    fn sub_with_borrow(self, other: Self) -> (Self, bool) {
        let mut out = [0_u64; 4];
        let mut borrow = 0_u128;
        for (i, slot) in out.iter_mut().enumerate() {
            let a = u128::from(self.0[i]);
            let b = u128::from(other.0[i]) + borrow;
            if a >= b {
                *slot = (a - b) as u64;
                borrow = 0;
            } else {
                *slot = ((1_u128 << 64) + a - b) as u64;
                borrow = 1;
            }
        }
        (Self(out), borrow != 0)
    }

    fn add_mod(self, other: Self, modulus: Self) -> Self {
        let (sum, carry) = self.add_with_carry(other);
        if carry || sum.ge(modulus) {
            sum.sub_with_borrow(modulus).0
        } else {
            sum
        }
    }

    fn sub_mod(self, other: Self, modulus: Self) -> Self {
        let (diff, borrow) = self.sub_with_borrow(other);
        if borrow {
            diff.add_with_carry(modulus).0
        } else {
            diff
        }
    }

    fn mul_mod(self, other: Self, modulus: Self) -> Self {
        let mut prod = [0_u64; 8];
        for i in 0..4 {
            let mut carry = 0_u128;
            for j in 0..4 {
                let term = u128::from(self.0[i]) * u128::from(other.0[j])
                    + u128::from(prod[i + j])
                    + carry;
                prod[i + j] = term as u64;
                carry = term >> 64;
            }
            prod[i + 4] = carry as u64;
        }
        // Reduce 512-bit product modulo 256-bit modulus via 8-bit window shift-add
        let mut rem = Self::ZERO;
        for byte_idx in (0..64).rev() {
            let limb = byte_idx / 8;
            let shift = (byte_idx % 8) * 8;
            let byte = (prod[limb] >> shift) & 0xff;
            for _ in 0..8 {
                rem = rem.add_mod(rem, modulus);
            }
            if byte != 0 {
                rem = rem.add_mod(Self([byte, 0, 0, 0]), modulus);
            }
        }
        rem
    }

    fn pow_mod(self, exp: Self, modulus: Self) -> Self {
        let mut result = Self::ONE;
        let mut base = self;
        for i in 0..256 {
            if exp.bit(i) {
                result = result.mul_mod(base, modulus);
            }
            base = base.mul_mod(base, modulus);
        }
        result
    }

    fn inv_mod_p(self) -> Self {
        self.pow_mod(P256_P_MINUS_2, P256_P)
    }

    fn inv_mod_n(self) -> Self {
        self.pow_mod(P256_N_MINUS_2, P256_N)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct JacobianPoint {
    x: U256,
    y: U256,
    z: U256,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AffinePoint {
    x: U256,
    y: U256,
    infinity: bool,
}

impl JacobianPoint {
    const INFINITY: Self = Self {
        x: U256::ONE,
        y: U256::ONE,
        z: U256::ZERO,
    };

    fn from_affine(p: AffinePoint) -> Self {
        if p.infinity {
            Self::INFINITY
        } else {
            Self {
                x: p.x,
                y: p.y,
                z: U256::ONE,
            }
        }
    }

    fn to_affine(self) -> AffinePoint {
        if self.z.is_zero() {
            return AffinePoint::INFINITY;
        }
        let z_inv = self.z.inv_mod_p();
        let z_inv2 = z_inv.mul_mod(z_inv, P256_P);
        let z_inv3 = z_inv2.mul_mod(z_inv, P256_P);
        AffinePoint {
            x: self.x.mul_mod(z_inv2, P256_P),
            y: self.y.mul_mod(z_inv3, P256_P),
            infinity: false,
        }
    }

    fn double(self) -> Self {
        if self.z.is_zero() || self.y.is_zero() {
            return Self::INFINITY;
        }
        let z2 = self.z.mul_mod(self.z, P256_P);
        let x_minus_z2 = self.x.sub_mod(z2, P256_P);
        let x_plus_z2 = self.x.add_mod(z2, P256_P);
        let m_base = x_minus_z2.mul_mod(x_plus_z2, P256_P);
        let m = m_base.add_mod(m_base, P256_P).add_mod(m_base, P256_P);

        let y2 = self.y.mul_mod(self.y, P256_P);
        let xy2 = self.x.mul_mod(y2, P256_P);
        let s = xy2.add_mod(xy2, P256_P);
        let s = s.add_mod(s, P256_P); // 4 * X * Y^2

        let x3 = m.mul_mod(m, P256_P).sub_mod(s.add_mod(s, P256_P), P256_P);

        let y4 = y2.mul_mod(y2, P256_P);
        let two_y4 = y4.add_mod(y4, P256_P);
        let four_y4 = two_y4.add_mod(two_y4, P256_P);
        let eight_y4 = four_y4.add_mod(four_y4, P256_P);

        let y3 = m
            .mul_mod(s.sub_mod(x3, P256_P), P256_P)
            .sub_mod(eight_y4, P256_P);
        let yz = self.y.mul_mod(self.z, P256_P);
        let z3 = yz.add_mod(yz, P256_P);

        Self {
            x: x3,
            y: y3,
            z: z3,
        }
    }

    fn add(self, other: Self) -> Self {
        if self.z.is_zero() {
            return other;
        }
        if other.z.is_zero() {
            return self;
        }
        let z1z1 = self.z.mul_mod(self.z, P256_P);
        let z2z2 = other.z.mul_mod(other.z, P256_P);
        let u1 = self.x.mul_mod(z2z2, P256_P);
        let u2 = other.x.mul_mod(z1z1, P256_P);
        let s1 = self.y.mul_mod(other.z, P256_P).mul_mod(z2z2, P256_P);
        let s2 = other.y.mul_mod(self.z, P256_P).mul_mod(z1z1, P256_P);

        if u1 == u2 {
            if s1 == s2 {
                return self.double();
            }
            return Self::INFINITY;
        }
        let h = u2.sub_mod(u1, P256_P);
        let r = s2.sub_mod(s1, P256_P);
        let h2 = h.mul_mod(h, P256_P);
        let h3 = h.mul_mod(h2, P256_P);
        let u1h2 = u1.mul_mod(h2, P256_P);

        let x3 = r
            .mul_mod(r, P256_P)
            .sub_mod(h3, P256_P)
            .sub_mod(u1h2.add_mod(u1h2, P256_P), P256_P);
        let y3 = r
            .mul_mod(u1h2.sub_mod(x3, P256_P), P256_P)
            .sub_mod(s1.mul_mod(h3, P256_P), P256_P);
        let z3 = h.mul_mod(self.z, P256_P).mul_mod(other.z, P256_P);
        Self {
            x: x3,
            y: y3,
            z: z3,
        }
    }
}

impl AffinePoint {
    const INFINITY: Self = Self {
        x: U256::ZERO,
        y: U256::ZERO,
        infinity: true,
    };

    const GENERATOR: Self = Self {
        x: P256_GX,
        y: P256_GY,
        infinity: false,
    };

    fn scalar_mul(self, scalar: U256) -> Self {
        let mut acc = JacobianPoint::INFINITY;
        let mut base = JacobianPoint::from_affine(self);
        for i in 0..256 {
            if scalar.bit(i) {
                acc = acc.add(base);
            }
            base = base.double();
        }
        acc.to_affine()
    }
}

#[derive(Clone, Copy)]
#[allow(non_camel_case_types)]
struct p256_PublicKey {
    uncompressed: [u8; 65],
}

impl p256_PublicKey {
    fn from_scalar(scalar: &U256) -> Self {
        let point = AffinePoint::GENERATOR.scalar_mul(*scalar);
        let mut uncompressed = [0_u8; 65];
        uncompressed[0] = 0x04;
        uncompressed[1..33].copy_from_slice(&point.x.to_be_bytes());
        uncompressed[33..65].copy_from_slice(&point.y.to_be_bytes());
        Self { uncompressed }
    }
}

fn derive_nonzero_scalar(seed: &[u8; 32], domain: &[u8]) -> U256 {
    for counter in 0_u32..1_024 {
        let mut input = Vec::with_capacity(32 + domain.len() + 4);
        input.extend_from_slice(seed);
        input.extend_from_slice(domain);
        input.extend_from_slice(&counter.to_be_bytes());
        let digest = sha256(&input);
        let mut candidate = U256::from_be_bytes(&digest);
        if candidate.ge(P256_N) {
            candidate = candidate.sub_with_borrow(P256_N).0;
        }
        if !candidate.is_zero() {
            return candidate;
        }
    }
    U256::ONE
}

fn derive_serial(seed: &[u8; 32], domain: &[u8]) -> [u8; 16] {
    let mut input = Vec::with_capacity(32 + domain.len());
    input.extend_from_slice(seed);
    input.extend_from_slice(domain);
    let digest = sha256(&input);
    let mut serial = [0_u8; 16];
    serial.copy_from_slice(&digest[0..16]);
    serial[0] &= 0x7f;
    if serial[0] == 0 {
        serial[0] = 0x01;
    }
    serial
}

fn key_identifier(public_key: &p256_PublicKey) -> [u8; 20] {
    let digest = sha256(&public_key.uncompressed);
    let mut id = [0_u8; 20];
    id.copy_from_slice(&digest[0..20]);
    id
}

fn ecdsa_p256_sign_deterministic(private_scalar: &U256, message: &[u8], domain: &[u8]) -> Vec<u8> {
    let digest = sha256(message);
    let mut z = U256::from_be_bytes(&digest);
    if z.ge(P256_N) {
        z = z.sub_with_borrow(P256_N).0;
    }
    let priv_bytes = private_scalar.to_be_bytes();
    for counter in 0_u32..1_024 {
        let mut nonce_input = Vec::with_capacity(64 + domain.len() + 4);
        nonce_input.extend_from_slice(&priv_bytes);
        nonce_input.extend_from_slice(&digest);
        nonce_input.extend_from_slice(domain);
        nonce_input.extend_from_slice(&counter.to_be_bytes());
        let nonce_hash = sha256(&nonce_input);
        let mut k = U256::from_be_bytes(&nonce_hash);
        if k.ge(P256_N) {
            k = k.sub_with_borrow(P256_N).0;
        }
        if k.is_zero() {
            continue;
        }
        let point = AffinePoint::GENERATOR.scalar_mul(k);
        if point.infinity {
            continue;
        }
        let mut r = point.x;
        if r.ge(P256_N) {
            r = r.sub_with_borrow(P256_N).0;
        }
        if r.is_zero() {
            continue;
        }
        let k_inv = k.inv_mod_n();
        let rd = r.mul_mod(*private_scalar, P256_N);
        let z_plus_rd = z.add_mod(rd, P256_N);
        let s = k_inv.mul_mod(z_plus_rd, P256_N);
        if s.is_zero() {
            continue;
        }
        let mut seq = Vec::new();
        seq.extend_from_slice(&der_integer(&r.to_be_bytes()));
        seq.extend_from_slice(&der_integer(&s.to_be_bytes()));
        return der_tlv(0x30, &seq);
    }
    unreachable!("deterministic nonce loop converges")
}

// ============================================================================
// ASN.1 DER & PEM Encoders
// ============================================================================

// OIDs
const OID_EC_PUBLIC_KEY: &[u8] = &[0x2a, 0x86, 0x48, 0xce, 0x3d, 0x02, 0x01]; // 1.2.840.10045.2.1
const OID_PRIME256V1: &[u8] = &[0x2a, 0x86, 0x48, 0xce, 0x3d, 0x03, 0x01, 0x07]; // 1.2.840.10045.3.1.7
const OID_ECDSA_WITH_SHA256: &[u8] = &[0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04, 0x03, 0x02]; // 1.2.840.10045.4.3.2
const OID_COMMON_NAME: &[u8] = &[0x55, 0x04, 0x03]; // 2.5.4.3
const OID_SUBJECT_KEY_ID: &[u8] = &[0x55, 0x1d, 0x0e]; // 2.5.29.14
const OID_KEY_USAGE: &[u8] = &[0x55, 0x1d, 0x0f]; // 2.5.29.15
const OID_SUBJECT_ALT_NAME: &[u8] = &[0x55, 0x1d, 0x11]; // 2.5.29.17
const OID_BASIC_CONSTRAINTS: &[u8] = &[0x55, 0x1d, 0x13]; // 2.5.29.19
const OID_AUTHORITY_KEY_ID: &[u8] = &[0x55, 0x1d, 0x23]; // 2.5.29.35
const OID_EXT_KEY_USAGE: &[u8] = &[0x55, 0x1d, 0x25]; // 2.5.29.37
const OID_KP_SERVER_AUTH: &[u8] = &[0x2b, 0x06, 0x01, 0x05, 0x05, 0x07, 0x03, 0x01]; // 1.3.6.1.5.5.7.3.1
const OID_KP_CLIENT_AUTH: &[u8] = &[0x2b, 0x06, 0x01, 0x05, 0x05, 0x07, 0x03, 0x02]; // 1.3.6.1.5.5.7.3.2

fn der_tlv(tag: u8, content: &[u8]) -> Vec<u8> {
    let len = content.len();
    let mut out = Vec::with_capacity(4 + len);
    out.push(tag);
    if len < 128 {
        out.push(len as u8);
    } else if len <= 0xff {
        out.push(0x81);
        out.push(len as u8);
    } else {
        out.push(0x82);
        out.push((len >> 8) as u8);
        out.push((len & 0xff) as u8);
    }
    out.extend_from_slice(content);
    out
}

fn der_integer(unsigned_be: &[u8]) -> Vec<u8> {
    let mut start = 0;
    while start + 1 < unsigned_be.len() && unsigned_be[start] == 0 {
        start += 1;
    }
    let trimmed = &unsigned_be[start..];
    let mut body = Vec::with_capacity(trimmed.len() + 1);
    if trimmed[0] & 0x80 != 0 {
        body.push(0x00);
    }
    body.extend_from_slice(trimmed);
    der_tlv(0x02, &body)
}

fn der_bit_string(bytes: &[u8], unused_bits: u8) -> Vec<u8> {
    let mut body = Vec::with_capacity(1 + bytes.len());
    body.push(unused_bits);
    body.extend_from_slice(bytes);
    der_tlv(0x03, &body)
}

fn encode_pkcs8_p256_private_key(scalar: &U256, public_key: &p256_PublicKey) -> Vec<u8> {
    // Inner SEC 1 ECPrivateKey (version 1, 32-byte privateKey, [1] EXPLICIT BIT STRING publicKey)
    let mut ec_priv = Vec::new();
    ec_priv.extend_from_slice(&der_tlv(0x02, &[0x01])); // version = 1
    ec_priv.extend_from_slice(&der_tlv(0x04, &scalar.to_be_bytes())); // privateKey OCTET STRING
    let pub_bits = der_bit_string(&public_key.uncompressed, 0);
    ec_priv.extend_from_slice(&der_tlv(0xa1, &pub_bits)); // [1] EXPLICIT publicKey
    let ec_priv_seq = der_tlv(0x30, &ec_priv);

    let mut alg_id = Vec::new();
    alg_id.extend_from_slice(&der_tlv(0x06, OID_EC_PUBLIC_KEY));
    alg_id.extend_from_slice(&der_tlv(0x06, OID_PRIME256V1));

    let mut pkcs8 = Vec::new();
    pkcs8.extend_from_slice(&der_tlv(0x02, &[0x00])); // version = 0 (v1)
    pkcs8.extend_from_slice(&der_tlv(0x30, &alg_id));
    pkcs8.extend_from_slice(&der_tlv(0x04, &ec_priv_seq));
    der_tlv(0x30, &pkcs8)
}

fn encode_name_cn(cn: &str) -> Vec<u8> {
    let mut atv = Vec::new();
    atv.extend_from_slice(&der_tlv(0x06, OID_COMMON_NAME));
    atv.extend_from_slice(&der_tlv(0x0c, cn.as_bytes())); // UTF8String
    let rdn = der_tlv(0x31, &der_tlv(0x30, &atv));
    der_tlv(0x30, &rdn)
}

fn encode_asn1_time(unix_seconds: u64) -> Vec<u8> {
    let (year, month, day, hour, minute, second) = unix_to_ymdhms(unix_seconds);
    if (1950..=2049).contains(&year) {
        let mut text = String::with_capacity(13);
        let _ = write!(
            &mut text,
            "{:02}{:02}{:02}{:02}{:02}{:02}Z",
            year % 100,
            month,
            day,
            hour,
            minute,
            second
        );
        der_tlv(0x17, text.as_bytes()) // UTCTime
    } else {
        let mut text = String::with_capacity(15);
        let _ = write!(
            &mut text,
            "{:04}{:02}{:02}{:02}{:02}{:02}Z",
            year, month, day, hour, minute, second
        );
        der_tlv(0x18, text.as_bytes()) // GeneralizedTime
    }
}

fn unix_to_ymdhms(unix_seconds: u64) -> (u32, u32, u32, u32, u32, u32) {
    let second = (unix_seconds % 60) as u32;
    let minute = ((unix_seconds / 60) % 60) as u32;
    let hour = ((unix_seconds / 3_600) % 24) as u32;
    // Civil date from days since 1970-01-01 (Howard Hinnant algorithm)
    let z = (unix_seconds / 86_400) as i64 + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u32;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = (yoe as i64) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = (y + i64::from(m <= 2)) as u32;
    (year, m, d, hour, minute, second)
}

fn encode_extension(oid: &[u8], critical: bool, value_der: &[u8]) -> Vec<u8> {
    let mut ext = Vec::new();
    ext.extend_from_slice(&der_tlv(0x06, oid));
    if critical {
        ext.extend_from_slice(&der_tlv(0x01, &[0xff]));
    }
    ext.extend_from_slice(&der_tlv(0x04, value_der));
    der_tlv(0x30, &ext)
}

fn encode_ca_extensions(ski: &[u8; 20]) -> Vec<u8> {
    let mut exts = Vec::new();
    // BasicConstraints: critical, cA = TRUE
    exts.extend_from_slice(&encode_extension(
        OID_BASIC_CONSTRAINTS,
        true,
        &der_tlv(0x30, &der_tlv(0x01, &[0xff])),
    ));
    // KeyUsage: critical, keyCertSign (bit 5) | cRLSign (bit 6) -> 0x06 with 1 unused bit
    exts.extend_from_slice(&encode_extension(
        OID_KEY_USAGE,
        true,
        &der_bit_string(&[0x06], 1),
    ));
    // SubjectKeyIdentifier
    exts.extend_from_slice(&encode_extension(
        OID_SUBJECT_KEY_ID,
        false,
        &der_tlv(0x04, ski),
    ));
    exts
}

fn encode_server_extensions(
    ski: &[u8; 20],
    aki: &[u8; 20],
    identities: &[TlsServerIdentity],
) -> Vec<u8> {
    let mut exts = Vec::new();
    // BasicConstraints: critical, cA = FALSE (empty SEQUENCE)
    exts.extend_from_slice(&encode_extension(
        OID_BASIC_CONSTRAINTS,
        true,
        &der_tlv(0x30, &[]),
    ));
    // KeyUsage: critical, digitalSignature (bit 0) -> 0x80 with 7 unused bits
    exts.extend_from_slice(&encode_extension(
        OID_KEY_USAGE,
        true,
        &der_bit_string(&[0x80], 7),
    ));
    // ExtendedKeyUsage: serverAuth
    exts.extend_from_slice(&encode_extension(
        OID_EXT_KEY_USAGE,
        false,
        &der_tlv(0x30, &der_tlv(0x06, OID_KP_SERVER_AUTH)),
    ));
    // SubjectAltName (GeneralNames ::= SEQUENCE SIZE (1..MAX) OF GeneralName)
    let mut san_entries = Vec::new();
    for identity in identities {
        let entry = match identity {
            TlsServerIdentity::Dns(name) => der_tlv(0x82, name.as_bytes()),
            TlsServerIdentity::Ip(IpAddr::V4(v4)) => der_tlv(0x87, &v4.octets()),
            TlsServerIdentity::Ip(IpAddr::V6(v6)) => der_tlv(0x87, &v6.octets()),
        };
        san_entries.extend_from_slice(&entry);
    }
    exts.extend_from_slice(&encode_extension(
        OID_SUBJECT_ALT_NAME,
        false,
        &der_tlv(0x30, &san_entries),
    ));
    // SubjectKeyIdentifier & AuthorityKeyIdentifier
    exts.extend_from_slice(&encode_extension(
        OID_SUBJECT_KEY_ID,
        false,
        &der_tlv(0x04, ski),
    ));
    exts.extend_from_slice(&encode_extension(
        OID_AUTHORITY_KEY_ID,
        false,
        &der_tlv(0x30, &der_tlv(0x80, aki)),
    ));
    exts
}

fn encode_client_extensions(ski: &[u8; 20], aki: &[u8; 20]) -> Vec<u8> {
    let mut exts = Vec::new();
    exts.extend_from_slice(&encode_extension(
        OID_BASIC_CONSTRAINTS,
        true,
        &der_tlv(0x30, &[]),
    ));
    exts.extend_from_slice(&encode_extension(
        OID_KEY_USAGE,
        true,
        &der_bit_string(&[0x80], 7),
    ));
    exts.extend_from_slice(&encode_extension(
        OID_EXT_KEY_USAGE,
        false,
        &der_tlv(0x30, &der_tlv(0x06, OID_KP_CLIENT_AUTH)),
    ));
    exts.extend_from_slice(&encode_extension(
        OID_SUBJECT_KEY_ID,
        false,
        &der_tlv(0x04, ski),
    ));
    exts.extend_from_slice(&encode_extension(
        OID_AUTHORITY_KEY_ID,
        false,
        &der_tlv(0x30, &der_tlv(0x80, aki)),
    ));
    exts
}

fn encode_tbs_certificate(
    serial: &[u8; 16],
    issuer_cn: &str,
    subject_cn: &str,
    not_before_unix: u64,
    not_after_unix: u64,
    public_key: &p256_PublicKey,
    extensions_body: &[u8],
) -> Vec<u8> {
    let mut tbs = Vec::new();
    // version [0] EXPLICIT INTEGER 2 (v3)
    tbs.extend_from_slice(&der_tlv(0xa0, &der_tlv(0x02, &[0x02])));
    // serialNumber
    tbs.extend_from_slice(&der_integer(serial));
    // signature AlgorithmIdentifier (ecdsa-with-SHA256, no parameters)
    tbs.extend_from_slice(&der_tlv(0x30, &der_tlv(0x06, OID_ECDSA_WITH_SHA256)));
    // issuer
    tbs.extend_from_slice(&encode_name_cn(issuer_cn));
    // validity
    let mut validity = Vec::new();
    validity.extend_from_slice(&encode_asn1_time(not_before_unix));
    validity.extend_from_slice(&encode_asn1_time(not_after_unix));
    tbs.extend_from_slice(&der_tlv(0x30, &validity));
    // subject
    tbs.extend_from_slice(&encode_name_cn(subject_cn));
    // subjectPublicKeyInfo
    let mut spki_alg = Vec::new();
    spki_alg.extend_from_slice(&der_tlv(0x06, OID_EC_PUBLIC_KEY));
    spki_alg.extend_from_slice(&der_tlv(0x06, OID_PRIME256V1));
    let mut spki = Vec::new();
    spki.extend_from_slice(&der_tlv(0x30, &spki_alg));
    spki.extend_from_slice(&der_bit_string(&public_key.uncompressed, 0));
    tbs.extend_from_slice(&der_tlv(0x30, &spki));
    // extensions [3] EXPLICIT SEQUENCE OF Extension
    tbs.extend_from_slice(&der_tlv(0xa3, &der_tlv(0x30, extensions_body)));
    der_tlv(0x30, &tbs)
}

fn encode_signed_certificate(tbs_der: &[u8], signature_der: &[u8]) -> Vec<u8> {
    let mut cert = Vec::new();
    cert.extend_from_slice(tbs_der);
    cert.extend_from_slice(&der_tlv(0x30, &der_tlv(0x06, OID_ECDSA_WITH_SHA256)));
    cert.extend_from_slice(&der_bit_string(signature_der, 0));
    der_tlv(0x30, &cert)
}

const BASE64_ALPHABET: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn encode_pem(label: &str, der: &[u8]) -> String {
    let mut b64 = String::with_capacity(der.len().div_ceil(3) * 4);
    for chunk in der.chunks(3) {
        let b0 = chunk[0];
        let b1 = if chunk.len() > 1 { chunk[1] } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] } else { 0 };
        b64.push(BASE64_ALPHABET[(b0 >> 2) as usize] as char);
        b64.push(BASE64_ALPHABET[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize] as char);
        if chunk.len() > 1 {
            b64.push(BASE64_ALPHABET[(((b1 & 0x0f) << 2) | (b2 >> 6)) as usize] as char);
        } else {
            b64.push('=');
        }
        if chunk.len() > 2 {
            b64.push(BASE64_ALPHABET[(b2 & 0x3f) as usize] as char);
        } else {
            b64.push('=');
        }
    }
    let mut pem = String::new();
    let _ = writeln!(&mut pem, "-----BEGIN {label}-----");
    for line in b64.as_bytes().chunks(64) {
        if let Ok(text) = std::str::from_utf8(line) {
            let _ = writeln!(&mut pem, "{text}");
        }
    }
    let _ = writeln!(&mut pem, "-----END {label}-----");
    pem
}

#[cfg(test)]
mod tests {
    use super::*;

    // req: NET-001, NET-003
    #[test]
    fn deterministic_x509_bundle_mints_valid_ca_server_and_client_identities() {
        let endpoint = CanonicalHttpsEndpoint::parse("https://127.0.0.1:7443").expect("endpoint");
        let bundle = issue_tls_bundle_from_seed([0x42; 32], &endpoint, 1_767_225_600, 365, true)
            .expect("issue bundle");
        assert!(
            bundle
                .ca()
                .certificate_pem()
                .starts_with("-----BEGIN CERTIFICATE-----\n")
        );
        assert!(
            bundle
                .ca()
                .private_key_pem()
                .starts_with("-----BEGIN PRIVATE KEY-----\n")
        );
        assert!(
            bundle
                .server()
                .certificate_pem()
                .starts_with("-----BEGIN CERTIFICATE-----\n")
        );
        assert!(bundle.client().is_some());
    }
}
