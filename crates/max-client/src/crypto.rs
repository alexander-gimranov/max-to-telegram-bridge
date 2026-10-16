use aes::Aes128;
use base64::Engine;
use cbc::cipher::{block_padding::Pkcs7, BlockDecryptMut, KeyIvInit};
use pbkdf2::pbkdf2_hmac;
use sha1::Sha1;

type Aes128CbcDec = cbc::Decryptor<Aes128>;

const SALT: &[u8] = b"saltysalt";
const IV: [u8; 16] = [b' '; 16];

pub fn derive_key(password: &[u8]) -> [u8; 16] {
    let mut key = [0u8; 16];
    pbkdf2_hmac::<Sha1>(password, SALT, 1, &mut key);
    key
}

pub fn decrypt_chrome(password: &[u8], data: &[u8]) -> Option<Vec<u8>> {
    let key = derive_key(password);
    decrypt_with_key(&key, data)
}

pub fn decrypt_with_key(key: &[u8], data: &[u8]) -> Option<Vec<u8>> {
    if data.len() < 3 + 16 {
        return None;
    }
    if !(data.starts_with(b"v10") || data.starts_with(b"v11")) {
        return None;
    }
    let ciphertext = &data[3..];
    if ciphertext.len() % 16 != 0 {
        return None;
    }
    let decryptor = Aes128CbcDec::new_from_slices(key, &IV).ok()?;
    let mut buffer = ciphertext.to_vec();
    let plain = decryptor
        .decrypt_padded_mut::<Pkcs7>(&mut buffer)
        .ok()?
        .to_vec();
    Some(plain)
}

pub fn decode_local_state_key(password: &[u8], encrypted_key_b64: &str) -> Option<[u8; 16]> {
    let raw = base64::engine::general_purpose::STANDARD
        .decode(encrypted_key_b64.trim())
        .ok()?;
    let plain = decrypt_chrome(password, &raw)?;
    if plain.len() < 16 {
        return None;
    }
    let mut key = [0u8; 16];
    key.copy_from_slice(&plain[..16]);
    Some(key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aes::cipher::BlockEncryptMut;
    use cbc::cipher::block_padding::Pkcs7 as Pad;

    type Aes128CbcEnc = cbc::Encryptor<Aes128>;

    #[test]
    fn round_trip_peanuts() {
        let password = b"peanuts";
        let key = derive_key(password);
        let encryptor = Aes128CbcEnc::new_from_slices(&key, &IV).unwrap();
        let mut buffer = b"device-secret".to_vec();
        let len = buffer.len();
        buffer.resize(len + 16, 0);
        let written = encryptor
            .encrypt_padded_mut::<Pad>(&mut buffer, len)
            .unwrap()
            .len();
        let mut stored = b"v10".to_vec();
        stored.extend_from_slice(&buffer[..written]);
        let plain = decrypt_chrome(password, &stored).unwrap();
        assert_eq!(plain, b"device-secret");
    }
}
