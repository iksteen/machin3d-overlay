use anyhow::{ensure, Result};

pub(super) const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;

pub(super) fn auth_packet(access_code: &str) -> Result<[u8; 80]> {
    let mut packet = [0_u8; 80];
    packet[0..4].copy_from_slice(&0x40_u32.to_le_bytes());
    packet[4..8].copy_from_slice(&0x3000_u32.to_le_bytes());
    packet[8..12].copy_from_slice(&0_u32.to_le_bytes());
    packet[12..16].copy_from_slice(&0_u32.to_le_bytes());
    write_auth_field(&mut packet[16..48], "bblp", "video username")?;
    write_auth_field(&mut packet[48..80], access_code.trim(), "video access code")?;
    Ok(packet)
}

fn write_auth_field(target: &mut [u8], value: &str, label: &str) -> Result<()> {
    ensure!(value.is_ascii(), "{label} must be ASCII");
    ensure!(
        value.len() <= target.len(),
        "{label} must fit in {} bytes",
        target.len()
    );
    target[..value.len()].copy_from_slice(value.as_bytes());
    Ok(())
}

pub(super) fn is_jpeg(frame: &[u8]) -> bool {
    frame.starts_with(&[0xff, 0xd8]) && frame.ends_with(&[0xff, 0xd9])
}

#[cfg(test)]
mod tests {
    use super::{auth_packet, is_jpeg};

    #[test]
    fn auth_packet_matches_a1_p1_protocol_layout() {
        let packet = auth_packet("12345678").expect("access code should fit");

        assert_eq!(&packet[0..4], &0x40_u32.to_le_bytes());
        assert_eq!(&packet[4..8], &0x3000_u32.to_le_bytes());
        assert_eq!(&packet[8..12], &0_u32.to_le_bytes());
        assert_eq!(&packet[12..16], &0_u32.to_le_bytes());
        assert_eq!(&packet[16..20], b"bblp");
        assert!(packet[20..48].iter().all(|byte| *byte == 0));
        assert_eq!(&packet[48..56], b"12345678");
        assert!(packet[56..80].iter().all(|byte| *byte == 0));
    }

    #[test]
    fn auth_packet_rejects_fields_that_do_not_fit() {
        let error = auth_packet("123456789012345678901234567890123").unwrap_err();
        assert!(error.to_string().contains("video access code"));
    }

    #[test]
    fn jpeg_check_requires_soi_and_eoi_markers() {
        assert!(is_jpeg(&[0xff, 0xd8, 0x00, 0xff, 0xd9]));
        assert!(!is_jpeg(&[0xff, 0xd8, 0x00]));
        assert!(!is_jpeg(&[0x00, 0xff, 0xd9]));
    }
}
