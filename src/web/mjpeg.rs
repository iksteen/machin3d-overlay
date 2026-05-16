use bytes::Bytes;

const MJPEG_BOUNDARY: &str = "frame";

pub(super) fn content_type() -> String {
    format!("multipart/x-mixed-replace; boundary={MJPEG_BOUNDARY}")
}

pub(super) fn part(frame: &[u8]) -> Bytes {
    let header = format!(
        "--{MJPEG_BOUNDARY}\r\nContent-Type: image/jpeg\r\nContent-Length: {}\r\n\r\n",
        frame.len()
    );
    let mut part = Vec::with_capacity(header.len() + frame.len() + 2);
    part.extend_from_slice(header.as_bytes());
    part.extend_from_slice(frame);
    part.extend_from_slice(b"\r\n");
    Bytes::from(part)
}

#[cfg(test)]
mod tests {
    use super::part;

    #[test]
    fn mjpeg_part_contains_boundary_headers_and_frame() {
        let part = part(&[0xff, 0xd8, 0xff, 0xd9]);

        assert!(
            part.starts_with(b"--frame\r\nContent-Type: image/jpeg\r\nContent-Length: 4\r\n\r\n")
        );
        assert!(part.ends_with(&[0xff, 0xd8, 0xff, 0xd9, b'\r', b'\n']));
    }
}
