use url::Url;

pub(super) fn horizontal_overlay(device_id: &str) -> String {
    device_path(device_id, "horizontal")
}

pub(super) fn vertical_overlay(device_id: &str) -> String {
    device_path(device_id, "vertical")
}

pub(super) fn thumbnail(device_id: &str) -> String {
    device_path(device_id, "thumbnail")
}

pub(super) fn video(device_id: &str) -> String {
    device_path(device_id, "video.mjpeg")
}

fn device_path(device_id: &str, endpoint: &str) -> String {
    path_segments(&["devices", device_id, endpoint])
}

fn path_segments(segments: &[&str]) -> String {
    let mut url = Url::parse("http://machin3d-overlay.local").expect("base URL should be valid");
    url.path_segments_mut()
        .expect("base URL should support path segments")
        .extend(segments);
    url.path().to_owned()
}
