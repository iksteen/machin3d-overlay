use url::form_urlencoded;

pub(super) fn horizontal_overlay(device_id: &str) -> String {
    format!("/overlay?{}", device_query(device_id))
}

pub(super) fn vertical_overlay(device_id: &str) -> String {
    format!("/vertical?{}", device_query(device_id))
}

pub(super) fn thumbnail(device_id: &str) -> String {
    format!("/api/thumbnail?{}", device_query(device_id))
}

pub(super) fn video(device_id: &str) -> String {
    format!("/api/video.mjpeg?{}", device_query(device_id))
}

fn device_query(device_id: &str) -> String {
    form_urlencoded::Serializer::new(String::new())
        .append_pair("device", device_id)
        .finish()
}
