use std::io::{Cursor, Read, Seek};

use anyhow::{ensure, Context, Result};
use bytes::Bytes;
use quick_xml::{
    events::{BytesStart, Event},
    reader::Reader as XmlReader,
    XmlVersion,
};
use tracing::debug;
use zip::{result::ZipError, ZipArchive};

use super::{
    image_content_type, path_content_type, read_limited, ThumbnailImage, MAX_THUMBNAIL_SIZE,
};

const ROOT_RELS_PATH: &str = "_rels/.rels";
const OPC_THUMBNAIL_REL: &str =
    "http://schemas.openxmlformats.org/package/2006/relationships/metadata/thumbnail";
const BAMBU_COVER_MIDDLE_REL: &str =
    "http://schemas.bambulab.com/package/2021/cover-thumbnail-middle";
const BAMBU_COVER_SMALL_REL: &str =
    "http://schemas.bambulab.com/package/2021/cover-thumbnail-small";
const THUMBNAIL_REL_PRIORITY: &[&str] = &[
    OPC_THUMBNAIL_REL,
    BAMBU_COVER_MIDDLE_REL,
    BAMBU_COVER_SMALL_REL,
];
const FALLBACK_THUMBNAIL_NAMES: &[&str] = &[
    "Metadata/thumbnail.png",
    "Metadata/thumbnail.jpg",
    "Metadata/thumbnail.jpeg",
    "Metadata/thumbnail_small.png",
    "Metadata/plate_1.png",
    "Metadata/plate_1_small.png",
    "Metadata/top_1.png",
    "Metadata/pick_1.png",
];

pub(super) fn extract_bambu_3mf_thumbnail_archive(archive: Vec<u8>) -> Result<ThumbnailImage> {
    let mut archive =
        ZipArchive::new(Cursor::new(archive)).context("failed to read local 3MF as ZIP archive")?;
    let thumbnail = select_thumbnail_entry(&mut archive)?
        .context("3MF did not include a supported thumbnail image")?;
    read_thumbnail_entry(&mut archive, &thumbnail)
}

fn select_thumbnail_entry<R: Read + Seek>(archive: &mut ZipArchive<R>) -> Result<Option<String>> {
    // 3MF stores the authoritative package thumbnail in the root relationship
    // file. Only fall back to file-name heuristics when that relationship is
    // absent, and keep those fallbacks explicitly ordered.
    if let Some(relationships) = read_archive_string(archive, ROOT_RELS_PATH)? {
        let relationships = parse_thumbnail_relationships(&relationships)?;
        for rel_type in THUMBNAIL_REL_PRIORITY {
            for relationship in relationships
                .iter()
                .filter(|relationship| relationship.rel_type == *rel_type)
            {
                let Some(target) = normalize_archive_path(&relationship.target) else {
                    continue;
                };
                if is_supported_thumbnail_entry(&target)
                    && archive.index_for_name(&target).is_some()
                {
                    return Ok(Some(target));
                }
            }
        }
    }

    for name in FALLBACK_THUMBNAIL_NAMES {
        if archive.index_for_name(name).is_some() {
            return Ok(Some((*name).to_owned()));
        }
    }

    let mut names = archive
        .file_names()
        .filter(|name| is_supported_thumbnail_entry(name))
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    names.sort_unstable();
    Ok(names.into_iter().next())
}

fn read_thumbnail_entry<R: Read + Seek>(
    archive: &mut ZipArchive<R>,
    name: &str,
) -> Result<ThumbnailImage> {
    let mut file = archive
        .by_name(name)
        .with_context(|| format!("failed to open thumbnail entry `{name}`"))?;
    ensure!(
        file.size() <= MAX_THUMBNAIL_SIZE as u64,
        "thumbnail entry `{name}` exceeds maximum supported size of {MAX_THUMBNAIL_SIZE} bytes"
    );
    let bytes = read_limited(&mut file, MAX_THUMBNAIL_SIZE, "thumbnail entry data")
        .with_context(|| format!("failed to read thumbnail entry `{name}`"))?;
    ensure!(!bytes.is_empty(), "thumbnail entry `{name}` is empty");
    debug!(
        entry = %name,
        size = bytes.len(),
        "loaded thumbnail from local 3MF"
    );
    Ok(ThumbnailImage {
        content_type: image_content_type(path_content_type(name), &bytes),
        bytes: Bytes::from(bytes),
    })
}

fn read_archive_string<R: Read + Seek>(
    archive: &mut ZipArchive<R>,
    name: &str,
) -> Result<Option<String>> {
    let mut file = match archive.by_name(name) {
        Ok(file) => file,
        Err(ZipError::FileNotFound) => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| format!("failed to open archive entry `{name}`"))
        }
    };
    let mut text = String::new();
    file.read_to_string(&mut text)
        .with_context(|| format!("failed to read archive entry `{name}`"))?;
    Ok(Some(text))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ThumbnailRelationship {
    rel_type: String,
    target: String,
}

fn parse_thumbnail_relationships(xml: &str) -> Result<Vec<ThumbnailRelationship>> {
    let mut reader = XmlReader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut relationships = Vec::new();
    loop {
        match reader
            .read_event()
            .context("failed to parse 3MF relationships")?
        {
            Event::Empty(element) | Event::Start(element) => {
                if element.local_name().as_ref() == b"Relationship" {
                    if let Some(relationship) = parse_thumbnail_relationship(&reader, &element)? {
                        relationships.push(relationship);
                    }
                }
            }
            Event::Eof => break,
            _ => {}
        }
    }
    Ok(relationships)
}

fn parse_thumbnail_relationship(
    reader: &XmlReader<&[u8]>,
    element: &BytesStart<'_>,
) -> Result<Option<ThumbnailRelationship>> {
    let mut rel_type = None;
    let mut target = None;
    for attribute in element.attributes().with_checks(false) {
        let attribute = attribute.context("failed to parse 3MF relationship attribute")?;
        let value = attribute
            .decoded_and_normalized_value(XmlVersion::Implicit1_0, reader.decoder())
            .context("failed to decode 3MF relationship attribute")?
            .into_owned();
        match attribute.key.as_ref() {
            b"Type" => rel_type = Some(value),
            b"Target" => target = Some(value),
            _ => {}
        }
    }
    Ok(match (rel_type, target) {
        (Some(rel_type), Some(target)) => Some(ThumbnailRelationship { rel_type, target }),
        _ => None,
    })
}

fn normalize_archive_path(path: &str) -> Option<String> {
    let path = path.trim().replace('\\', "/");
    let path = path.trim_start_matches('/');
    if path.is_empty() || path.contains('\0') || path.split('/').any(|part| part == "..") {
        return None;
    }
    Some(path.to_owned())
}

fn is_supported_thumbnail_entry(name: &str) -> bool {
    let normalized = name.replace('\\', "/").to_ascii_lowercase();
    match normalized.as_str() {
        "metadata/thumbnail.png"
        | "metadata/thumbnail.jpg"
        | "metadata/thumbnail.jpeg"
        | "metadata/thumbnail_small.png"
        | "metadata/plate_1.png"
        | "metadata/top_1.png" => true,
        _ if normalized.starts_with("metadata/")
            && (normalized.ends_with(".png")
                || normalized.ends_with(".jpg")
                || normalized.ends_with(".jpeg")) =>
        {
            true
        }
        _ if normalized.ends_with(".png")
            || normalized.ends_with(".jpg")
            || normalized.ends_with(".jpeg") =>
        {
            true
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Write};

    use zip::{write::SimpleFileOptions, CompressionMethod, ZipWriter};

    use super::{
        extract_bambu_3mf_thumbnail_archive, is_supported_thumbnail_entry, BAMBU_COVER_MIDDLE_REL,
        OPC_THUMBNAIL_REL,
    };

    #[test]
    fn archive_thumbnail_uses_root_thumbnail_relationship() {
        let thumbnail = b"\x89PNG\r\n\x1a\nthumbnail";
        let relationships = relationship_xml(&[(OPC_THUMBNAIL_REL, "/Metadata/plate_1.png")]);
        let archive = make_archive(&[
            ("_rels/.rels", relationships.as_bytes()),
            ("Metadata/pick_1.png", b"wrong"),
            ("Metadata/plate_1.png", thumbnail),
        ]);

        let image = extract_bambu_3mf_thumbnail_archive(archive).unwrap();

        assert_eq!(image.content_type, "image/png");
        assert_eq!(image.bytes.as_ref(), thumbnail);
    }

    #[test]
    fn supported_thumbnail_entry_recognizes_bambu_thumbnail_names() {
        assert!(is_supported_thumbnail_entry("Metadata/thumbnail.png"));
        assert!(is_supported_thumbnail_entry("Metadata/plate_1.png"));
        assert!(is_supported_thumbnail_entry("foo/model.png"));
        assert!(!is_supported_thumbnail_entry("Metadata/model.xml"));
    }

    #[test]
    fn archive_thumbnail_uses_bambu_middle_relationship() {
        let thumbnail = b"\x89PNG\r\n\x1a\nthumbnail";
        let relationships = relationship_xml(&[(BAMBU_COVER_MIDDLE_REL, "/Metadata/plate_2.png")]);
        let archive = make_archive(&[
            ("_rels/.rels", relationships.as_bytes()),
            ("Metadata/plate_1.png", b"wrong"),
            ("Metadata/plate_2.png", thumbnail),
        ]);

        let image = extract_bambu_3mf_thumbnail_archive(archive).unwrap();

        assert_eq!(image.content_type, "image/png");
        assert_eq!(image.bytes.as_ref(), thumbnail);
    }

    #[test]
    fn archive_thumbnail_falls_back_by_explicit_priority() {
        let thumbnail = b"\x89PNG\r\n\x1a\nthumbnail";
        let archive = make_archive(&[
            ("Metadata/top_1.png", b"wrong"),
            ("Metadata/plate_1.png", thumbnail),
        ]);

        let image = extract_bambu_3mf_thumbnail_archive(archive).unwrap();

        assert_eq!(image.content_type, "image/png");
        assert_eq!(image.bytes.as_ref(), thumbnail.as_slice());
    }

    #[test]
    fn archive_thumbnail_falls_back_to_sorted_supported_entries() {
        let archive = make_archive(&[("z/cover.png", b"wrong"), ("a/cover.png", b"right")]);

        let image = extract_bambu_3mf_thumbnail_archive(archive).unwrap();

        assert_eq!(image.bytes.as_ref(), b"right");
    }

    fn relationship_xml(relationships: &[(&str, &str)]) -> String {
        let mut xml = String::from(
            r#"<?xml version="1.0" encoding="UTF-8"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">"#,
        );
        for (rel_type, target) in relationships {
            xml.push_str(&format!(
                r#"<Relationship Target="{target}" Id="rel" Type="{rel_type}"/>"#
            ));
        }
        xml.push_str("</Relationships>");
        xml
    }

    fn make_archive(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let cursor = Cursor::new(Vec::new());
        let mut writer = ZipWriter::new(cursor);
        let options = SimpleFileOptions::default().compression_method(CompressionMethod::Stored);
        for (name, data) in entries {
            writer.start_file(*name, options).unwrap();
            writer.write_all(data).unwrap();
        }
        writer.finish().unwrap().into_inner()
    }
}
