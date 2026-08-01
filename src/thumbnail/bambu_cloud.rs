use anyhow::{Context, Result};

use crate::{
    bambu::cloud::CloudSession,
    bambu::{PrinterStatus, Task},
};

use super::{image_content_type, trimmed, ThumbnailImage, MAX_THUMBNAIL_SIZE};

const CLOUD_TASK_LIMIT: usize = 10;

pub(super) async fn fetch_thumbnail(
    cloud: Option<&CloudSession>,
    device_id: &str,
    report: &PrinterStatus,
) -> Result<ThumbnailImage> {
    let cloud = cloud.context("cloud thumbnail lookup requires a Bambu Cloud token")?;
    let tasks = cloud
        .client
        .tasks(
            cloud.access_token.expose(),
            CLOUD_TASK_LIMIT,
            Some(device_id),
        )
        .await
        .with_context(|| format!("failed to load Bambu Cloud tasks for device `{device_id}`"))?;
    let task = select_cloud_task(&tasks.hits, report)
        .with_context(|| format!("no matching Bambu Cloud task found for device `{device_id}`"))?;
    let cover = task
        .cover
        .as_deref()
        .map(str::trim)
        .filter(|cover| !cover.is_empty())
        .context("matching Bambu Cloud task does not include a thumbnail URL")?;
    let downloaded = cloud
        .client
        .download_bytes(cover, MAX_THUMBNAIL_SIZE)
        .await
        .context("failed to download Bambu Cloud thumbnail")?;

    Ok(ThumbnailImage {
        content_type: image_content_type(
            downloaded.content_type.as_deref(),
            downloaded.bytes.as_ref(),
        ),
        bytes: downloaded.bytes,
    })
}

fn select_cloud_task<'a>(tasks: &'a [Task], report: &PrinterStatus) -> Option<&'a Task> {
    let task_id = trimmed(report.task_id.as_deref());
    if let Some(task_id) = task_id {
        if let Some(task) = tasks
            .iter()
            .find(|task| trimmed(task.id.as_deref()) == Some(task_id))
        {
            return Some(task);
        }
    }

    let task_name = trimmed(report.task_name.as_deref());
    if let Some(task_name) = task_name {
        if let Some(task) = tasks.iter().find(|task| {
            task.display_title()
                .as_deref()
                .map(str::trim)
                .filter(|title| !title.is_empty())
                .is_some_and(|title| title == task_name)
        }) {
            return Some(task);
        }
    }

    let start_time = trimmed(report.start_time.as_deref());
    if let Some(start_time) = start_time {
        if let Some(task) = tasks
            .iter()
            .find(|task| trimmed(task.start_time.as_deref()) == Some(start_time))
        {
            return Some(task);
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use crate::bambu::{PrinterStatus, Task};

    use super::select_cloud_task;

    #[test]
    fn cloud_task_selection_prefers_task_id_then_title() {
        let tasks = vec![
            Task {
                id: Some("old".to_owned()),
                title: Some("Cube".to_owned()),
                ..Task::default()
            },
            Task {
                id: Some("task-1".to_owned()),
                title: Some("Other".to_owned()),
                ..Task::default()
            },
        ];
        let report = PrinterStatus {
            task_id: Some("task-1".to_owned()),
            task_name: Some("Cube".to_owned()),
            ..PrinterStatus::default()
        };

        assert_eq!(
            select_cloud_task(&tasks, &report).unwrap().id.as_deref(),
            Some("task-1")
        );

        let report = PrinterStatus {
            task_name: Some("Cube".to_owned()),
            ..PrinterStatus::default()
        };
        assert_eq!(
            select_cloud_task(&tasks, &report).unwrap().id.as_deref(),
            Some("old")
        );

        let report = PrinterStatus {
            start_time: Some("2026-01-01T10:00:00Z".to_owned()),
            ..PrinterStatus::default()
        };
        let tasks = vec![Task {
            id: Some("start-time-match".to_owned()),
            start_time: Some("2026-01-01T10:00:00Z".to_owned()),
            ..Task::default()
        }];
        assert_eq!(
            select_cloud_task(&tasks, &report).unwrap().id.as_deref(),
            Some("start-time-match")
        );

        let report = PrinterStatus {
            task_id: Some("missing".to_owned()),
            task_name: Some("No match".to_owned()),
            start_time: Some("no-match".to_owned()),
            ..PrinterStatus::default()
        };
        assert!(select_cloud_task(&tasks, &report).is_none());
    }
}
