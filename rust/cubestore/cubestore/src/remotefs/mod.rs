pub mod gcs;
pub mod minio;
pub mod queue;
pub mod s3;

use crate::config::injection::DIService;
use crate::di_service;
use crate::util::lock::acquire_lock;
use crate::CubeError;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use datafusion::cube_ext;
use futures::future::BoxFuture;
use futures::FutureExt;
use log::debug;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::{NamedTempFile, PathPersistError};
use tokio::fs;
use tokio::sync::{Mutex, RwLock};

#[derive(Debug, Clone)]
pub struct RemoteFile {
    remote_path: String,
    updated: DateTime<Utc>,
    file_size: u64,
}

impl RemoteFile {
    pub fn remote_path(&self) -> &str {
        self.remote_path.as_str()
    }

    pub fn updated(&self) -> &DateTime<Utc> {
        &self.updated
    }
}

#[async_trait]
pub trait RemoteFs: DIService + Send + Sync + Debug {
    /// Use this path to prepare files for upload. Writing into `local_path()` directly can result
    /// in files being deleted by the background cleanup process, see `QueueRemoteFs::cleanup_loop`.
    async fn temp_upload_path(&self, remote_path: &str) -> Result<String, CubeError> {
        // Putting files into a subdirectory prevents cleanups from removing them.
        self.local_file(&format!("uploads/{}", remote_path)).await
    }

    /// Convention is to use this directory for creating files to be uploaded later.
    async fn uploads_dir(&self) -> Result<String, CubeError> {
        // Call to `temp_upload_path` ensures we created the uploads dir.
        let file_in_dir = self
            .temp_upload_path("never_created_remote_fs_file")
            .await?;
        Ok(Path::new(&file_in_dir)
            .parent()
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned())
    }

    /// In addition to uploading this file to the remote filesystem, this function moves the file
    /// from `temp_upload_path` to `self.local_path(remote_path)` on the local file system.
    async fn upload_file(
        &self,
        temp_upload_path: &str,
        remote_path: &str,
    ) -> Result<u64, CubeError>;

    async fn download_file(
        &self,
        remote_path: &str,
        expected_file_size: Option<u64>,
    ) -> Result<String, CubeError>;

    async fn delete_file(&self, remote_path: &str) -> Result<(), CubeError>;

    async fn list(&self, remote_prefix: &str) -> Result<Vec<String>, CubeError>;

    async fn list_with_metadata(&self, remote_prefix: &str) -> Result<Vec<RemoteFile>, CubeError>;

    async fn local_path(&self) -> String;

    async fn local_file(&self, remote_path: &str) -> Result<String, CubeError>;
}

pub fn ensure_temp_file_is_dropped(path: String) {
    if std::fs::metadata(path.clone()).is_ok() {
        if let Err(e) = std::fs::remove_file(path) {
            log::error!("Error during cleaning up temp file: {}", e);
        }
    }
}

#[derive(Debug)]
pub struct LocalDirRemoteFs {
    remote_dir_for_debug: Option<PathBuf>,
    remote_dir: RwLock<Option<PathBuf>>,
    dir: PathBuf,
    dir_delete_mut: Mutex<()>,
}

impl LocalDirRemoteFs {
    pub fn new(remote_dir: Option<PathBuf>, dir: PathBuf) -> Arc<LocalDirRemoteFs> {
        Arc::new(LocalDirRemoteFs {
            remote_dir_for_debug: remote_dir.clone(),
            remote_dir: RwLock::new(remote_dir),
            dir,
            dir_delete_mut: Mutex::new(()),
        })
    }

    pub fn new_noop(dir: PathBuf) -> Arc<LocalDirRemoteFs> {
        Arc::new(LocalDirRemoteFs {
            remote_dir_for_debug: None,
            remote_dir: RwLock::new(None),
            dir,
            dir_delete_mut: Mutex::new(()),
        })
    }

    pub async fn drop_local_path(&self) -> Result<(), CubeError> {
        Ok(fs::remove_dir_all(&*self.dir).await?)
    }
}

di_service!(LocalDirRemoteFs, [RemoteFs]);

#[async_trait]
impl RemoteFs for LocalDirRemoteFs {
    async fn upload_file(
        &self,
        temp_upload_path: &str,
        remote_path: &str,
    ) -> Result<u64, CubeError> {
        if let Some(remote_dir) = self.remote_dir.write().await.as_ref() {
            debug!("Uploading {}", remote_path);
            let dest = remote_dir.as_path().join(remote_path);
            fs::create_dir_all(dest.parent().unwrap())
                .await
                .map_err(|e| {
                    CubeError::internal(format!(
                        "Create dir {}: {}",
                        dest.parent().as_ref().unwrap().to_string_lossy(),
                        e
                    ))
                })?;
            fs::copy(&temp_upload_path, dest.clone())
                .await
                .map_err(|e| {
                    CubeError::internal(format!(
                        "Copy {} -> {}: {}",
                        temp_upload_path,
                        dest.to_string_lossy(),
                        e
                    ))
                })?;
        }
        let local_path = self.dir.as_path().join(remote_path);
        if Path::new(temp_upload_path) != local_path {
            fs::create_dir_all(local_path.parent().unwrap())
                .await
                .map_err(|e| {
                    CubeError::internal(format!(
                        "Create dir {}: {}",
                        local_path.parent().as_ref().unwrap().to_string_lossy(),
                        e
                    ))
                })?;
            fs::rename(&temp_upload_path, local_path.clone())
                .await
                .map_err(|e| {
                    CubeError::internal(format!(
                        "Rename {} -> {}: {}",
                        temp_upload_path,
                        local_path.to_string_lossy(),
                        e
                    ))
                })?;
        }
        Ok(fs::metadata(local_path).await?.len())
    }

    async fn download_file(
        &self,
        remote_path: &str,
        _expected_file_size: Option<u64>,
    ) -> Result<String, CubeError> {
        let mut local_file = self.dir.as_path().join(remote_path);
        let local_dir = local_file.parent().unwrap();
        let downloads_dir = local_dir.join("downloads");
        fs::create_dir_all(&downloads_dir).await?;
        if !local_file.exists() {
            debug!("Downloading {}", remote_path);
            if let Some(remote_dir) = self.remote_dir.write().await.as_ref() {
                let temp_path =
                    cube_ext::spawn_blocking(move || NamedTempFile::new_in(downloads_dir))
                        .await??
                        .into_temp_path();
                fs::copy(remote_dir.as_path().join(remote_path), &temp_path)
                    .await
                    .map_err(|e| {
                        CubeError::internal(format!(
                            "Error during downloading of {}: {}",
                            remote_path, e
                        ))
                    })?;
                local_file =
                    cube_ext::spawn_blocking(move || -> Result<PathBuf, PathPersistError> {
                        temp_path.persist(&local_file)?;
                        Ok(local_file)
                    })
                    .await??;
            } else {
                return Err(CubeError::internal(format!(
                    "File not found: {}",
                    local_file.as_os_str().to_string_lossy()
                )));
            }
        }
        Ok(local_file.into_os_string().into_string().unwrap())
    }

    async fn delete_file(&self, remote_path: &str) -> Result<(), CubeError> {
        debug!("Deleting {}", remote_path);
        {
            if let Some(remote_dir) = self.remote_dir.write().await.as_ref() {
                let remote = remote_dir.as_path().join(remote_path);
                if fs::metadata(remote.clone()).await.is_ok() {
                    fs::remove_file(remote.clone()).await?;
                    Self::remove_empty_paths(remote_dir.clone(), remote.clone()).await?;
                }
            }
        }

        let _local_guard = acquire_lock("delete file", self.dir_delete_mut.lock()).await?;
        let local = self.dir.as_path().join(remote_path);
        if fs::metadata(local.clone()).await.is_ok() {
            fs::remove_file(local.clone()).await?;
            LocalDirRemoteFs::remove_empty_paths(self.dir.as_path().to_path_buf(), local.clone())
                .await?;
        }

        Ok(())
    }

    async fn list(&self, remote_prefix: &str) -> Result<Vec<String>, CubeError> {
        Ok(self
            .list_with_metadata(remote_prefix)
            .await?
            .into_iter()
            .map(|f| f.remote_path)
            .collect::<Vec<_>>())
    }

    async fn list_with_metadata(&self, remote_prefix: &str) -> Result<Vec<RemoteFile>, CubeError> {
        let remote_dir = self.remote_dir.read().await.as_ref().cloned();
        let result = Self::list_recursive(
            remote_dir.clone().unwrap_or(self.dir.clone()),
            remote_prefix.to_string(),
            remote_dir.unwrap_or(self.dir.clone()),
        )
        .await?;
        Ok(result)
    }

    async fn local_path(&self) -> String {
        self.dir.to_str().unwrap().to_owned()
    }

    async fn local_file(&self, remote_path: &str) -> Result<String, CubeError> {
        let buf = self.dir.join(remote_path);
        fs::create_dir_all(buf.parent().unwrap()).await?;
        Ok(buf.to_str().unwrap().to_string())
    }
}

impl LocalDirRemoteFs {
    fn remove_empty_paths_boxed(
        root: PathBuf,
        path: PathBuf,
    ) -> BoxFuture<'static, Result<(), CubeError>> {
        async move { Self::remove_empty_paths(root, path).await }.boxed()
    }

    async fn remove_empty_paths(root: PathBuf, path: PathBuf) -> Result<(), CubeError> {
        if let Some(parent_path) = path.parent() {
            let mut dir = fs::read_dir(parent_path).await?;
            if dir.next_entry().await?.is_none() {
                fs::remove_dir(parent_path).await?;
            }
            if root != parent_path.to_path_buf() {
                return Ok(Self::remove_empty_paths_boxed(root, parent_path.to_path_buf()).await?);
            }
        }
        Ok(())
    }

    fn list_recursive_boxed(
        remote_dir: PathBuf,
        remote_prefix: String,
        dir: PathBuf,
    ) -> BoxFuture<'static, Result<Vec<RemoteFile>, CubeError>> {
        async move { Self::list_recursive(remote_dir, remote_prefix, dir).await }.boxed()
    }

    pub async fn list_recursive(
        remote_dir: PathBuf,
        remote_prefix: String,
        dir: PathBuf,
    ) -> Result<Vec<RemoteFile>, CubeError> {
        let mut result = Vec::new();
        if fs::metadata(dir.clone()).await.is_err() {
            return Ok(vec![]);
        }
        if let Ok(mut dir) = fs::read_dir(dir).await {
            while let Ok(Some(file)) = dir.next_entry().await {
                if let Ok(true) = file.file_type().await.map(|r| r.is_dir()) {
                    result.append(
                        &mut Self::list_recursive_boxed(
                            remote_dir.clone(),
                            remote_prefix.to_string(),
                            file.path(),
                        )
                        .await?,
                    );
                } else if let Ok(metadata) = file.metadata().await {
                    let relative_name = file
                        .path()
                        .to_str()
                        .unwrap()
                        .to_string()
                        .replace(&remote_dir.to_str().unwrap().to_string(), "")
                        .trim_start_matches("/")
                        .to_string();
                    if relative_name.starts_with(&remote_prefix) {
                        result.push(RemoteFile {
                            remote_path: relative_name.to_string(),
                            updated: DateTime::from(metadata.modified()?),
                            file_size: metadata.len(),
                        });
                    }
                }
            }
        }
        Ok(result)
    }
}
