use std::{path::Path, sync::Arc};

use anyhow::Context;
use remote_storage::{GenericRemoteStorage, RemotePath};
use tokio::sync::OwnedMutexGuard;
use tracing::{error, instrument, Instrument, Span};

use utils::{crashsafe, id::TenantId};

use crate::{
    config::PageServerConf,
    task_mgr::{self, TaskKind},
};

use super::{
    mgr::{GetTenantError, TenantsMap},
    timeline::delete::DeleteTimelineFlow,
    tree_sort_timelines, Tenant,
};

#[derive(Debug, thiserror::Error)]
pub enum DeleteTenantError {
    #[error("GetTenant {0}")]
    Get(#[from] GetTenantError),

    #[error("Tenant deletion is already in progress")]
    AlreadyInProgress,

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

async fn create_remote_delete_mark(
    conf: &PageServerConf,
    remote_storage: GenericRemoteStorage,
    tenant_id: TenantId,
) -> Result<(), DeleteTenantError> {
    let tenant_remote_path =
        RemotePath::new(&conf.tenant_path(&tenant_id)).context("tenant path")?;
    let remote_mark_path = tenant_remote_path.join(Path::new("deleted"));

    // TODO check if that works
    let data: &[u8] = &[];
    remote_storage
        .upload(data, 0, &remote_mark_path, None)
        .await
        .context("mark upload")?;

    Ok(())
}

async fn create_local_delete_mark(
    conf: &PageServerConf,
    tenant_id: TenantId,
) -> Result<(), DeleteTenantError> {
    let marker_path = conf.tenant_deleted_mark_file_path(tenant_id);

    // Note: we're ok to replace existing file.
    let _ = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(&marker_path)
        .with_context(|| format!("could not create delete marker file {marker_path:?}"))?;

    crashsafe::fsync_file_and_parent(&marker_path).context("sync_mark")?;

    Ok(())
}

async fn schedule_ordered_timeline_deletions(
    tenant: &Arc<Tenant>,
) -> Result<tokio::sync::mpsc::UnboundedReceiver<()>, DeleteTenantError> {
    // Tenant is stopping at this point. We know it will be deleted. No new timelines should be created.
    // Tree sort timelines to delete from leafs to the root.
    let timelines = tenant.timelines.lock().unwrap().clone();
    let sorted =
        tree_sort_timelines(timelines, |t| t.get_ancestor_timeline_id()).context("tree sort")?;

    // Use the trick from tokio shutdown tips: https://tokio.rs/tokio/topics/shutdown
    // See the "Waiting for things to finish shutting down" paragraph
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<()>();

    for (timeline_id, _) in sorted.into_iter().rev() {
        // NOTE: idea, can skip local/remote marks probably? because there is already one for tenant
        DeleteTimelineFlow::run(tenant, timeline_id, Some(tx.clone()))
            .await
            .context("delete timeline")?;
    }

    Ok(rx)
}

/// FIXME dajust this comment:
/// Orchestrates timeline shut down of all timeline tasks, removes its in-memory structures,
/// and deletes its data from both disk and s3.
/// The sequence of steps:
/// 1. Set deleted_at in remote index part.
/// 2. Create local mark file.
/// 3. Delete local files except metadata (it is simpler this way, to be able to reuse timeline initialization code that expects metadata)
/// 4. Delete remote layers
/// 5. Delete index part
/// 6. Delete meta, timeline directory
/// 7. Delete mark file
/// It is resumable from any step in case a crash/restart occurs.
/// There are three entrypoints to the process:
/// 1. [`DeleteTimelineFlow::run`] this is the main one called by a management api handler.
/// 2. [`DeleteTimelineFlow::resume_deletion`] is called during restarts when local metadata is still present
/// and we possibly neeed to continue deletion of remote files.
/// 3. [`DeleteTimelineFlow::cleanup_remaining_fs_traces_after_timeline_deletion`] is used when we deleted remote
/// index but still have local metadata, timeline directory and delete mark.
/// Note the only other place that messes around timeline delete mark is the logic that scans directory with timelines during tenant load.
#[derive(Default)]
pub enum DeleteTenantFlow {
    #[default]
    NotStarted,
    InProgress,
    Finished,
}

impl DeleteTenantFlow {
    // These steps are run in the context of management api request handler.
    // Long running steps are continued to run in the background.
    // NB: If this fails half-way through, and is retried, the retry will go through
    // all the same steps again. Make sure the code here is idempotent, and don't
    // error out if some of the shutdown tasks have already been completed!
    #[instrument(skip(conf, tenants, remote_storage), fields(tenant_id=%tenant_id))]
    pub(crate) async fn run(
        conf: &'static PageServerConf,
        remote_storage: Option<GenericRemoteStorage>,
        tenants: &tokio::sync::RwLock<TenantsMap>,
        tenant_id: TenantId,
    ) -> Result<(), DeleteTenantError> {
        let (tenant, mut guard) = Self::prepare(tenants, tenant_id).await?;

        guard.mark_in_progress()?;
        // IDEA: implement detach as delete without remote storage. Then they would use the same lock (deletion_progress) so wont contend.
        // Though sounds scary, different mark name?
        // Detach currently uses remove_dir_all so in case of a crash we can end up in a weird state.
        if let Some(remote_storage) = remote_storage {
            create_remote_delete_mark(conf, remote_storage, tenant_id)
                .await
                .context("create delete mark")?
        }

        create_local_delete_mark(conf, tenant_id)
            .await
            .context("local delete mark")?;

        Self::schedule_background(guard, conf, tenant);

        Ok(())
    }

    fn mark_in_progress(&mut self) -> anyhow::Result<()> {
        match self {
            Self::Finished => anyhow::bail!("Bug. Is in finished state"),
            Self::InProgress { .. } => { /* We're in a retry */ }
            Self::NotStarted => { /* Fresh start */ }
        }

        *self = Self::InProgress;

        Ok(())
    }

    async fn prepare(
        tenants: &tokio::sync::RwLock<TenantsMap>,
        tenant_id: TenantId,
    ) -> Result<(Arc<Tenant>, tokio::sync::OwnedMutexGuard<Self>), DeleteTenantError> {
        let m = tenants.read().await;

        let tenant = m
            .get(&tenant_id)
            .ok_or(GetTenantError::NotFound(tenant_id))?;

        // FIXME: unsure about active only. Our init jobs may not be cancellable properly,
        // so at least for now allow deletions only for active tenants. TODO recheck
        // Broken is needed for retries.
        if !(tenant.is_active() || tenant.is_broken()) {
            return Err(GetTenantError::NotActive(tenant_id).into());
        }

        let guard = Arc::clone(&tenant.delete_progress)
            .try_lock_owned()
            .map_err(|_| DeleteTenantError::AlreadyInProgress)?;

        tenant
            .shutdown(false)
            .await
            .map_err(|e| anyhow::anyhow!("tenant shutdown failed: {e:?}"))?;

        Ok((Arc::clone(tenant), guard))
    }

    fn schedule_background(
        guard: OwnedMutexGuard<Self>,
        conf: &'static PageServerConf,
        tenant: Arc<Tenant>,
    ) {
        let tenant_id = tenant.tenant_id;

        task_mgr::spawn(
            task_mgr::BACKGROUND_RUNTIME.handle(),
            TaskKind::TimelineDeletionWorker,
            Some(tenant_id),
            None,
            "tenant_delete",
            false,
            async move {
                if let Err(err) = Self::background(guard, conf, &tenant).await {
                    error!("Error: {err:#}");
                    tenant.set_broken(err.to_string()).await;
                };
                Ok(())
            }
            .instrument({
                let span = tracing::info_span!(parent: None, "delete_tenant", tenant_id=%tenant_id);
                span.follows_from(Span::current());
                span
            }),
        );
    }

    async fn background(
        mut guard: OwnedMutexGuard<Self>,
        conf: &PageServerConf,
        tenant: &Arc<Tenant>,
    ) -> Result<(), DeleteTenantError> {
        // Tree sort timelines, schedule delete for them. Mention retries from the console side.
        let mut rx = schedule_ordered_timeline_deletions(tenant).await?;

        assert!(rx.recv().await.is_none());
        // Note that deletions can fail. If all deletion tasks exited but there are still timelines -> transition into broken state and follow the same retry logic
        // Wait till delete finishes (can use channel trick i e wait till its closed and give out receivers to timeline delete flows)

        // TODO cleanup remote mark, remove local traces

        // Call the same as detach calls

        // Cleanup local traces
        *guard = Self::Finished;

        Ok(())
    }
}
