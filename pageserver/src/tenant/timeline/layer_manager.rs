use anyhow::{Context, Result};
use std::{collections::HashMap, sync::Arc};
use utils::lsn::Lsn;

use crate::{
    metrics::TimelineMetrics,
    tenant::{
        layer_map::{BatchedUpdates, LayerMap},
        storage_layer::{
            DeltaLayer, ImageLayer, InMemoryLayer, PersistentLayer, PersistentLayerDesc,
            PersistentLayerKey, RemoteLayer,
        },
        timeline::compare_arced_layers,
    },
};

pub struct LayerManager {
    layer_map: LayerMap,
    layer_fmgr: LayerFileManager,
}

pub struct ApplyGcResultGuard<'a>(BatchedUpdates<'a>);

impl ApplyGcResultGuard<'_> {
    pub fn flush(self) {
        self.0.flush();
    }
}

impl LayerManager {
    pub fn create() -> Self {
        Self {
            layer_map: LayerMap::default(),
            layer_fmgr: LayerFileManager::new(),
        }
    }

    pub fn get_from_desc(&self, desc: &PersistentLayerDesc) -> Arc<dyn PersistentLayer> {
        self.layer_fmgr.get_from_desc(desc)
    }

    /// Get an immutable reference to the layer map.
    ///
    /// We expect users only to be able to get an immutable layer map. If users want to make modifications,
    /// they should use the below semantic APIs. This design makes us step closer to immutable storage state.
    pub fn layer_map(&self) -> &LayerMap {
        &self.layer_map
    }

    pub fn replace_and_verify(
        &mut self,
        expected: Arc<dyn PersistentLayer>,
        new: Arc<dyn PersistentLayer>,
    ) -> Result<()> {
        self.layer_fmgr.replace_and_verify(expected, new)
    }

    /// Called from `load_layer_map`. Initialize the layer manager with:
    /// 1. all on-disk layers
    /// 2. next open layer (with disk disk_consistent_lsn LSN)
    pub fn initialize(
        &mut self,
        on_disk_layers: Vec<Arc<dyn PersistentLayer>>,
        next_open_layer_at: Lsn,
    ) {
        let mut updates = self.layer_map.batch_update();
        for layer in on_disk_layers {
            Self::insert_historic_layer(layer, &mut updates, &mut self.layer_fmgr);
        }
        updates.flush();
        self.layer_map.next_open_layer_at = Some(next_open_layer_at);
    }

    /// Initialize when creating a new timeline, called in `init_empty_layer_map`.
    pub fn initialize_empty(&mut self, next_open_layer_at: Lsn) {
        self.layer_map.next_open_layer_at = Some(next_open_layer_at);
    }

    pub fn initialize_remote_layers(
        &mut self,
        corrupted_local_layers: Vec<Arc<dyn PersistentLayer>>,
        remote_layers: Vec<Arc<RemoteLayer>>,
    ) {
        let mut updates = self.layer_map.batch_update();
        for layer in corrupted_local_layers {
            Self::remove_historic_layer(layer, &mut updates, &mut self.layer_fmgr);
        }
        for layer in remote_layers {
            Self::insert_historic_layer(layer, &mut updates, &mut self.layer_fmgr);
        }
        updates.flush();
    }

    /// Open a new writable layer to append data, called within `get_layer_for_write`.
    pub fn open_new_layer(&mut self, layer: Arc<InMemoryLayer>) {
        self.layer_map.open_layer = Some(layer);
        self.layer_map.next_open_layer_at = None;
    }

    /// Called from `freeze_inmem_layer`, returns true if successfully frozen.
    #[must_use]
    pub fn try_freeze_in_memory_layer(&mut self, end_lsn: Lsn) -> bool {
        if let Some(open_layer) = &self.layer_map.open_layer {
            let open_layer_rc = Arc::clone(open_layer);
            // Does this layer need freezing?
            open_layer.freeze(end_lsn);

            // The layer is no longer open, update the layer map to reflect this.
            // We will replace it with on-disk historics below.
            self.layer_map.frozen_layers.push_back(open_layer_rc);
            self.layer_map.open_layer = None;
            self.layer_map.next_open_layer_at = Some(end_lsn);
            true
        } else {
            false
        }
    }

    /// Pop the last frozen layer from the layer map, called from `flush_frozen_layer`.
    pub fn flush_frozen_layer(&mut self) -> Option<Arc<InMemoryLayer>> {
        self.layer_map.frozen_layers.pop_front()
    }

    /// Add image layers to the layer map, called from `create_image_layers`.
    pub fn create_image_layers(&mut self, image_layers: Vec<ImageLayer>) {
        let mut updates = self.layer_map.batch_update();
        for layer in image_layers {
            Self::insert_historic_layer(Arc::new(layer), &mut updates, &mut self.layer_fmgr);
        }
        updates.flush();
    }

    pub fn flush_l0_delta_layer(&mut self, delta_layer: Arc<DeltaLayer>) {
        let mut updates = self.layer_map.batch_update();
        Self::insert_historic_layer(delta_layer, &mut updates, &mut self.layer_fmgr);
        updates.flush();
    }

    pub fn compact_l0(
        &mut self,
        layer_removal_cs: Arc<tokio::sync::OwnedMutexGuard<()>>,
        compact_from: Vec<Arc<dyn PersistentLayer>>,
        compact_to: Vec<Arc<dyn PersistentLayer>>,
        metrics: &TimelineMetrics,
    ) -> Result<()> {
        let mut updates = self.layer_map.batch_update();
        for l in compact_to {
            Self::insert_historic_layer(l, &mut updates, &mut self.layer_fmgr);
        }
        for l in compact_from {
            // NB: the layer file identified by descriptor `l` is guaranteed to be present
            // in the LayerFileManager because we kept holding `layer_removal_cs` the entire
            // time, even though we dropped `Timeline::layers` inbetween.
            Self::delete_historic_layer(
                layer_removal_cs.clone(),
                l,
                &mut updates,
                metrics,
                &mut self.layer_fmgr,
            )?;
        }
        updates.flush();
        Ok(())
    }

    pub fn gc_timeline(
        &mut self,
        layer_removal_cs: Arc<tokio::sync::OwnedMutexGuard<()>>,
        gc_layers: Vec<Arc<dyn PersistentLayer>>,
        metrics: &TimelineMetrics,
    ) -> Result<ApplyGcResultGuard> {
        let mut updates = self.layer_map.batch_update();
        for doomed_layer in gc_layers {
            Self::delete_historic_layer(
                layer_removal_cs.clone(),
                doomed_layer,
                &mut updates,
                metrics,
                &mut self.layer_fmgr,
            )?; // FIXME: schedule succeeded deletions in timeline.rs `gc_timeline` instead of in batch?
        }
        Ok(ApplyGcResultGuard(updates))
    }

    /// Helper function to insert a layer into the layer map and file manager.
    fn insert_historic_layer(
        layer: Arc<dyn PersistentLayer>,
        updates: &mut BatchedUpdates<'_>,
        mapping: &mut LayerFileManager,
    ) {
        updates.insert_historic(layer.layer_desc().clone());
        mapping.insert(layer);
    }

    /// Helper function to remove a layer into the layer map and file manager
    fn remove_historic_layer(
        layer: Arc<dyn PersistentLayer>,
        updates: &mut BatchedUpdates<'_>,
        mapping: &mut LayerFileManager,
    ) {
        updates.remove_historic(layer.layer_desc().clone());
        mapping.remove(layer);
    }

    /// Removes the layer from local FS (if present) and from memory.
    /// Remote storage is not affected by this operation.
    fn delete_historic_layer(
        // we cannot remove layers otherwise, since gc and compaction will race
        _layer_removal_cs: Arc<tokio::sync::OwnedMutexGuard<()>>,
        layer: Arc<dyn PersistentLayer>,
        updates: &mut BatchedUpdates<'_>,
        metrics: &TimelineMetrics,
        mapping: &mut LayerFileManager,
    ) -> anyhow::Result<()> {
        if !layer.is_remote_layer() {
            layer.delete_resident_layer_file()?;
            let layer_file_size = layer.file_size();
            metrics.resident_physical_size_gauge.sub(layer_file_size);
        }

        // TODO Removing from the bottom of the layer map is expensive.
        //      Maybe instead discard all layer map historic versions that
        //      won't be needed for page reconstruction for this timeline,
        //      and mark what we can't delete yet as deleted from the layer
        //      map index without actually rebuilding the index.
        updates.remove_historic(layer.layer_desc().clone());
        mapping.remove(layer);

        Ok(())
    }
}

pub struct LayerFileManager(HashMap<PersistentLayerKey, Arc<dyn PersistentLayer>>);

impl LayerFileManager {
    fn get_from_desc(&self, desc: &PersistentLayerDesc) -> Arc<dyn PersistentLayer> {
        // The assumption for the `expect()` is that all code maintains the following invariant:
        // A layer's descriptor is present in the LayerMap => the LayerFileManager contains a layer for the descriptor.
        self.0
            .get(&desc.key())
            .with_context(|| format!("get layer from desc: {}", desc.filename().file_name()))
            .expect("not found")
            .clone()
    }

    pub(crate) fn insert(&mut self, layer: Arc<dyn PersistentLayer>) {
        let present = self.0.insert(layer.layer_desc().key(), layer.clone());
        if present.is_some() && cfg!(debug_assertions) {
            panic!("overwriting a layer: {:?}", layer.layer_desc())
        }
    }

    pub(crate) fn new() -> Self {
        Self(HashMap::new())
    }

    pub(crate) fn remove(&mut self, layer: Arc<dyn PersistentLayer>) {
        let present = self.0.remove(&layer.layer_desc().key());
        if present.is_none() && cfg!(debug_assertions) {
            panic!(
                "removing layer that is not present in layer mapping: {:?}",
                layer.layer_desc()
            )
        }
    }

    pub(crate) fn replace_and_verify(
        &mut self,
        expected: Arc<dyn PersistentLayer>,
        new: Arc<dyn PersistentLayer>,
    ) -> Result<()> {
        let key = expected.layer_desc().key();
        let other = new.layer_desc().key();

        let expected_l0 = LayerMap::is_l0(expected.layer_desc());
        let new_l0 = LayerMap::is_l0(new.layer_desc());

        fail::fail_point!("layermap-replace-notfound", |_| anyhow::bail!(
            "layermap-replace-notfound"
        ));

        anyhow::ensure!(
            key == other,
            "expected and new layer have different keys: {key:?} != {other:?}"
        );

        anyhow::ensure!(
            expected_l0 == new_l0,
            "one layer is l0 while the other is not: {expected_l0} != {new_l0}"
        );

        if let Some(layer) = self.0.get_mut(&expected.layer_desc().key()) {
            anyhow::ensure!(
                compare_arced_layers(&expected, layer),
                "another layer was found instead of expected, expected={expected:?}, new={new:?}",
                expected = Arc::as_ptr(&expected),
                new = Arc::as_ptr(layer),
            );
            *layer = new;
            Ok(())
        } else {
            anyhow::bail!("layer was not found");
        }
    }
}
