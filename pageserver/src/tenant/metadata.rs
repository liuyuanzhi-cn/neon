//! Every image of a certain timeline from [`crate::tenant::Tenant`]
//! has a metadata that needs to be stored persistently.
//!
//! Later, the file gets used in [`remote_timeline_client`] as a part of
//! external storage import and export operations.
//!
//! The module contains all structs and related helper methods related to timeline metadata.
//!
//! [`remote_timeline_client`]: super::remote_timeline_client

use std::io::{self};

use anyhow::{ensure, Context};
use serde::{de::Error, Deserialize, Serialize, Serializer};
use thiserror::Error;
use utils::bin_ser::SerializeError;
use utils::crashsafe::path_with_suffix_extension;
use utils::{
    bin_ser::BeSer,
    id::{TenantId, TimelineId},
    lsn::Lsn,
};

use crate::config::PageServerConf;
use crate::virtual_file::VirtualFile;
use crate::TEMP_FILE_SUFFIX;

/// Use special format number to enable backward compatibility.
const METADATA_FORMAT_VERSION: u16 = 4;

/// Previous supported format versions.
const METADATA_OLD_FORMAT_VERSION: u16 = 3;

/// We assume that a write of up to METADATA_MAX_SIZE bytes is atomic.
///
/// This is the same assumption that PostgreSQL makes with the control file,
/// see PG_CONTROL_MAX_SAFE_SIZE
const METADATA_MAX_SIZE: usize = 512;

/// Metadata stored on disk for each timeline
///
/// The fields correspond to the values we hold in memory, in Timeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimelineMetadata {
    hdr: TimelineMetadataHeader,
    body: TimelineMetadataBodyV2,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TimelineMetadataHeader {
    checksum: u32,       // CRC of serialized metadata body
    size: u16,           // size of serialized metadata
    format_version: u16, // metadata format version (used for compatibility checks)
}
const METADATA_HDR_SIZE: usize = std::mem::size_of::<TimelineMetadataHeader>();

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TimelineMetadataBodyV2 {
    disk_consistent_lsn: Lsn,
    // This is only set if we know it. We track it in memory when the page
    // server is running, but we only track the value corresponding to
    // 'last_record_lsn', not 'disk_consistent_lsn' which can lag behind by a
    // lot. We only store it in the metadata file when we flush *all* the
    // in-memory data so that 'last_record_lsn' is the same as
    // 'disk_consistent_lsn'.  That's OK, because after page server restart, as
    // soon as we reprocess at least one record, we will have a valid
    // 'prev_record_lsn' value in memory again. This is only really needed when
    // doing a clean shutdown, so that there is no more WAL beyond
    // 'disk_consistent_lsn'
    prev_record_lsn: Option<Lsn>,
    ancestor_timeline: Option<TimelineId>,
    ancestor_lsn: Lsn,
    latest_gc_cutoff_lsn: Lsn,
    initdb_lsn: Lsn,
    pg_version: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TimelineMetadataBodyV1 {
    disk_consistent_lsn: Lsn,
    // This is only set if we know it. We track it in memory when the page
    // server is running, but we only track the value corresponding to
    // 'last_record_lsn', not 'disk_consistent_lsn' which can lag behind by a
    // lot. We only store it in the metadata file when we flush *all* the
    // in-memory data so that 'last_record_lsn' is the same as
    // 'disk_consistent_lsn'.  That's OK, because after page server restart, as
    // soon as we reprocess at least one record, we will have a valid
    // 'prev_record_lsn' value in memory again. This is only really needed when
    // doing a clean shutdown, so that there is no more WAL beyond
    // 'disk_consistent_lsn'
    prev_record_lsn: Option<Lsn>,
    ancestor_timeline: Option<TimelineId>,
    ancestor_lsn: Lsn,
    latest_gc_cutoff_lsn: Lsn,
    initdb_lsn: Lsn,
}

impl TimelineMetadata {
    pub fn new(
        disk_consistent_lsn: Lsn,
        prev_record_lsn: Option<Lsn>,
        ancestor_timeline: Option<TimelineId>,
        ancestor_lsn: Lsn,
        latest_gc_cutoff_lsn: Lsn,
        initdb_lsn: Lsn,
        pg_version: u32,
    ) -> Self {
        Self {
            hdr: TimelineMetadataHeader {
                checksum: 0,
                size: 0,
                format_version: METADATA_FORMAT_VERSION,
            },
            body: TimelineMetadataBodyV2 {
                disk_consistent_lsn,
                prev_record_lsn,
                ancestor_timeline,
                ancestor_lsn,
                latest_gc_cutoff_lsn,
                initdb_lsn,
                pg_version,
            },
        }
    }

    fn upgrade_timeline_metadata(metadata_bytes: &[u8]) -> anyhow::Result<Self> {
        let mut hdr = TimelineMetadataHeader::des(&metadata_bytes[0..METADATA_HDR_SIZE])?;

        // backward compatible only up to this version
        ensure!(
            hdr.format_version == METADATA_OLD_FORMAT_VERSION,
            "unsupported metadata format version {}",
            hdr.format_version
        );

        let metadata_size = hdr.size as usize;

        let body: TimelineMetadataBodyV1 =
            TimelineMetadataBodyV1::des(&metadata_bytes[METADATA_HDR_SIZE..metadata_size])?;

        let body = TimelineMetadataBodyV2 {
            disk_consistent_lsn: body.disk_consistent_lsn,
            prev_record_lsn: body.prev_record_lsn,
            ancestor_timeline: body.ancestor_timeline,
            ancestor_lsn: body.ancestor_lsn,
            latest_gc_cutoff_lsn: body.latest_gc_cutoff_lsn,
            initdb_lsn: body.initdb_lsn,
            pg_version: 14, // All timelines created before this version had pg_version 14
        };

        hdr.format_version = METADATA_FORMAT_VERSION;

        Ok(Self { hdr, body })
    }

    pub fn from_bytes(metadata_bytes: &[u8]) -> anyhow::Result<Self> {
        ensure!(
            metadata_bytes.len() == METADATA_MAX_SIZE,
            "metadata bytes size is wrong"
        );
        let hdr = TimelineMetadataHeader::des(&metadata_bytes[0..METADATA_HDR_SIZE])?;

        let metadata_size = hdr.size as usize;
        ensure!(
            metadata_size <= METADATA_MAX_SIZE,
            "corrupted metadata file"
        );
        let calculated_checksum = crc32c::crc32c(&metadata_bytes[METADATA_HDR_SIZE..metadata_size]);
        ensure!(
            hdr.checksum == calculated_checksum,
            "metadata checksum mismatch"
        );

        if hdr.format_version != METADATA_FORMAT_VERSION {
            // If metadata has the old format,
            // upgrade it and return the result
            TimelineMetadata::upgrade_timeline_metadata(metadata_bytes)
        } else {
            let body =
                TimelineMetadataBodyV2::des(&metadata_bytes[METADATA_HDR_SIZE..metadata_size])?;
            ensure!(
                body.disk_consistent_lsn.is_aligned(),
                "disk_consistent_lsn is not aligned"
            );
            Ok(TimelineMetadata { hdr, body })
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, SerializeError> {
        let body_bytes = self.body.ser()?;
        let metadata_size = METADATA_HDR_SIZE + body_bytes.len();
        let hdr = TimelineMetadataHeader {
            size: metadata_size as u16,
            format_version: METADATA_FORMAT_VERSION,
            checksum: crc32c::crc32c(&body_bytes),
        };
        let hdr_bytes = hdr.ser()?;
        let mut metadata_bytes = vec![0u8; METADATA_MAX_SIZE];
        metadata_bytes[0..METADATA_HDR_SIZE].copy_from_slice(&hdr_bytes);
        metadata_bytes[METADATA_HDR_SIZE..metadata_size].copy_from_slice(&body_bytes);
        Ok(metadata_bytes)
    }

    /// [`Lsn`] that corresponds to the corresponding timeline directory
    /// contents, stored locally in the pageserver workdir.
    pub fn disk_consistent_lsn(&self) -> Lsn {
        self.body.disk_consistent_lsn
    }

    pub fn prev_record_lsn(&self) -> Option<Lsn> {
        self.body.prev_record_lsn
    }

    pub fn ancestor_timeline(&self) -> Option<TimelineId> {
        self.body.ancestor_timeline
    }

    pub fn ancestor_lsn(&self) -> Lsn {
        self.body.ancestor_lsn
    }

    pub fn latest_gc_cutoff_lsn(&self) -> Lsn {
        self.body.latest_gc_cutoff_lsn
    }

    pub fn initdb_lsn(&self) -> Lsn {
        self.body.initdb_lsn
    }

    pub fn pg_version(&self) -> u32 {
        self.body.pg_version
    }

    // Checksums make it awkward to build a valid instance by hand.  This helper
    // provides a TimelineMetadata with a valid checksum in its header.
    #[cfg(test)]
    pub fn example() -> Self {
        let instance = Self::new(
            "0/16960E8".parse::<Lsn>().unwrap(),
            None,
            None,
            Lsn::from_hex("00000000").unwrap(),
            Lsn::from_hex("00000000").unwrap(),
            Lsn::from_hex("00000000").unwrap(),
            0,
        );
        let bytes = instance.to_bytes().unwrap();
        Self::from_bytes(&bytes).unwrap()
    }
}

impl<'de> Deserialize<'de> for TimelineMetadata {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let bytes = Vec::<u8>::deserialize(deserializer)?;
        Self::from_bytes(bytes.as_slice()).map_err(|e| D::Error::custom(format!("{e}")))
    }
}

impl Serialize for TimelineMetadata {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let bytes = self
            .to_bytes()
            .map_err(|e| serde::ser::Error::custom(format!("{e}")))?;
        bytes.serialize(serializer)
    }
}

/// Save timeline metadata to file
#[tracing::instrument(skip_all, fields(%tenant_id, %timeline_id))]
pub async fn save_metadata(
    conf: &'static PageServerConf,
    tenant_id: &TenantId,
    timeline_id: &TimelineId,
    data: &TimelineMetadata,
) -> anyhow::Result<()> {
    let path = conf.metadata_path(tenant_id, timeline_id);
    let temp_path = path_with_suffix_extension(&path, TEMP_FILE_SUFFIX);
    let metadata_bytes = data.to_bytes().context("serialize metadata")?;
    VirtualFile::crashsafe_overwrite(&path, &temp_path, &metadata_bytes)
        .await
        .context("write metadata")?;
    Ok(())
}

#[derive(Error, Debug)]
pub enum LoadMetadataError {
    #[error(transparent)]
    Read(#[from] io::Error),

    #[error(transparent)]
    Decode(#[from] anyhow::Error),
}

pub fn load_metadata(
    conf: &'static PageServerConf,
    tenant_id: &TenantId,
    timeline_id: &TimelineId,
) -> Result<TimelineMetadata, LoadMetadataError> {
    let metadata_path = conf.metadata_path(tenant_id, timeline_id);
    let metadata_bytes = std::fs::read(metadata_path)?;

    Ok(TimelineMetadata::from_bytes(&metadata_bytes)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tenant::harness::TIMELINE_ID;

    #[test]
    fn metadata_serializes_correctly() {
        let original_metadata = TimelineMetadata::new(
            Lsn(0x200),
            Some(Lsn(0x100)),
            Some(TIMELINE_ID),
            Lsn(0),
            Lsn(0),
            Lsn(0),
            // Any version will do here, so use the default
            crate::DEFAULT_PG_VERSION,
        );

        let metadata_bytes = original_metadata
            .to_bytes()
            .expect("Should serialize correct metadata to bytes");

        let deserialized_metadata = TimelineMetadata::from_bytes(&metadata_bytes)
            .expect("Should deserialize its own bytes");

        assert_eq!(
            deserialized_metadata.body, original_metadata.body,
            "Metadata that was serialized to bytes and deserialized back should not change"
        );
    }

    // Generate old version metadata and read it with current code.
    // Ensure that it is upgraded correctly
    #[test]
    fn test_metadata_upgrade() {
        #[derive(Debug, Clone, PartialEq, Eq)]
        struct TimelineMetadataV1 {
            hdr: TimelineMetadataHeader,
            body: TimelineMetadataBodyV1,
        }

        let metadata_v1 = TimelineMetadataV1 {
            hdr: TimelineMetadataHeader {
                checksum: 0,
                size: 0,
                format_version: METADATA_OLD_FORMAT_VERSION,
            },
            body: TimelineMetadataBodyV1 {
                disk_consistent_lsn: Lsn(0x200),
                prev_record_lsn: Some(Lsn(0x100)),
                ancestor_timeline: Some(TIMELINE_ID),
                ancestor_lsn: Lsn(0),
                latest_gc_cutoff_lsn: Lsn(0),
                initdb_lsn: Lsn(0),
            },
        };

        impl TimelineMetadataV1 {
            pub fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
                let body_bytes = self.body.ser()?;
                let metadata_size = METADATA_HDR_SIZE + body_bytes.len();
                let hdr = TimelineMetadataHeader {
                    size: metadata_size as u16,
                    format_version: METADATA_OLD_FORMAT_VERSION,
                    checksum: crc32c::crc32c(&body_bytes),
                };
                let hdr_bytes = hdr.ser()?;
                let mut metadata_bytes = vec![0u8; METADATA_MAX_SIZE];
                metadata_bytes[0..METADATA_HDR_SIZE].copy_from_slice(&hdr_bytes);
                metadata_bytes[METADATA_HDR_SIZE..metadata_size].copy_from_slice(&body_bytes);
                Ok(metadata_bytes)
            }
        }

        let metadata_bytes = metadata_v1
            .to_bytes()
            .expect("Should serialize correct metadata to bytes");

        // This should deserialize to the latest version format
        let deserialized_metadata = TimelineMetadata::from_bytes(&metadata_bytes)
            .expect("Should deserialize its own bytes");

        let expected_metadata = TimelineMetadata::new(
            Lsn(0x200),
            Some(Lsn(0x100)),
            Some(TIMELINE_ID),
            Lsn(0),
            Lsn(0),
            Lsn(0),
            14, // All timelines created before this version had pg_version 14
        );

        assert_eq!(
            deserialized_metadata.body, expected_metadata.body,
            "Metadata of the old version {} should be upgraded to the latest version {}",
            METADATA_OLD_FORMAT_VERSION, METADATA_FORMAT_VERSION
        );
    }
}
