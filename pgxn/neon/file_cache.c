/*
 *
 * file_cache.c
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  pgxn/neon/file_cache.c
 *
 *-------------------------------------------------------------------------
 */

#include <sys/file.h>
#include <sys/statvfs.h>
#include <unistd.h>
#include <fcntl.h>

#include "postgres.h"

#include "neon_pgversioncompat.h"

#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "pagestore_client.h"
#include "access/parallel.h"
#include "postmaster/bgworker.h"
#include RELFILEINFO_HDR
#include "storage/buf_internals.h"
#include "storage/latch.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "utils/dynahash.h"
#include "utils/guc.h"
#include "storage/fd.h"
#include "storage/pg_shmem.h"
#include "storage/buf_internals.h"
#include "storage/procsignal.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"

/*
 * Local file cache is used to temporary store relations pages in local file system.
 * All blocks of all relations are stored inside one file and addressed using shared hash map.
 * Currently LRU eviction policy based on L2 list is used as replacement algorithm.
 * As far as manipulation of L2-list requires global critical section, we are not using partitioned hash.
 * Also we are using exclusive lock even for read operation because LRU requires relinking element in L2 list.
 * If this lock become a bottleneck, we can consider other eviction strategies, for example clock algorithm.
 *
 * Cache is always reconstructed at node startup, so we do not need to save mapping somewhere and worry about
 * its consistency.
 */

/* Local file storage allocation chunk.
 * Should be power of two and not less than 32. Using larger than page chunks can
 * 1. Reduce hash-map memory footprint: 8TB database contains billion pages
 *    and size of hash entry is 40 bytes, so we need 40Gb just for hash map.
 *    1Mb chunks can reduce hash map size to 320Mb.
 * 2. Improve access locality, subsequent pages will be allocated together improving seqscan speed
 */
#define BLOCKS_PER_CHUNK	128 /* 1Mb chunk */
#define MB					((uint64)1024*1024)

#define SIZE_MB_TO_CHUNKS(size) ((uint32)((size) * MB / BLCKSZ / BLOCKS_PER_CHUNK))

#define MAX_MONITOR_INTERVAL_USEC 1000000 /* 1 second */
#define MAX_DISK_WRITE_RATE       1000 /* MB/sec */

typedef struct FileCacheEntry
{
	BufferTag	key;
	uint32		offset;
	uint32		access_count;
	uint32		bitmap[BLOCKS_PER_CHUNK/32];
	dlist_node	lru_node; /* LRU list node */
} FileCacheEntry;

typedef struct FileCacheControl
{
	uint64 generation; /* generation is needed to handle correct hash reenabling */
	uint32 size; /* size of cache file in chunks */
	uint32 used; /* number of used chunks */
	dlist_head lru; /* double linked list for LRU replacement algorithm */
} FileCacheControl;

static HTAB* lfc_hash;
static int   lfc_desc = 0;
static LWLockId lfc_lock;
static int   lfc_max_size;
static int   lfc_size_limit;
static int   lfc_free_space_watermark;
static char* lfc_path;
static  FileCacheControl* lfc_ctl;
static shmem_startup_hook_type prev_shmem_startup_hook;
#if PG_VERSION_NUM>=150000
static shmem_request_hook_type prev_shmem_request_hook;
#endif
static int   lfc_shrinking_factor; /* power of two by which local cache size will be shrinked when lfc_free_space_watermark is reached */

void FileCacheMonitorMain(Datum main_arg);

/*
 * Local file cache is mandatory and Neon can work without it.
 * In case of any any errors with this cache, we should disable it but to not throw error.
 * Also we should allow  re-enable it if source of failure (lack of disk space, permissions,...) is fixed.
 * All cache content should be invalidated to avoid reading of stale or corrupted data
 */
static void
lfc_disable(char const* op)
{
	HASH_SEQ_STATUS status;
	FileCacheEntry* entry;

	elog(WARNING, "Failed to %s local file cache at %s: %m, disabling local file cache", op, lfc_path);

	if (lfc_desc > 0)
		close(lfc_desc);

	lfc_desc = -1;
	lfc_size_limit = 0;

	/* Invalidate hash */
	LWLockAcquire(lfc_lock, LW_EXCLUSIVE);

	hash_seq_init(&status, lfc_hash);
	while ((entry = hash_seq_search(&status)) != NULL)
	{
		hash_search(lfc_hash, &entry->key, HASH_REMOVE, NULL);
		memset(entry->bitmap, 0, sizeof entry->bitmap);
	}
	hash_seq_term(&status);
	lfc_ctl->generation += 1;
	lfc_ctl->size = 0;
	lfc_ctl->used = 0;
	dlist_init(&lfc_ctl->lru);

	LWLockRelease(lfc_lock);
}

static bool
lfc_ensure_opened(void)
{
	/* Open cache file if not done yet */
	if (lfc_desc <= 0)
	{
		lfc_desc = BasicOpenFile(lfc_path, O_RDWR|O_CREAT);

		if (lfc_desc < 0) {
			lfc_disable("open");
			return false;
		}
	}
	return true;
}

static void
lfc_shmem_startup(void)
{
	bool found;
	static HASHCTL info;

	if (prev_shmem_startup_hook)
	{
		prev_shmem_startup_hook();
	}

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	lfc_ctl = (FileCacheControl*)ShmemInitStruct("lfc", sizeof(FileCacheControl), &found);
	if (!found)
	{
		uint32 lfc_size = SIZE_MB_TO_CHUNKS(lfc_max_size);
		lfc_lock = (LWLockId)GetNamedLWLockTranche("lfc_lock");
		info.keysize = sizeof(BufferTag);
		info.entrysize = sizeof(FileCacheEntry);
		lfc_hash = ShmemInitHash("lfc_hash",
								 /* lfc_size+1 because we add new element to hash table before eviction of victim */
								 lfc_size+1, lfc_size+1,
								 &info,
								 HASH_ELEM | HASH_BLOBS);
		lfc_ctl->generation = 0;
		lfc_ctl->size = 0;
		lfc_ctl->used = 0;
		dlist_init(&lfc_ctl->lru);

		/* Remove file cache on restart */
		(void)unlink(lfc_path);
	}
	LWLockRelease(AddinShmemInitLock);
}

static void
lfc_shmem_request(void)
{
#if PG_VERSION_NUM>=150000
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();
#endif

	RequestAddinShmemSpace(sizeof(FileCacheControl) + hash_estimate_size(SIZE_MB_TO_CHUNKS(lfc_max_size)+1, sizeof(FileCacheEntry)));
	RequestNamedLWLockTranche("lfc_lock", 1);
}

static bool
lfc_check_limit_hook(int *newval, void **extra, GucSource source)
{
	if (*newval > lfc_max_size)
	{
		elog(ERROR, "neon.file_cache_size_limit can not be larger than neon.max_file_cache_size");
		return false;
	}
	return true;
}

static void
lfc_change_limit_hook(int newval, void *extra)
{
	uint32 new_size = SIZE_MB_TO_CHUNKS(newval);
	/*
	 * Stats collector detach shared memory, so we should not try to access shared memory here.
	 * Parallel workers first assign default value (0), so not perform truncation in parallel workers.
	 * The Postmaster can handle SIGHUP and it has access to shared memory (UsedShmemSegAddr != NULL), but has no PGPROC.
	 */
	if (!lfc_ctl || !MyProc || !UsedShmemSegAddr || IsParallelWorker())
		return;

	/* Open cache file if not done yet */
	if (lfc_desc <= 0)
	{
		lfc_desc = BasicOpenFile(lfc_path, O_RDWR|O_CREAT);
		if (lfc_desc < 0) {
			elog(WARNING, "Failed to open file cache %s: %m, disabling file cache", lfc_path);
			lfc_size_limit = 0; /* disable file cache */
			return;
		}
	}
	LWLockAcquire(lfc_lock, LW_EXCLUSIVE);
	while (new_size < lfc_ctl->used && !dlist_is_empty(&lfc_ctl->lru))
	{
		/* Shrink cache by throwing away least recently accessed chunks and returning their space to file system */
		FileCacheEntry* victim = dlist_container(FileCacheEntry, lru_node, dlist_pop_head_node(&lfc_ctl->lru));
		Assert(victim->access_count == 0);
#ifdef FALLOC_FL_PUNCH_HOLE
		if (fallocate(lfc_desc, FALLOC_FL_PUNCH_HOLE|FALLOC_FL_KEEP_SIZE, (off_t)victim->offset*BLOCKS_PER_CHUNK*BLCKSZ, BLOCKS_PER_CHUNK*BLCKSZ) < 0)
			elog(LOG, "Failed to punch hole in file: %m");
#endif
		hash_search(lfc_hash, &victim->key, HASH_REMOVE, NULL);
		lfc_ctl->used -= 1;
	}
	elog(DEBUG1, "set local file cache limit to %d", new_size);
	LWLockRelease(lfc_lock);
}

/*
 * Local file system state monitor check available free space.
 * If it is lower than lfc_free_space_watermark then we shrink size of local cache
 * but throwing away least recently accessed chunks.
 * First time low space watermark is reached cache size is divided by two,
 * second time by four,... Finally we remove all chunks from local cache.
 *
 * Please notice that we are not changing lfc_cache_size: it is used to be adjusted by autoscaler.
 * We only throw away cached chunks but do not prevent from filling cache by new chunks.
 *
 * Interval of poooling cache state is calculated as minimal time needed to consume lfc_free_space_watermark
 * disk space with maximal possible disk write speed (1Gb/sec). But not larger than 1 second.
 * Calling statvfs each second should not add any noticeable overhead.
 */
void
FileCacheMonitorMain(Datum main_arg)
{
	/*
	 * Choose file system state monitor interval so that space can not be exosted
	 * during this period but not longer than  MAX_MONITOR_INTERVAL (10 sec)
	 */
	uint64 monitor_interval = Min(MAX_MONITOR_INTERVAL_USEC, lfc_free_space_watermark*MB/MAX_DISK_WRITE_RATE);

	/* Establish signal handlers. */
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	BackgroundWorkerUnblockSignals();

	/* Periodically dump buffers until terminated. */
	while (!ShutdownRequestPending)
	{
		if (lfc_size_limit != 0)
		{
			struct statvfs sfs;
			if (statvfs(lfc_path, &sfs) < 0)
			{
				elog(WARNING, "Failed to obtain status of %s: %m", lfc_path);
			}
			else
			{
				if (sfs.f_bavail*sfs.f_bsize < lfc_free_space_watermark*MB)
				{
					if (lfc_shrinking_factor < 31) {
						lfc_shrinking_factor += 1;
					}
					lfc_change_limit_hook(lfc_size_limit >> lfc_shrinking_factor, NULL);
				}
				else
					lfc_shrinking_factor = 0; /* reset to initial value */
			}
		}
		pg_usleep(monitor_interval);
	}
}

static void
lfc_register_free_space_monitor(void)
{
	BackgroundWorker bgw;
	memset(&bgw, 0, sizeof(bgw));
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	snprintf(bgw.bgw_library_name, BGW_MAXLEN, "neon");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "FileCacheMonitorMain");
	snprintf(bgw.bgw_name, BGW_MAXLEN, "Local free space monitor");
	snprintf(bgw.bgw_type, BGW_MAXLEN, "Local free space monitor");
	bgw.bgw_restart_time = 5;
	bgw.bgw_notify_pid = 0;
	bgw.bgw_main_arg = (Datum) 0;

	RegisterBackgroundWorker(&bgw);
}

void
lfc_init(void)
{
	/*
	 * In order to create our shared memory area, we have to be loaded via
	 * shared_preload_libraries.
	 */
	if (!process_shared_preload_libraries_in_progress)
		elog(ERROR, "Neon module should be loaded via shared_preload_libraries");

	DefineCustomIntVariable("neon.max_file_cache_size",
							"Maximal size of Neon local file cache",
							NULL,
							&lfc_max_size,
							0, /* disabled by default */
							0,
							INT_MAX,
							PGC_POSTMASTER,
							GUC_UNIT_MB,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("neon.file_cache_size_limit",
							"Current limit for size of Neon local file cache",
							NULL,
							&lfc_size_limit,
							0, /* disabled by default */
							0,
							INT_MAX,
							PGC_SIGHUP,
							GUC_UNIT_MB,
							lfc_check_limit_hook,
							lfc_change_limit_hook,
							NULL);

	DefineCustomIntVariable("neon.free_space_watermark",
							"Minimal free space in local file system after reaching which local file cache will be truncated",
							NULL,
							&lfc_free_space_watermark,
							1024, /* 1GB */
							0,
							INT_MAX,
							PGC_SIGHUP,
							GUC_UNIT_MB,
							NULL,
							NULL,
							NULL);

	DefineCustomStringVariable("neon.file_cache_path",
							   "Path to local file cache (can be raw device)",
							   NULL,
							   &lfc_path,
							   "file.cache",
							   PGC_POSTMASTER,
							   0,
							   NULL,
							   NULL,
							   NULL);

	if (lfc_max_size == 0)
		return;

	if (lfc_free_space_watermark != 0)
		lfc_register_free_space_monitor();

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = lfc_shmem_startup;
#if PG_VERSION_NUM>=150000
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = lfc_shmem_request;
#else
	lfc_shmem_request();
#endif
}

/*
 * Check if page is present in the cache.
 * Returns true if page is found in local cache.
 */
bool
lfc_cache_contains(NRelFileInfo rinfo, ForkNumber forkNum, BlockNumber blkno)
{
	BufferTag tag;
	FileCacheEntry* entry;
	int chunk_offs = blkno & (BLOCKS_PER_CHUNK-1);
	bool found;
	uint32 hash;

	if (lfc_size_limit == 0) /* fast exit if file cache is disabled */
		return false;

	CopyNRelFileInfoToBufTag(tag, rinfo);
	tag.forkNum = forkNum;
	tag.blockNum = blkno & ~(BLOCKS_PER_CHUNK-1);
	hash = get_hash_value(lfc_hash, &tag);

	LWLockAcquire(lfc_lock, LW_SHARED);
	entry = hash_search_with_hash_value(lfc_hash, &tag, hash, HASH_FIND, NULL);
	found = entry != NULL && (entry->bitmap[chunk_offs >> 5] & (1 << (chunk_offs & 31))) != 0;
	LWLockRelease(lfc_lock);
	return found;
}

/*
 * Evict a page (if present) from the local file cache
 */
void
lfc_evict(NRelFileInfo rinfo, ForkNumber forkNum, BlockNumber blkno)
{
	BufferTag tag;
	FileCacheEntry* entry;
	bool found;
	int chunk_offs = blkno & (BLOCKS_PER_CHUNK-1);
	uint32 hash;

	if (lfc_size_limit == 0) /* fast exit if file cache is disabled */
		return;

	CopyNRelFileInfoToBufTag(tag, rinfo);
	tag.forkNum = forkNum;
	tag.blockNum = (blkno & ~(BLOCKS_PER_CHUNK - 1));

	hash = get_hash_value(lfc_hash, &tag);

	LWLockAcquire(lfc_lock, LW_EXCLUSIVE);
	entry = hash_search_with_hash_value(lfc_hash, &tag, hash, HASH_FIND, &found);

	if (!found)
	{
		/* nothing to do */
		LWLockRelease(lfc_lock);
		return;
	}

	/* remove the page from the cache */
	entry->bitmap[chunk_offs >> 5] &= ~(1 << (chunk_offs & (32 - 1)));

	/*
	 * If the chunk has no live entries, we can position the chunk to be
	 * recycled first.
	 */
	if (entry->bitmap[chunk_offs >> 5] == 0)
	{
		bool has_remaining_pages;

		for (int i = 0; i < (BLOCKS_PER_CHUNK / 32); i++) {
			if (entry->bitmap[i] != 0)
			{
				has_remaining_pages = true;
				break;
			}
		}

		/*
		 * Put the entry at the position that is first to be reclaimed when
		 * we have no cached pages remaining in the chunk
		 */
		if (!has_remaining_pages)
		{
			dlist_delete(&entry->lru_node);
			dlist_push_head(&lfc_ctl->lru, &entry->lru_node);
		}
	}

	/*
	 * Done: apart from empty chunks, we don't move chunks in the LRU when
	 * they're empty because eviction isn't usage.
	 */

	LWLockRelease(lfc_lock);
}

/*
 * Try to read page from local cache.
 * Returns true if page is found in local cache.
 * In case of error lfc_size_limit is set to zero to disable any further opera-tins with cache.
 */
bool
lfc_read(NRelFileInfo rinfo, ForkNumber forkNum, BlockNumber blkno,
		 char *buffer)
{
	BufferTag tag;
	FileCacheEntry* entry;
	ssize_t rc;
	int chunk_offs = blkno & (BLOCKS_PER_CHUNK-1);
	bool result = true;
	uint32 hash;
	uint64 generation;
	uint32 entry_offset;

	if (lfc_size_limit == 0) /* fast exit if file cache is disabled */
		return false;

	if (!lfc_ensure_opened())
		return false;

	CopyNRelFileInfoToBufTag(tag, rinfo);
	tag.forkNum = forkNum;
	tag.blockNum = blkno & ~(BLOCKS_PER_CHUNK-1);
	hash = get_hash_value(lfc_hash, &tag);

	LWLockAcquire(lfc_lock, LW_EXCLUSIVE);
	entry = hash_search_with_hash_value(lfc_hash, &tag, hash, HASH_FIND, NULL);
	if (entry == NULL || (entry->bitmap[chunk_offs >> 5] & (1 << (chunk_offs & 31))) == 0)
	{
		/* Page is not cached */
		LWLockRelease(lfc_lock);
		return false;
	}
	/* Unlink entry from LRU list to pin it for the duration of IO operation */
	if (entry->access_count++ == 0)
		dlist_delete(&entry->lru_node);
	generation = lfc_ctl->generation;
	entry_offset = entry->offset;

	LWLockRelease(lfc_lock);

	rc = pread(lfc_desc, buffer, BLCKSZ, ((off_t)entry_offset*BLOCKS_PER_CHUNK + chunk_offs)*BLCKSZ);
	if (rc != BLCKSZ)
	{
		lfc_disable("read");
		return false;
	}

	/* Place entry to the head of LRU list */
	LWLockAcquire(lfc_lock, LW_EXCLUSIVE);
	if (lfc_ctl->generation == generation)
	{
		Assert(entry->access_count > 0);
		if (--entry->access_count == 0)
			dlist_push_tail(&lfc_ctl->lru, &entry->lru_node);
	}
	else
		result = false;

	LWLockRelease(lfc_lock);

	return result;
}

/*
 * Put page in local file cache.
 * If cache is full then evict some other page.
 */
void
lfc_write(NRelFileInfo rinfo, ForkNumber forkNum, BlockNumber blkno,
#if PG_MAJORVERSION_NUM < 16
		  char *buffer)
#else
		  const void *buffer)
#endif
{
	BufferTag tag;
	FileCacheEntry* entry;
	ssize_t rc;
	bool found;
	int chunk_offs = blkno & (BLOCKS_PER_CHUNK-1);
	uint32 hash;

	if (lfc_size_limit == 0) /* fast exit if file cache is disabled */
		return;

	if (!lfc_ensure_opened())
		return;

	tag.forkNum = forkNum;
	tag.blockNum = blkno & ~(BLOCKS_PER_CHUNK-1);
	
	CopyNRelFileInfoToBufTag(tag, rinfo);
	
	hash = get_hash_value(lfc_hash, &tag);

	LWLockAcquire(lfc_lock, LW_EXCLUSIVE);
	entry = hash_search_with_hash_value(lfc_hash, &tag, hash, HASH_ENTER, &found);

	if (found)
	{
		/* Unlink entry from LRU list to pin it for the duration of IO operation */
		if (entry->access_count++ == 0)
			dlist_delete(&entry->lru_node);
	}
	else
	{
		/*
		 * We have two choices if all cache pages are pinned (i.e. used in IO operations):
		 * 1. Wait until some of this operation is completed and pages is unpinned
		 * 2. Allocate one more chunk, so that specified cache size is more recommendation than hard limit.
		 * As far as probability of such event (that all pages are pinned) is considered to be very very small:
		 * there are should be very large number of concurrent IO operations and them are limited by max_connections,
		 * we prefer not to complicate code and use second approach.
		 */
		if (lfc_ctl->used >= SIZE_MB_TO_CHUNKS(lfc_size_limit) && !dlist_is_empty(&lfc_ctl->lru))
		{
			/* Cache overflow: evict least recently used chunk */
			FileCacheEntry* victim = dlist_container(FileCacheEntry, lru_node, dlist_pop_head_node(&lfc_ctl->lru));
			Assert(victim->access_count == 0);
			entry->offset = victim->offset; /* grab victim's chunk */
			hash_search(lfc_hash, &victim->key, HASH_REMOVE, NULL);
			elog(DEBUG2, "Swap file cache page");
		}
		else
		{
			lfc_ctl->used += 1;
			entry->offset = lfc_ctl->size++; /* allocate new chunk at end of file */
		}
		entry->access_count = 1;
		memset(entry->bitmap, 0, sizeof entry->bitmap);
	}

	rc = pwrite(lfc_desc, buffer, BLCKSZ, ((off_t)entry->offset*BLOCKS_PER_CHUNK + chunk_offs)*BLCKSZ);
	if (rc != BLCKSZ)
	{
		LWLockRelease(lfc_lock);
		lfc_disable("write");
	}
	else
	{
		/* Place entry to the head of LRU list */
		Assert(entry->access_count > 0);
		if (--entry->access_count == 0)
			dlist_push_tail(&lfc_ctl->lru, &entry->lru_node);

		entry->bitmap[chunk_offs >> 5] |= (1 << (chunk_offs & 31));
		LWLockRelease(lfc_lock);
	}
}

/*
 * Record structure holding the to be exposed cache data.
 */
typedef struct
{
	uint32		pageoffs;
	Oid			relfilenode;
	Oid			reltablespace;
	Oid			reldatabase;
	ForkNumber	forknum;
	BlockNumber blocknum;
	uint16		accesscount;
} LocalCachePagesRec;

/*
 * Function context for data persisting over repeated calls.
 */
typedef struct
{
	TupleDesc	tupdesc;
	LocalCachePagesRec *record;
} LocalCachePagesContext;

/*
 * Function returning data from the local file cache
 * relation node/tablespace/database/blocknum and access_counter
 */
PG_FUNCTION_INFO_V1(local_cache_pages);

#define NUM_LOCALCACHE_PAGES_ELEM	7

Datum
local_cache_pages(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	Datum		result;
	MemoryContext oldcontext;
	LocalCachePagesContext *fctx;	/* User function context. */
	TupleDesc	tupledesc;
	TupleDesc	expected_tupledesc;
	HeapTuple	tuple;

	if (SRF_IS_FIRSTCALL())
	{
        HASH_SEQ_STATUS status;
		FileCacheEntry* entry;
		uint32 n_pages = 0;

		funcctx = SRF_FIRSTCALL_INIT();

		/* Switch context when allocating stuff to be used in later calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* Create a user function context for cross-call persistence */
		fctx = (LocalCachePagesContext *) palloc(sizeof(LocalCachePagesContext));

		/*
		 * To smoothly support upgrades from version 1.0 of this extension
		 * transparently handle the (non-)existence of the pinning_backends
		 * column. We unfortunately have to get the result type for that... -
		 * we can't use the result type determined by the function definition
		 * without potentially crashing when somebody uses the old (or even
		 * wrong) function definition though.
		 */
		if (get_call_result_type(fcinfo, NULL, &expected_tupledesc) != TYPEFUNC_COMPOSITE)
			elog(ERROR, "return type must be a row type");

		if (expected_tupledesc->natts != NUM_LOCALCACHE_PAGES_ELEM)
			elog(ERROR, "incorrect number of output arguments");

		/* Construct a tuple descriptor for the result rows. */
		tupledesc = CreateTemplateTupleDesc(expected_tupledesc->natts);
		TupleDescInitEntry(tupledesc, (AttrNumber) 1, "pageoffs",
						   INT8OID, -1, 0);
#if PG_MAJORVERSION_NUM < 16
		TupleDescInitEntry(tupledesc, (AttrNumber) 2, "relfilenode",
						   OIDOID, -1, 0);
#else
		TupleDescInitEntry(tupledesc, (AttrNumber) 2, "relfilenumber",
						   OIDOID, -1, 0);
#endif
		TupleDescInitEntry(tupledesc, (AttrNumber) 3, "reltablespace",
						   OIDOID, -1, 0);
		TupleDescInitEntry(tupledesc, (AttrNumber) 4, "reldatabase",
						   OIDOID, -1, 0);
		TupleDescInitEntry(tupledesc, (AttrNumber) 5, "relforknumber",
						   INT2OID, -1, 0);
		TupleDescInitEntry(tupledesc, (AttrNumber) 6, "relblocknumber",
						   INT8OID, -1, 0);
		TupleDescInitEntry(tupledesc, (AttrNumber) 7, "accesscount",
						   INT4OID, -1, 0);

		fctx->tupdesc = BlessTupleDesc(tupledesc);

		LWLockAcquire(lfc_lock, LW_SHARED);

        hash_seq_init(&status, lfc_hash);
        while ((entry = hash_seq_search(&status)) != NULL)
		{
			for (int i = 0; i < BLOCKS_PER_CHUNK; i++)
				n_pages += (entry->bitmap[i >> 5] & (1 << (i & 31))) != 0;
		}
		hash_seq_term(&status);
		fctx->record = (LocalCachePagesRec *)
			MemoryContextAllocHuge(CurrentMemoryContext,
								   sizeof(LocalCachePagesRec) * n_pages);

		/* Set max calls and remember the user function context. */
		funcctx->max_calls = n_pages;
		funcctx->user_fctx = fctx;

		/* Return to original context when allocating transient memory */
		MemoryContextSwitchTo(oldcontext);

		/*
		 * Scan through all the buffers, saving the relevant fields in the
		 * fctx->record structure.
		 *
		 * We don't hold the partition locks, so we don't get a consistent
		 * snapshot across all buffers, but we do grab the buffer header
		 * locks, so the information of each buffer is self-consistent.
		 */
		n_pages = 0;
        hash_seq_init(&status, lfc_hash);
        while ((entry = hash_seq_search(&status)) != NULL)
		{
			for (int i = 0; i < BLOCKS_PER_CHUNK; i++)
			{
				if (entry->bitmap[i >> 5] & (1 << (i & 31)))
				{
					fctx->record[n_pages].pageoffs = entry->offset*BLOCKS_PER_CHUNK + i;
					fctx->record[n_pages].relfilenode = NInfoGetRelNumber(BufTagGetNRelFileInfo(entry->key));
					fctx->record[n_pages].reltablespace = NInfoGetSpcOid(BufTagGetNRelFileInfo(entry->key));
					fctx->record[n_pages].reldatabase = NInfoGetDbOid(BufTagGetNRelFileInfo(entry->key));
					fctx->record[n_pages].forknum = entry->key.forkNum;
					fctx->record[n_pages].blocknum = entry->key.blockNum + i;
					fctx->record[n_pages].accesscount = entry->access_count;
					n_pages += 1;
				}
			}
		}
		hash_seq_term(&status);
		Assert(n_pages == funcctx->max_calls);
		LWLockRelease(lfc_lock);
	}

	funcctx = SRF_PERCALL_SETUP();

	/* Get the saved state */
	fctx = funcctx->user_fctx;

	if (funcctx->call_cntr < funcctx->max_calls)
	{
		uint32		i = funcctx->call_cntr;
		Datum		values[NUM_LOCALCACHE_PAGES_ELEM];
		bool		nulls[NUM_LOCALCACHE_PAGES_ELEM] = {
			false, false, false, false, false, false, false
		};

		values[0] = Int64GetDatum((int64) fctx->record[i].pageoffs);
		values[1] = ObjectIdGetDatum(fctx->record[i].relfilenode);
		values[2] = ObjectIdGetDatum(fctx->record[i].reltablespace);
		values[3] = ObjectIdGetDatum(fctx->record[i].reldatabase);
		values[4] = ObjectIdGetDatum(fctx->record[i].forknum);
		values[5] = Int64GetDatum((int64) fctx->record[i].blocknum);
		values[6] = Int32GetDatum(fctx->record[i].accesscount);

		/* Build and return the tuple. */
		tuple = heap_form_tuple(fctx->tupdesc, values, nulls);
		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);
	}
	else
		SRF_RETURN_DONE(funcctx);
}
