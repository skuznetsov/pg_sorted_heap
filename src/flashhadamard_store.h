/*
 * flashhadamard_store.h — Page-native segment store for FlashHadamard
 *
 * Layout:
 *   Page 0: Meta page (params, dimensions, offsets)
 *   Pages 1..P: Packed code pages (transposed byte columns, contiguous)
 *   Pages P+1..Q: SQ8 payload pages (row-major, fetched on demand)
 *   Pages Q+1..R: Norm pages (float32 per row)
 */

#ifndef FLASHHADAMARD_STORE_H
#define FLASHHADAMARD_STORE_H

#include "postgres.h"
#include "storage/bufmgr.h"

#define FH_STORE_MAGIC      0x46484D31  /* "FHM1" */
#define FH_META_BLKNO       0

/* Usable bytes per page (after page header + opaque) */
#define FH_PAGE_USABLE      (BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - sizeof(uint32))

/* Max Lloyd-Max centers (4-bit = 16) */
#define FH_MAX_CENTERS      16
#define FH_MAX_GROUPS       512  /* dim/group_size, max 8192/16=512 */

/* Meta page: stored in page 0 */
typedef struct FHMetaPageData
{
    uint32      magic;
    int32       version;        /* 1 */
    int32       dim;
    int32       n_rows;
    int32       n_bytes;        /* (dim+1)/2 */
    int32       group_size;
    int32       n_groups;
    int32       seed;
    BlockNumber packed_start;   /* first page of packed codes */
    int32       packed_npages;
    BlockNumber sq8_start;      /* first page of SQ8 payload */
    int32       sq8_npages;
    BlockNumber norm_start;     /* first page of norms */
    int32       norm_npages;
    float       centers[FH_MAX_CENTERS];
    float       group_scales[FH_MAX_GROUPS];
    float       sq8_mins[0];    /* variable-length: [dim] floats after group_scales */
    /* sq8_scales[dim] follows sq8_mins */
} FHMetaPageData;

/* Opaque data at end of each data page */
typedef struct FHPageOpaqueData
{
    uint32      fh_flags;       /* reserved */
} FHPageOpaqueData;

/* Size check: meta must fit in one page */
#define FH_META_FIXED_SIZE  (offsetof(FHMetaPageData, sq8_mins))
/* sq8_mins[dim] + sq8_scales[dim] = 8 * dim bytes */
/* For dim=2880: 8*2880 = 23040 bytes. Doesn't fit in one 8K page! */
/* Solution: store sq8 params in overflow pages or split meta. */

/* Actually: for high dim, store sq8_mins/scales in their own pages. */
/* Keep meta page small: just fixed params + group_scales. */

typedef struct FHMetaPageDataV2
{
    uint32      magic;
    int32       version;
    int32       dim;
    int32       n_rows;
    int32       n_bytes;
    int32       group_size;
    int32       n_groups;
    int32       seed;
    /* Byte offsets within the store file for raw contiguous sections */
    int64       off_sq8_params;     /* sq8_mins[dim] + sq8_scales[dim] */
    int64       off_packed;         /* packed_t[n_bytes * n_rows] */
    int64       off_sq8;            /* sq8_codes[n_rows * dim] */
    int64       off_norms;          /* norms[n_rows] */
    int64       off_end;            /* total file size */
    float       centers[FH_MAX_CENTERS];
    float       group_scales[FH_MAX_GROUPS];
} FHMetaPageDataV2;

/* Total fixed meta size: ~2112 bytes for FH_MAX_GROUPS=512. Fits in 8K page. */

/* File page header for raw page I/O */
typedef struct FHFilePageHeader
{
    uint32  magic;
    uint16  page_type;   /* 0=meta, 1=sq8params, 2=packed, 3=sq8, 4=norms */
    uint16  reserved;
    int32   offset;
    int32   length;
} FHFilePageHeader;

#define FH_DATA_PER_PAGE  (BLCKSZ - (int)sizeof(FHFilePageHeader))

/* Store API */
extern int fh_store_write(const char *path,
                           const FHMetaPageDataV2 *meta,
                           const float *sq8_mins, const float *sq8_scales,
                           const uint8 *packed_t, Size packed_t_size,
                           const uint8 *sq8_codes, Size sq8_size,
                           const float *norms, int n_rows);

extern int fh_store_scan(const char *path, const FHParams *params,
                          const float *query_vec, int k, int shortlist_m,
                          int32 *out_ids, float *out_scores);

#endif /* FLASHHADAMARD_STORE_H */
