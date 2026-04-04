/*
 * flashhadamard_store.c — Page-native segment store for FlashHadamard
 *
 * Uses a dedicated file (via smgr/fd.h) to store packed codes and SQ8
 * payload in sequential pages. No TOAST, no SPI in hot path.
 *
 * File layout:
 *   - Header page (meta: dim, seed, group_scales, centers, offsets)
 *   - SQ8 params pages (sq8_mins + sq8_scales)
 *   - Packed code pages (transposed byte columns, sequential)
 *   - SQ8 payload pages (row-major, read on demand for rerank)
 *   - Norm pages (float32 per row)
 */

#include "flashhadamard.h"
#include "flashhadamard_store.h"

#include <math.h>
#include <string.h>
#include <float.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>

/* Simple file-based store: write/read raw pages to a file in PG data dir.
 * This avoids buffer manager complexity for the first AM-lite prototype.
 * Pages are BLCKSZ-aligned for compatibility with future buffer mgr use. */

#define FH_PAGE_SIZE  BLCKSZ  /* 8192 bytes */

/* Backend-local store cache instance */
FHStoreCache fh_cache = { .path = "", .fd = -1, .mapped = MAP_FAILED };

FHStoreCache *
fh_store_cache_get(const char *path)
{
    /* Return cached if same path */
    if (fh_cache.fd >= 0 && strcmp(fh_cache.path, path) == 0 && fh_cache.mapped != MAP_FAILED)
        return &fh_cache;

    /* Close old cache */
    if (fh_cache.mapped != MAP_FAILED)
    { munmap(fh_cache.mapped, fh_cache.mapped_size); fh_cache.mapped = MAP_FAILED; }
    if (fh_cache.fd >= 0)
    { close(fh_cache.fd); fh_cache.fd = -1; }

    /* Open new */
    fh_cache.fd = open(path, O_RDONLY);
    if (fh_cache.fd < 0)
        return NULL;

    {
        struct stat st;
        char meta_page[FH_PAGE_SIZE];
        FHMetaPageDataV2 *m;

        if (fstat(fh_cache.fd, &st) < 0 || st.st_size < FH_PAGE_SIZE)
        { close(fh_cache.fd); fh_cache.fd = -1; return NULL; }

        fh_cache.mapped_size = st.st_size;
        fh_cache.mapped = mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, fh_cache.fd, 0);
        if (fh_cache.mapped == MAP_FAILED)
        { close(fh_cache.fd); fh_cache.fd = -1; return NULL; }

        /* Parse meta from mapped region */
        m = (FHMetaPageDataV2 *)((char *)fh_cache.mapped + sizeof(FHFilePageHeader));
        if (m->magic != FH_STORE_MAGIC)
        { munmap(fh_cache.mapped, fh_cache.mapped_size); fh_cache.mapped = MAP_FAILED; close(fh_cache.fd); fh_cache.fd = -1; return NULL; }

        memcpy(&fh_cache.meta, m, sizeof(FHMetaPageDataV2));
        strncpy(fh_cache.path, path, sizeof(fh_cache.path) - 1);

        /* Cache SQ8 params pointers into mmap'd region */
        fh_cache.sq8_mins = (float *)((char *)fh_cache.mapped + fh_cache.meta.off_sq8_params);
        fh_cache.sq8_scales = fh_cache.sq8_mins + fh_cache.meta.dim;
    }

    return &fh_cache;
}

/* Write raw contiguous bytes (no page framing) and return bytes written */
static Size
fh_write_raw(int fd, const void *data, Size total_bytes)
{
    Size written = 0;
    while (written < total_bytes)
    {
        ssize_t n = write(fd, (const char *)data + written,
                          Min(total_bytes - written, (Size)64 * 1024 * 1024));
        if (n <= 0)
            return written;
        written += n;
    }
    return written;
}

/* Read raw contiguous bytes */
static Size
fh_read_raw(int fd, off_t offset, void *out, Size total_bytes)
{
    Size got = 0;
    lseek(fd, offset, SEEK_SET);
    while (got < total_bytes)
    {
        ssize_t n = read(fd, (char *)out + got,
                         Min(total_bytes - got, (Size)64 * 1024 * 1024));
        if (n <= 0)
            return got;
        got += n;
    }
    return got;
}

/* Write raw bytes to sequential pages in a file (legacy, kept for meta page) */
static int
fh_write_section(int fd, const void *data, Size total_bytes, int *page_count)
{
    Size    written = 0;
    int     pages = 0;
    char    page_buf[FH_PAGE_SIZE];

    while (written < total_bytes)
    {
        FHFilePageHeader *hdr = (FHFilePageHeader *)page_buf;
        Size chunk = Min(FH_DATA_PER_PAGE, total_bytes - written);

        memset(page_buf, 0, FH_PAGE_SIZE);
        hdr->magic = FH_STORE_MAGIC;
        hdr->page_type = 0;  /* set by caller */
        hdr->offset = (int32)written;
        hdr->length = (int32)chunk;
        memcpy(page_buf + sizeof(FHFilePageHeader), (const char *)data + written, chunk);

        if (write(fd, page_buf, FH_PAGE_SIZE) != FH_PAGE_SIZE)
            return -1;

        written += chunk;
        pages++;
    }
    *page_count = pages;
    return 0;
}

/* Read a section from sequential pages */
static int
fh_read_section(int fd, off_t start_offset, void *out, Size total_bytes)
{
    Size    read_so_far = 0;
    char    page_buf[FH_PAGE_SIZE];

    if (lseek(fd, start_offset, SEEK_SET) < 0)
        return -1;

    while (read_so_far < total_bytes)
    {
        FHFilePageHeader *hdr = (FHFilePageHeader *)page_buf;
        Size chunk;

        if (read(fd, page_buf, FH_PAGE_SIZE) != FH_PAGE_SIZE)
            return -1;

        chunk = Min((Size)hdr->length, total_bytes - read_so_far);
        memcpy((char *)out + read_so_far, page_buf + sizeof(FHFilePageHeader), chunk);
        read_so_far += chunk;
    }
    return 0;
}

/* ================================================================
 * Build: write segment store file
 * ================================================================ */

int
fh_store_write(const char *path,
               const FHMetaPageDataV2 *meta,
               const float *sq8_mins, const float *sq8_scales,
               const uint8 *packed_t, Size packed_t_size,
               const uint8 *sq8_codes, Size sq8_size,
               const float *norms, int n_rows,
               const float *centroids, int n_segments)
{
    int     fd;
    char    meta_page[FH_PAGE_SIZE];
    int     sq8_params_pages = 0, packed_pages = 0, sq8_pages = 0, norm_pages = 0;
    off_t   sq8_params_offset, packed_offset, sq8_data_offset, norm_offset, centroid_offset;

    fd = open(path, O_CREAT | O_TRUNC | O_WRONLY, 0600);
    if (fd < 0)
        return -1;

    /* Page 0: meta (fixed size, fits in one page) */
    memset(meta_page, 0, FH_PAGE_SIZE);
    {
        FHFilePageHeader *hdr = (FHFilePageHeader *)meta_page;
        hdr->magic = FH_STORE_MAGIC;
        hdr->page_type = 0;
        hdr->length = FH_META_FIXED_SIZE;  /* just the fixed part */
    }
    memcpy(meta_page + sizeof(FHFilePageHeader), meta,
           Min(FH_META_FIXED_SIZE, FH_DATA_PER_PAGE));
    if (write(fd, meta_page, FH_PAGE_SIZE) != FH_PAGE_SIZE)
    { close(fd); return -1; }

    /* Raw contiguous sections (no page framing for bulk data) */
    /* Layout after meta page: [sq8_params | packed_t | sq8_codes | norms] */
    {
        Size sq8_params_size = sizeof(float) * meta->dim * 2;
        float *params_buf = malloc(sq8_params_size);
        off_t section_pos;
        FHMetaPageDataV2 *m;

        if (!params_buf) { close(fd); return -1; }
        memcpy(params_buf, sq8_mins, sizeof(float) * meta->dim);
        memcpy(params_buf + meta->dim, sq8_scales, sizeof(float) * meta->dim);

        /* Section offsets stored as byte positions from file start */
        sq8_params_offset = FH_PAGE_SIZE;  /* right after meta page */
        fh_write_raw(fd, params_buf, sq8_params_size);
        free(params_buf);

        packed_offset = sq8_params_offset + (off_t)sq8_params_size;
        fh_write_raw(fd, packed_t, packed_t_size);

        sq8_data_offset = packed_offset + (off_t)packed_t_size;
        fh_write_raw(fd, sq8_codes, sq8_size);

        norm_offset = sq8_data_offset + (off_t)sq8_size;
        fh_write_raw(fd, norms, sizeof(float) * n_rows);

        centroid_offset = norm_offset + (off_t)(sizeof(float) * n_rows);
        if (centroids && n_segments > 0)
            fh_write_raw(fd, centroids, sizeof(float) * (Size)n_segments * meta->dim);

        /* Rewrite meta page with exact byte offsets */
        m = (FHMetaPageDataV2 *)(meta_page + sizeof(FHFilePageHeader));
        m->off_sq8_params = sq8_params_offset;
        m->off_packed = packed_offset;
        m->off_sq8 = sq8_data_offset;
        m->off_norms = norm_offset;
        m->off_centroids = centroid_offset;
        m->n_segments = n_segments;
        m->segment_size = FH_SEGMENT_SIZE;
        m->off_end = centroid_offset + (centroids ? sizeof(float) * (Size)n_segments * meta->dim : 0);

        lseek(fd, 0, SEEK_SET);
        write(fd, meta_page, FH_PAGE_SIZE);
    }

    close(fd);
    return 0;
}

/* ================================================================
 * Scan: read segment store and score
 * ================================================================ */

int
fh_store_scan(const char *path, const FHParams *params,
              const float *query_vec, int k, int shortlist_m,
              int32 *out_ids, float *out_scores)
{
    FHStoreCache *cache;
    FHMetaPageDataV2 *meta;
    int     dim, n_rows, n_bytes;
    float  *byte_tables;
    float  *coeffs;
    float  *rotated_q;
    int32  *top_ids;
    float  *top_scores;
    int     filled = 0;
    int     i, j;
    float   inv_sqrt_dim;
    uint8  *packed_ptr;
    float  *norms_ptr;

    /* Get or create backend-local cache (persistent mmap) */
    cache = fh_store_cache_get(path);
    if (!cache)
        return -1;

    meta = &cache->meta;
    dim = meta->dim;
    n_rows = meta->n_rows;
    n_bytes = meta->n_bytes;
    inv_sqrt_dim = 1.0f / sqrtf((float)dim);

    /* Rotate query + build byte tables */
    rotated_q = palloc(sizeof(float) * dim);
    fh_rotate_vec(query_vec, rotated_q, params);

    coeffs = palloc(sizeof(float) * dim);
    {
        int g;
        for (g = 0; g < params->n_groups; g++)
        {
            int gstart = g * params->group_size;
            int gend = Min(gstart + params->group_size, dim);
            for (j = gstart; j < gend; j++)
                coeffs[j] = rotated_q[j] * params->group_scales[g] * inv_sqrt_dim;
        }
    }
    pfree(rotated_q);

    byte_tables = palloc(sizeof(float) * n_bytes * 256);
    fh_build_byte_tables(coeffs, params->centers, dim, byte_tables);
    pfree(coeffs);

    /* Stage 1: Score with optional segment pruning */
    top_ids = palloc(sizeof(int32) * shortlist_m);
    top_scores = palloc(sizeof(float) * shortlist_m);

    packed_ptr = (uint8 *)cache->mapped + meta->off_packed;
    norms_ptr = (float *)((char *)cache->mapped + meta->off_norms);

    if (meta->n_segments > 1 && meta->off_centroids > 0)
    {
        /* Segment pruning: rank segments by centroid similarity, scan top-P */
        float  *centroids_ptr = (float *)((char *)cache->mapped + meta->off_centroids);
        int     n_seg = meta->n_segments;
        int     seg_size = meta->segment_size;
        float  *seg_scores;
        int    *seg_order;
        int     n_probe;
        int     s;

        /* Normalize query for centroid comparison */
        float  *q_norm_seg = palloc(sizeof(float) * dim);
        {
            float qn = 0;
            for (i = 0; i < dim; i++) qn += query_vec[i] * query_vec[i];
            qn = sqrtf(qn);
            if (qn < 1e-12f) qn = 1e-12f;
            for (i = 0; i < dim; i++) q_norm_seg[i] = query_vec[i] / qn;
        }

        /* Compute dot product with each centroid */
        seg_scores = palloc(sizeof(float) * n_seg);
        for (s = 0; s < n_seg; s++)
        {
            float *cen = centroids_ptr + (Size)s * dim;
            float dot = 0;
            for (j = 0; j < dim; j++) dot += q_norm_seg[j] * cen[j];
            seg_scores[s] = dot;
        }
        pfree(q_norm_seg);

        /* Sort segments by descending score */
        seg_order = palloc(sizeof(int) * n_seg);
        for (s = 0; s < n_seg; s++) seg_order[s] = s;
        for (i = 0; i < n_seg; i++)
            for (j = i + 1; j < n_seg; j++)
                if (seg_scores[seg_order[j]] > seg_scores[seg_order[i]])
                { int tmp = seg_order[i]; seg_order[i] = seg_order[j]; seg_order[j] = tmp; }

        /* Probe top-P segments (default: half, min all if few segments) */
        n_probe = (n_seg + 1) / 2;
        if (n_probe < 3) n_probe = n_seg;  /* don't prune if very few segments */

        /* Score only probed segments using parallel scorer */
        for (s = 0; s < n_probe; s++)
        {
            int sid = seg_order[s];
            int rstart = sid * seg_size;
            int rcount = Min(seg_size, n_rows - rstart);
            /* Create virtual packed_t pointer for this segment */
            /* packed_t is transposed: packed_t[col * n_rows + row] */
            /* For a segment [rstart..rstart+rcount), we need offsets within each column */
            /* Pass the full packed_t + n_rows, and let scorer handle the range */
            /* Actually: the parallel scorer already shards by row range.
             * We need to call score_topk on the segment's row range.
             * But fh_packed_score_topk_t scores ALL rows. Need a range variant. */

            /* Simple: score this segment's rows directly (single-thread per segment) */
            {
                float *seg_row_scores = palloc(sizeof(float) * rcount);
                int byte_idx, row;
                int min_pos = 0;
                float min_score_local = -FLT_MAX;

                memset(seg_row_scores, 0, sizeof(float) * rcount);

                for (byte_idx = 0; byte_idx + 1 < n_bytes; byte_idx += 2)
                {
                    const uint8 *codes0 = packed_ptr + (Size)byte_idx * n_rows + rstart;
                    const uint8 *codes1 = packed_ptr + (Size)(byte_idx + 1) * n_rows + rstart;
                    const float *table0 = byte_tables + byte_idx * 256;
                    const float *table1 = byte_tables + (byte_idx + 1) * 256;

                    for (row = 0; row + 3 < rcount; row += 4)
                    {
                        seg_row_scores[row+0] += table0[codes0[row+0]] + table1[codes1[row+0]];
                        seg_row_scores[row+1] += table0[codes0[row+1]] + table1[codes1[row+1]];
                        seg_row_scores[row+2] += table0[codes0[row+2]] + table1[codes1[row+2]];
                        seg_row_scores[row+3] += table0[codes0[row+3]] + table1[codes1[row+3]];
                    }
                    for (; row < rcount; row++)
                        seg_row_scores[row] += table0[codes0[row]] + table1[codes1[row]];
                }
                if (byte_idx < n_bytes)
                {
                    const uint8 *codes = packed_ptr + (Size)byte_idx * n_rows + rstart;
                    const float *table = byte_tables + byte_idx * 256;
                    for (row = 0; row < rcount; row++)
                        seg_row_scores[row] += table[codes[row]];
                }

                /* Merge into global top-k */
                for (row = 0; row < rcount; row++)
                {
                    float sc = norms_ptr ? seg_row_scores[row] * norms_ptr[rstart + row]
                                          : seg_row_scores[row];
                    fh_topk_insert(sc, rstart + row, top_scores, top_ids, shortlist_m,
                                    &filled, &min_pos, &min_score_local);
                }
                pfree(seg_row_scores);
            }
        }

        pfree(seg_scores);
        pfree(seg_order);
    }
    else
    {
        /* No pruning: exhaustive parallel scan */
        fh_packed_score_topk_t(packed_ptr, byte_tables, norms_ptr,
                                n_rows, n_bytes, shortlist_m,
                                top_ids, top_scores, &filled);
    }
    pfree(byte_tables);

    /* Stage 2: SQ8 rerank using cached params + mmap'd SQ8 payload */
    if (shortlist_m > k && filled > k)
    {
        float  *q_norm;
        int32  *final_ids = palloc(sizeof(int32) * k);
        float  *final_scores = palloc(sizeof(float) * k);
        int     final_filled = 0, final_min_pos = 0;
        float   final_min_score = -FLT_MAX;

        /* SQ8 params from cache (already points into mmap) */
        float *sq8_mins_local = cache->sq8_mins;
        float *sq8_scales_local = cache->sq8_scales;

        /* SQ8 payload pointer (mmap'd) */
        uint8 *sq8_base = (uint8 *)cache->mapped + meta->off_sq8;

        q_norm = palloc(sizeof(float) * dim);
        {
            float norm = 0.0f;
            for (i = 0; i < dim; i++) norm += query_vec[i] * query_vec[i];
            norm = sqrtf(norm);
            if (norm < 1e-12f) norm = 1e-12f;
            for (i = 0; i < dim; i++) q_norm[i] = query_vec[i] / norm;
        }

        for (i = 0; i < filled; i++)
        {
            int row = top_ids[i];
            uint8 *row_codes = sq8_base + (Size)row * dim;
            float dot = 0.0f;

            for (j = 0; j < dim; j++)
            {
                float decoded = (float)row_codes[j] * sq8_scales_local[j] + sq8_mins_local[j];
                dot += decoded * q_norm[j];
            }

            fh_topk_insert(dot, row, final_scores, final_ids, k,
                         &final_filled, &final_min_pos, &final_min_score);
        }

        pfree(q_norm);

        /* Sort */
        for (i = 0; i < final_filled; i++)
            for (j = i + 1; j < final_filled; j++)
                if (final_scores[j] > final_scores[i])
                {
                    float ts = final_scores[i]; final_scores[i] = final_scores[j]; final_scores[j] = ts;
                    int32 ti = final_ids[i]; final_ids[i] = final_ids[j]; final_ids[j] = ti;
                }

        memcpy(out_ids, final_ids, sizeof(int32) * Min(final_filled, k));
        memcpy(out_scores, final_scores, sizeof(float) * Min(final_filled, k));
        pfree(final_ids); pfree(final_scores);
        pfree(top_ids); pfree(top_scores);
        return Min(final_filled, k);
    }

    /* No rerank */
    {
        int out_k = Min(filled, k);
        for (i = 0; i < filled; i++)
            for (j = i + 1; j < filled; j++)
                if (top_scores[j] > top_scores[i])
                {
                    float ts = top_scores[i]; top_scores[i] = top_scores[j]; top_scores[j] = ts;
                    int32 ti = top_ids[i]; top_ids[i] = top_ids[j]; top_ids[j] = ti;
                }
        memcpy(out_ids, top_ids, sizeof(int32) * out_k);
        memcpy(out_scores, top_scores, sizeof(float) * out_k);
        pfree(top_ids); pfree(top_scores);
        return out_k;
    }
}
