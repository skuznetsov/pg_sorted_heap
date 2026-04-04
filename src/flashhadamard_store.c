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
               const float *norms, int n_rows)
{
    int     fd;
    char    meta_page[FH_PAGE_SIZE];
    int     sq8_params_pages = 0, packed_pages = 0, sq8_pages = 0, norm_pages = 0;
    off_t   sq8_params_offset, packed_offset, sq8_data_offset, norm_offset;

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

        /* Rewrite meta page with exact byte offsets */
        m = (FHMetaPageDataV2 *)(meta_page + sizeof(FHFilePageHeader));
        m->off_sq8_params = sq8_params_offset;
        m->off_packed = packed_offset;
        m->off_sq8 = sq8_data_offset;
        m->off_norms = norm_offset;
        m->off_end = norm_offset + (off_t)(sizeof(float) * n_rows);

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
    int     fd;
    char    meta_page[FH_PAGE_SIZE];
    FHMetaPageDataV2 *meta;
    int     dim, n_rows, n_bytes;
    float  *byte_tables;
    float  *coeffs;
    float  *rotated_q;
    int32  *top_ids;
    float  *top_scores;
    int     filled = 0, min_pos = 0;
    float   min_score = -FLT_MAX;
    int     i, j;
    float   inv_sqrt_dim;

    fd = open(path, O_RDONLY);
    if (fd < 0)
        return -1;

    /* Read meta */
    if (read(fd, meta_page, FH_PAGE_SIZE) != FH_PAGE_SIZE)
    { close(fd); return -1; }
    meta = (FHMetaPageDataV2 *)(meta_page + sizeof(FHFilePageHeader));
    if (meta->magic != FH_STORE_MAGIC)
    { close(fd); return -1; }

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

    byte_tables = palloc(sizeof(float) * n_bytes * 256);
    fh_build_byte_tables(coeffs, params->centers, dim, byte_tables);

    /* Stage 1: Score packed codes page by page */
    top_ids = palloc(sizeof(int32) * shortlist_m);
    top_scores = palloc(sizeof(float) * shortlist_m);

    {
        /* mmap the entire file for zero-copy access to packed codes + norms */
        struct stat st;
        void *mapped = MAP_FAILED;
        uint8 *packed_ptr;
        float *norms_ptr;

        if (fstat(fd, &st) == 0 && st.st_size > 0)
            mapped = mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);

        if (mapped != MAP_FAILED)
        {
            /* Direct pointer into mmap'd region — zero copy */
            packed_ptr = (uint8 *)mapped + meta->off_packed;
            norms_ptr = (float *)((char *)mapped + meta->off_norms);

            fh_packed_score_topk_t(packed_ptr, byte_tables, norms_ptr,
                                    n_rows, n_bytes, shortlist_m,
                                    top_ids, top_scores, &filled);
        }
        else
        {
            /* Fallback: read() */
            Size total_packed = (Size)n_bytes * n_rows;
            uint8 *packed_buf = MemoryContextAllocHuge(CurrentMemoryContext, total_packed);
            float *norms_buf = palloc(sizeof(float) * n_rows);
            fh_read_raw(fd, meta->off_packed, packed_buf, total_packed);
            fh_read_raw(fd, meta->off_norms, norms_buf, sizeof(float) * n_rows);
            fh_packed_score_topk_t(packed_buf, byte_tables, norms_buf,
                                    n_rows, n_bytes, shortlist_m,
                                    top_ids, top_scores, &filled);
            pfree(norms_buf);
            pfree(packed_buf);
        }

        /* munmap if used */
        if (mapped != MAP_FAILED)
            munmap(mapped, st.st_size);
    }

    /* Stage 2: SQ8 rerank if shortlist_m > k */
    if (shortlist_m > k && filled > k)
    {
        float  *sq8_mins_local, *sq8_scales_local;
        float  *q_norm;
        int32  *final_ids = palloc(sizeof(int32) * k);
        float  *final_scores = palloc(sizeof(float) * k);
        int     final_filled = 0, final_min_pos = 0;
        float   final_min_score = -FLT_MAX;

        /* Read SQ8 params (raw) */
        {
            Size params_size = sizeof(float) * dim * 2;
            float *params_buf = palloc(params_size);
            fh_read_raw(fd, meta->off_sq8_params, params_buf, params_size);
            sq8_mins_local = params_buf;
            sq8_scales_local = params_buf + dim;
        }

        /* Normalize query */
        q_norm = palloc(sizeof(float) * dim);
        {
            float norm = 0.0f;
            for (i = 0; i < dim; i++) norm += query_vec[i] * query_vec[i];
            norm = sqrtf(norm);
            if (norm < 1e-12f) norm = 1e-12f;
            for (i = 0; i < dim; i++) q_norm[i] = query_vec[i] / norm;
        }

        /* Rerank each shortlist candidate */
        for (i = 0; i < filled; i++)
        {
            int row = top_ids[i];
            /* Raw pread of this row's SQ8 codes */
            uint8 row_codes[8192]; /* max dim */
            float dot = 0.0f;
            fh_read_raw(fd, meta->off_sq8 + (off_t)row * dim, row_codes, dim);

            for (j = 0; j < dim; j++)
            {
                float decoded = (float)row_codes[j] * sq8_scales_local[j] + sq8_mins_local[j];
                dot += decoded * q_norm[j];
            }

            fh_topk_insert(dot, row, final_scores, final_ids, k,
                         &final_filled, &final_min_pos, &final_min_score);
        }

        /* Sort final results */
        for (i = 0; i < final_filled; i++)
            for (j = i + 1; j < final_filled; j++)
                if (final_scores[j] > final_scores[i])
                {
                    float ts = final_scores[i]; final_scores[i] = final_scores[j]; final_scores[j] = ts;
                    int32 ti = final_ids[i]; final_ids[i] = final_ids[j]; final_ids[j] = ti;
                }

        memcpy(out_ids, final_ids, sizeof(int32) * Min(final_filled, k));
        memcpy(out_scores, final_scores, sizeof(float) * Min(final_filled, k));
        close(fd);
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
        close(fd);
        return out_k;
    }
}
