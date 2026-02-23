/*
  zlib License

  Copyright (c) 2026 Robin Brase https://brase.xyz/

  This software is provided 'as-is', without any express or implied
  warranty. In no event will the authors be held liable for any damages
  arising from the use of this software.

  Permission is granted to anyone to use this software for any purpose,
  including commercial applications, and to alter it and redistribute it
  freely, subject to the following restrictions:

  1. The origin of this software must not be misrepresented; you must not
     claim that you wrote the original software. If you use this software
     in a product, an acknowledgment in the product documentation would be
     appreciated but is not required.
  2. Altered source versions must be plainly marked as such, and must not be
     misrepresented as being the original software.
  3. This notice may not be removed or altered from any source distribution.
*/

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <limits.h>

#include "nanopq.h"
#ifdef ZSTD
#include "zstd.h"
#endif


static const char NANOPQ_CREATED_BY[] = "NanoPQ version 0.0.1";

// https://issues.apache.org/jira/secure/attachment/12399869/compact-proto-spec-2.txt
typedef enum {
  PQ_TTYPE_STOP = 0,
  PQ_TTYPE_TRUE = 1,
  PQ_TTYPE_FALSE = 2,
  PQ_TTYPE_BYTE = 3,
  PQ_TTYPE_I16 = 4,
  PQ_TTYPE_I32 = 5,
  PQ_TTYPE_I64 = 6,
  PQ_TTYPE_DOUBLE = 7,
  PQ_TTYPE_BINARY = 8,
  PQ_TTYPE_LIST = 9,
  PQ_TTYPE_SET = 10,
  PQ_TTYPE_MAP = 11,
  PQ_TTYPE_STRUCT = 12,
} PqThriftType;

// https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L32
typedef enum {
  PQ_TYPE_BOOLEAN = 0,
  PQ_TYPE_INT32 = 1,
  PQ_TYPE_INT64 = 2,
  PQ_TYPE_INT96 = 3,
  PQ_TYPE_FLOAT = 4,
  PQ_TYPE_DOUBLE = 5,
  PQ_TYPE_BYTE_ARRAY = 6,
  PQ_TYPE_FIXED_LEN_BYTE_ARRAY = 7,
  PQ_TYPE_UNDEFINED
} PqType;

// https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L183
typedef enum {
  PQ_FR_TYPE_REQUIRED  = 0,
  PQ_FR_TYPE_OPTIONAL = 1,
  PQ_FR_TYPE_REPEATED = 2
} PqFieldRepetitionType;

// https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L566
typedef enum {
  PQ_ENCODING_PLAIN = 0,
  PQ_ENCODING_PLAIN_DICTIONARY = 2,
  PQ_ENCODING_RLE = 3,
  PQ_ENCODING_BIT_PACKED = 4,
  PQ_ENCODING_DELTA_BINARY_PACKED = 5,
  PQ_ENCODING_DELTA_LENGTH_BYTE_ARRAY = 6,
  PQ_ENCODING_DELTA_BYTE_ARRAY = 7,
  PQ_ENCODING_RLE_DICTIONARY = 8,
  PQ_ENCODING_BYTE_STREAM_SPLIT = 9
} PqEncoding;

// https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift#L642
typedef enum {
  PQ_COMPRESSION_UNCOMPRESSED = 0,
  PQ_COMPRESSION_SNAPPY = 1,
  PQ_COMPRESSION_GZIP = 2,
  PQ_COMPRESSION_LZO = 3,
  PQ_COMPRESSION_BROTLI = 4,
  PQ_COMPRESSION_LZ4 = 5,
  PQ_COMPRESSION_ZSTD = 6,
  PQ_COMPRESSION_LZ4_RAW = 7
} PqCompressionCodec;

typedef enum {
  PQ_PAGE_TYPE_DATA_PAGE = 0,
  PQ_PAGE_TYPE_INDEX_PAGE = 1,
  PQ_PAGE_TYPE_DICTIONARY_PAGE = 2,
  PQ_PAGE_TYPE_DATA_PAGE_V2 = 3
} PqPageType;

typedef struct {
    uint64_t size;
    const char *data;
} PqString;

#define CHECK(cond, msg, ...) if (!(cond)) { printf("Error (%s:L%d): " msg "\n", __FILE__, __LINE__, __VA_ARGS__); exit(1); }
#define UNSUPPORTED(msg, ...) do { printf("Unsupported feature (%s:%d, %s): " msg "\n", __FILE__, __LINE__, __func__, __VA_ARGS__); exit(1); } while(0);

static uint64_t zigzag_encode_64(int64_t n) {
    return ((uint64_t)n << 1) ^ (uint64_t)(n >> 63);
}

static int64_t zigzag_decode_64(uint64_t n) {
    return (int64_t)((n >> 1) ^ (uint64_t)(-(int64_t)(n & 1)));
}

static size_t uleb128_encode_to_file(uint64_t value, FILE *file) {
    uint8_t byte;
    size_t written = 0;

    do {
        byte = (uint8_t)(value & 0x7F);
        value >>= 7;
        if (value != 0) {
            byte |= 0x80;
        }
        CHECK(fputc((int)byte, file) != EOF, "%s", "Failed writing uleb128 byte");
        written++;
    } while (value != 0);
    return written;
}

static size_t pq_write_string(PqString s, FILE* f) {
    size_t written = uleb128_encode_to_file(s.size, f);
    written += fwrite(s.data, 1, s.size, f);

    return written;
}

static uint8_t compute_start_byte(PqThriftType t_type, int prev_id, int curr_id) {
    uint8_t type = (uint8_t)t_type & 0x0Fu;
    unsigned int delta = (unsigned int)curr_id - (unsigned int)prev_id;
    uint8_t id_delta = (uint8_t)(delta & 0x0Fu);

    return (uint8_t)((id_delta << 4) | type);
}

static size_t pq_write_list_header(int64_t len, PqThriftType type, FILE* f) {
    CHECK(len >= 0, "Invalid list length: %ld", len);
    if (len >= 15) {
        uint8_t c = (uint8_t)(((uint8_t)type & 0x0Fu) | 0xF0u);
        fputc(c, f);
        return 1u + uleb128_encode_to_file((uint64_t)len, f);
    } else {
        uint8_t c = (uint8_t)(((uint8_t)type & 0x0Fu) | ((((uint8_t)len) & 0x0Fu) << 4));
        fputc(c, f);
        return 1u;
    }
}

typedef struct {
    PqType type;
    PqCompressionCodec codec;
    PqString path_in_schema;
    int64_t num_values;
    int64_t total_uncompressed_size;
    int64_t total_compressed_size;
    int64_t data_page_offset;
} ColumnMetaData;

typedef struct {
    int64_t length;
    int sub_type;
} PqListHeader;

typedef struct {
    int64_t version;
    int64_t num_rows;
    int64_t row_group_count;
    PqString schema;
    PqString row_groups;
    PqString created_by;
} ExistingFooter;

// https://github.com/apache/parquet-format/blob/apache-parquet-format-2.12.0/src/main/thrift/parquet.thrift#L875
static size_t pq_write_column_meta_data(ColumnMetaData d,  FILE *f) {
    uint8_t current_field_id = 0;
    size_t metadata_size = 0;

    // 1: required Type type
    fputc(compute_start_byte(PQ_TTYPE_I32, current_field_id, 1), f);
    current_field_id = 1;
    metadata_size += 1 + uleb128_encode_to_file(zigzag_encode_64(d.type), f);

    // 2: required list<Encoding> encodings
    fputc(compute_start_byte(PQ_TTYPE_LIST, current_field_id, 2), f);
    current_field_id = 2;
    metadata_size += 1 + pq_write_list_header(1, PQ_TTYPE_I32, f);
    metadata_size +=  uleb128_encode_to_file(zigzag_encode_64(PQ_ENCODING_PLAIN), f);

    // 3: required list<string> path_in_schema
    fputc(compute_start_byte(PQ_TTYPE_LIST, current_field_id, 3), f);
    current_field_id = 3;
    metadata_size += 1 + pq_write_list_header(1, PQ_TTYPE_BINARY, f);
    metadata_size += pq_write_string(d.path_in_schema, f);

    // 4: required CompressionCodec codec
    fputc(compute_start_byte(PQ_TTYPE_I32, current_field_id, 4), f);
    current_field_id = 4;
    metadata_size += 1 + uleb128_encode_to_file(zigzag_encode_64(d.codec), f);

    // 5: required i64 num_values
    fputc(compute_start_byte(PQ_TTYPE_I64, current_field_id, 5), f);
    current_field_id = 5;
    metadata_size += 1 + uleb128_encode_to_file(zigzag_encode_64(d.num_values), f);

    // 6: required i64 total_uncompressed_size
    fputc(compute_start_byte(PQ_TTYPE_I64, current_field_id, 6), f);
    current_field_id = 6;
    metadata_size += 1 + uleb128_encode_to_file(zigzag_encode_64(d.total_uncompressed_size), f);

    // 7: required i64 total_compressed_size
    fputc(compute_start_byte(PQ_TTYPE_I64, current_field_id, 7), f);
    current_field_id = 7;
    metadata_size += 1 + uleb128_encode_to_file(zigzag_encode_64(d.total_compressed_size), f);

    // 9: required i64 data_page_offset
    fputc(compute_start_byte(PQ_TTYPE_I64, current_field_id, 9), f);
    current_field_id = 9;
    metadata_size += 1 + uleb128_encode_to_file(zigzag_encode_64(d.data_page_offset), f);

    fputc(compute_start_byte(PQ_TTYPE_STOP, 0, 0), f);
    metadata_size += 1;

    return metadata_size;
}

// https://github.com/apache/parquet-format/blob/apache-parquet-format-2.12.0/src/main/thrift/parquet.thrift#L958
static size_t pq_write_column_chunk(ColumnMetaData d, FILE *f) {
    uint8_t current_field_id = 0;
    size_t metadata_size = 0;

    // 2: required i64 file_offset = 0
    fputc(compute_start_byte(PQ_TTYPE_I64, current_field_id, 2), f);
    current_field_id = 2;
    metadata_size += 1 + uleb128_encode_to_file(zigzag_encode_64(0), f);

    // 3: optional ColumnMetaData meta_data
    fputc(compute_start_byte(PQ_TTYPE_STRUCT, current_field_id, 3), f);
    current_field_id = 3;
    metadata_size += 1 + pq_write_column_meta_data(d, f);

    fputc(compute_start_byte(PQ_TTYPE_STOP, 0, 0), f);
    metadata_size += 1;

    return metadata_size;
}

// https://github.com/apache/parquet-format/blob/apache-parquet-format-2.12.0/src/main/thrift/parquet.thrift#L1001
static size_t pq_write_row_group(ColumnMetaData *columns, int64_t num_cols,  FILE *f) {
    uint8_t current_field_id = 0;
    size_t metadata_size = 0;

    CHECK(num_cols > 0, "NumCols must be > 0 (is %ld)", num_cols);

    int64_t total_byte_size = 0;
    int64_t num_rows = 0;

    // 1: required list<ColumnChunk> columns
    fputc(compute_start_byte(PQ_TTYPE_LIST, current_field_id, 1), f);
    current_field_id = 1;
    metadata_size += 1 + pq_write_list_header(num_cols, PQ_TTYPE_STRUCT, f);
    for (int64_t i=0; i < num_cols; i++) {
        metadata_size += pq_write_column_chunk(columns[i], f);

        total_byte_size += columns[i].total_compressed_size;
        num_rows = columns[i].num_values;
    }

    // 2: required i64 total_byte_size
    fputc(compute_start_byte(PQ_TTYPE_I64, current_field_id, 2), f);
    current_field_id = 2;
    metadata_size += 1 + uleb128_encode_to_file(zigzag_encode_64(total_byte_size), f);

    // 3: required i64 num_rows
    fputc(compute_start_byte(PQ_TTYPE_I64, current_field_id, 3), f);
    current_field_id = 3;
    metadata_size += 1 + uleb128_encode_to_file(zigzag_encode_64(num_rows), f);

    fputc(compute_start_byte(PQ_TTYPE_STOP, 0, 0), f);
    metadata_size += 1;

    return metadata_size;
}

// https://github.com/apache/parquet-format/blob/apache-parquet-format-2.12.0/src/main/thrift/parquet.thrift#L505
static size_t pq_write_schema_element(ColumnMetaData d, FILE* f) {
    uint8_t current_field_id = 0;
    size_t metadata_size = 0;

    if (d.type == PQ_TYPE_UNDEFINED) { // Root node
        // 4: required string name;
        fputc(compute_start_byte(PQ_TTYPE_BINARY, current_field_id, 4), f);
        current_field_id = 4;
        metadata_size += 1 + pq_write_string(d.path_in_schema, f);

        // 5: optional i32 num_children;
        fputc(compute_start_byte(PQ_TTYPE_I32, current_field_id, 5), f);
        current_field_id = 5;
        metadata_size += 1 + uleb128_encode_to_file(zigzag_encode_64(d.num_values), f);
    } else {
        // 1: optional Type type;
        fputc(compute_start_byte(PQ_TTYPE_I32, current_field_id, 1), f);
        current_field_id = 1;
        metadata_size += 1 + uleb128_encode_to_file(zigzag_encode_64(d.type), f);

        // 3: optional FieldRepetitionType repetition_type
        fputc(compute_start_byte(PQ_TTYPE_I32, current_field_id, 3), f);
        current_field_id = 3;
        metadata_size += 1 + uleb128_encode_to_file(zigzag_encode_64(PQ_FR_TYPE_REQUIRED), f);

        // 4: required string name;
        fputc(compute_start_byte(PQ_TTYPE_BINARY, current_field_id, 4), f);
        current_field_id = 4;
        metadata_size += 1 + pq_write_string(d.path_in_schema, f);
    }

    fputc(compute_start_byte(PQ_TTYPE_STOP, 0, 0), f);
    metadata_size += 1;

    return metadata_size;
}

static size_t write_page_header(int64_t num_values, int64_t uncompressed_page_size, int64_t compressed_page_size, FILE  *f) {
    uint8_t current_field_id = 0;
    size_t metadata_size = 0;

    // 1: required PageType type
    fputc(compute_start_byte(PQ_TTYPE_I32, current_field_id, 1), f);
    current_field_id = 1;
    metadata_size += 1 + uleb128_encode_to_file(zigzag_encode_64(PQ_PAGE_TYPE_DATA_PAGE), f);

    // 2: required i32 uncompressed_page_size
    fputc(compute_start_byte(PQ_TTYPE_I32, current_field_id, 2), f);
    current_field_id = 2;
    metadata_size += 1 + uleb128_encode_to_file(zigzag_encode_64(uncompressed_page_size), f);

    // 3: required i32 compressed_page_size
    fputc(compute_start_byte(PQ_TTYPE_I32, current_field_id, 3), f);
    current_field_id = 3;
    metadata_size += 1 + uleb128_encode_to_file(zigzag_encode_64(compressed_page_size), f);

    // 5: optional DataPageHeader data_page_header;
    fputc(compute_start_byte(PQ_TTYPE_STRUCT, current_field_id, 5), f);
    current_field_id = 5;
    metadata_size += 1;
    {
        uint8_t inner_field_id = 0;

        // 1: required i32 num_values
        fputc(compute_start_byte(PQ_TTYPE_I32, inner_field_id, 1), f);
        inner_field_id = 1;
        metadata_size += 1 + uleb128_encode_to_file(zigzag_encode_64(num_values), f);

        // 2: required Encoding encoding
        // 3: required Encoding definition_level_encoding;
        // 4: required Encoding repetition_level_encoding;
        for (uint8_t i = 2; i < 5; i++) {
            fputc(compute_start_byte(PQ_TTYPE_I32, inner_field_id, i), f);
            inner_field_id = i;
            metadata_size += 1 + uleb128_encode_to_file(zigzag_encode_64(PQ_ENCODING_PLAIN), f);
        }

        fputc(compute_start_byte(PQ_TTYPE_STOP, 0, 0), f);
        metadata_size += 1;
    }

    fputc(compute_start_byte(PQ_TTYPE_STOP, 0, 0), f);
    metadata_size += 1;

    return metadata_size;
}

static void uleb128_decode_from_memory(const uint8_t **cursor, const uint8_t *end, uint64_t *value) {
    uint64_t result = 0;
    int shift = 0;

    while (*cursor < end) {
        uint8_t byte = *(*cursor)++;
        result |= (uint64_t)(byte & 0x7F) << shift;
        if ((byte & 0x80) == 0) {
            *value = result;
            return;
        }
        shift += 7;
    }

    CHECK(0, "%s", "Unexpected end while decoding uleb128");
}

// https://issues.apache.org/jira/secure/attachment/12399869/compact-proto-spec-2.txt
static void read_list_header_from_memory(const uint8_t **cursor, const uint8_t *end, PqListHeader *header) {
    CHECK(*cursor < end, "%s", "Unexpected end while reading list header");

    uint8_t c = *(*cursor)++;
    header->sub_type = c & 0xF;

    int64_t len = c >> 4;
    if (len == 15) {
        uint64_t decoded = 0;
        uleb128_decode_from_memory(cursor, end, &decoded);
        len = (int64_t)decoded;
    }

    header->length = len;
}

static void skip_value(const uint8_t **cursor, const uint8_t *end, int type);

static void skip_struct(const uint8_t **cursor, const uint8_t *end) {
    int current_field_id = 0;

    while (*cursor < end) {
        uint8_t c = *(*cursor)++;
        int type = c & 0xF;
        if (type == PQ_TTYPE_STOP) {
            return;
        }

        current_field_id += c >> 4;
        (void)current_field_id;
        skip_value(cursor, end, type);
    }

    CHECK(0, "%s", "Unexpected end while skipping struct");
}

static void skip_value(const uint8_t **cursor, const uint8_t *end, int type) {
    uint64_t decoded = 0;

    switch (type) {
        case PQ_TTYPE_TRUE:
        case PQ_TTYPE_FALSE:
            return;
        case PQ_TTYPE_BYTE:
            CHECK(*cursor < end, "%s", "Unexpected end while skipping byte");
            (*cursor)++;
            return;
        case PQ_TTYPE_I16:
        case PQ_TTYPE_I32:
        case PQ_TTYPE_I64:
            uleb128_decode_from_memory(cursor, end, &decoded);
            return;
        case PQ_TTYPE_DOUBLE:
            CHECK((size_t)(end - *cursor) >= sizeof(double), "%s", "Unexpected end while skipping double");
            *cursor += sizeof(double);
            return;
        case PQ_TTYPE_BINARY: {
            uleb128_decode_from_memory(cursor, end, &decoded);
            CHECK((uint64_t)(end - *cursor) >= decoded, "%s", "Unexpected end while skipping binary");
            *cursor += decoded;
            return;
        }
        case PQ_TTYPE_LIST:
        case PQ_TTYPE_SET: {
            PqListHeader header = {0};
            read_list_header_from_memory(cursor, end, &header);
            for (int64_t i = 0; i < header.length; i++) {
                skip_value(cursor, end, header.sub_type);
            }
            return;
        }
        case PQ_TTYPE_STRUCT:
            skip_struct(cursor, end);
            return;
        default:
            CHECK(0, "Unsupported compact type: %d", type);
    }
}


static void parse_existing_footer(const uint8_t *metadata, size_t metadata_size, ExistingFooter *footer) {
    memset(footer, 0, sizeof(*footer));

    const uint8_t *cursor = metadata;
    const uint8_t *end = metadata + metadata_size;
    uint8_t current_field_id = 0;

    while (cursor < end) {
        uint8_t c = *cursor++;
        uint8_t type = c & 0x0Fu;
        if (type == PQ_TTYPE_STOP) break;

        current_field_id += c >> 4;

        switch (current_field_id) {
            case 1: { // version
                CHECK(type == PQ_TTYPE_I32, "%d != %d", type, PQ_TTYPE_I32);

                uint64_t v = 0;
                uleb128_decode_from_memory(&cursor, end, &v);
                footer->version = zigzag_decode_64(v);
                break;
            }
            case 2: { // schema
                CHECK(type == PQ_TTYPE_LIST, "%d != %d", type, PQ_TTYPE_LIST);

                const uint8_t *start = cursor;
                PqListHeader header = {0};
                read_list_header_from_memory(&cursor, end, &header);
                CHECK(header.sub_type == PQ_TTYPE_STRUCT, "Invalid schema list subtype: %d", header.sub_type);
                for (int64_t i = 0; i < header.length; i++) {
                    skip_struct(&cursor, end);
                }

                footer->schema.data = (const char *)start;
                footer->schema.size = (size_t)(cursor - start);
                break;
            }
            case 3: { // num_rows
                CHECK(type == PQ_TTYPE_I64, "%d != %d", type, PQ_TTYPE_I64);

                uint64_t v = 0;
                uleb128_decode_from_memory(&cursor, end, &v);
                footer->num_rows = zigzag_decode_64(v);
                break;
            }
            case 4: { // row_groups
                CHECK(type == PQ_TTYPE_LIST, "%d != %d", type, PQ_TTYPE_LIST);

                PqListHeader header = {0};
                read_list_header_from_memory(&cursor, end, &header);
                CHECK(header.sub_type == PQ_TTYPE_STRUCT, "Invalid row_groups list subtype: %d", header.sub_type);
                footer->row_group_count = header.length;
                footer->row_groups.data = (const char *)cursor;
                for (int64_t i = 0; i < header.length; i++) {
                    skip_struct(&cursor, end);
                }
                footer->row_groups.size = (size_t)(cursor - (const uint8_t *)footer->row_groups.data);
                break;
            }
            case 6: { // created_by
                uint64_t len = 0;
                uleb128_decode_from_memory(&cursor, end, &len);
                CHECK((uint64_t)(end - cursor) >= len, "%s", "Invalid created_by field size");

                CHECK(len == strlen(NANOPQ_CREATED_BY), "Unexpected created_by length: %lu", len);
                CHECK(memcmp(cursor, NANOPQ_CREATED_BY, len) == 0, "%s", "Unsupported created_by value");

                footer->created_by.data = (const char *)cursor;
                footer->created_by.size = len;
                cursor += len;
                break;
            }
            default:
                skip_value(&cursor, end, type);
                break;
        }
    }

    CHECK(footer->schema.data != NULL, "%s", "Missing schema in footer");
    CHECK(footer->row_groups.data != NULL, "%s", "Missing row_groups in footer");
    CHECK(footer->created_by.data != NULL, "%s", "Missing created_by in footer");
}

static int64_t compress_data(const double *data, size_t uncompressed_size, int compression_level, void *out, size_t out_capacity) {
#ifndef ZSTD
    UNSUPPORTED("%s", "Compression requested but writer was compiled without ZSTD support");
    return 0;
#else
    size_t compressed_size = ZSTD_compress(
        out,
        out_capacity,
        data,
        uncompressed_size,
        compression_level
    );

    if (ZSTD_isError(compressed_size)) {
        UNSUPPORTED("zstd compression failed: %s", ZSTD_getErrorName(compressed_size));
    }

    return (int64_t)compressed_size;
#endif
}

static void init_column_metadata(ColumnMetaData *columns, const NanopqInputColumn *input_columns, int64_t num_rows, int64_t num_cols, PqCompressionCodec codec) {
    for (int64_t i = 0; i < num_cols; i++) {
        columns[i].type = PQ_TYPE_DOUBLE;
        columns[i].codec = codec;
        columns[i].path_in_schema.size = strlen(input_columns[i].name);
        columns[i].path_in_schema.data = input_columns[i].name;
        columns[i].num_values = num_rows;
        columns[i].total_uncompressed_size = 0;
        columns[i].total_compressed_size = 0;
        columns[i].data_page_offset = 0;
    }
}

static void write_row_group_data(
    FILE *f,
    const NanopqInputColumn *input_columns,
    int64_t num_rows,
    int64_t num_cols,
    int64_t values_per_block,
    int compression_level,
    uint8_t *compressed_buffer,
    size_t compressed_buffer_size,
    ColumnMetaData *columns
) {
    for (int64_t col = 0; col < num_cols; col++) {
        columns[col].data_page_offset = ftell(f);

        for (int64_t start = 0; start < num_rows; start += values_per_block) {
            int64_t page_num_values = values_per_block;
            if (start + page_num_values > num_rows) {
                page_num_values = num_rows - start;
            }

            int64_t page_uncompressed_size = page_num_values * (int64_t)sizeof(double);
            int64_t page_compressed_size = page_uncompressed_size;

            const double *column_data = input_columns[col].data;
            const void *output_data = column_data + start;
            if (columns[col].codec == PQ_COMPRESSION_ZSTD) {
                page_compressed_size = compress_data(
                    column_data + start,
                    (size_t)page_uncompressed_size,
                    compression_level,
                    compressed_buffer,
                    compressed_buffer_size
                );
                output_data = compressed_buffer;
            }

            size_t written = write_page_header(page_num_values, page_uncompressed_size, page_compressed_size, f);
            columns[col].total_uncompressed_size += (int64_t)written + page_uncompressed_size;
            columns[col].total_compressed_size += (int64_t)written + page_compressed_size;

            CHECK(fwrite(output_data, 1, (size_t)page_compressed_size, f) == (size_t)page_compressed_size, "%s", "Failed writing page bytes");
        }
    }
}

// https://github.com/apache/parquet-format/blob/apache-parquet-format-2.12.0/src/main/thrift/parquet.thrift#L1289
static void write_new_footer(FILE *f, ColumnMetaData *columns, int64_t num_rows, int64_t num_cols) {
    size_t metadata_size = 0;
    int current_field_id = 0;

    int64_t version = 1;
    fputc(compute_start_byte(PQ_TTYPE_I32, current_field_id, 1), f);
    current_field_id = 1;
    metadata_size += 1 + uleb128_encode_to_file(zigzag_encode_64(version), f);

    fputc(compute_start_byte(PQ_TTYPE_LIST, current_field_id, 2), f);
    current_field_id = 2;
    metadata_size += 1 + pq_write_list_header(1 + num_cols, PQ_TTYPE_STRUCT, f);

    ColumnMetaData root_node = {
        .type = PQ_TYPE_UNDEFINED,
        .num_values = num_cols,
        .path_in_schema = {6, "schema"},
    };
    metadata_size += pq_write_schema_element(root_node, f);

    for (int64_t i = 0; i < num_cols; i++) {
        metadata_size += pq_write_schema_element(columns[i], f);
    }

    fputc(compute_start_byte(PQ_TTYPE_I64, current_field_id, 3), f);
    current_field_id = 3;
    metadata_size += 1 + uleb128_encode_to_file(zigzag_encode_64(num_rows), f);

    fputc(compute_start_byte(PQ_TTYPE_LIST, current_field_id, 4), f);
    current_field_id = 4;
    metadata_size += 1 + pq_write_list_header(1, PQ_TTYPE_STRUCT, f);
    metadata_size += pq_write_row_group(columns, num_cols, f);

    fputc(compute_start_byte(PQ_TTYPE_BINARY, current_field_id, 6), f);
    current_field_id = 6;
    metadata_size += 1 + uleb128_encode_to_file(strlen(NANOPQ_CREATED_BY), f);
    metadata_size += fwrite(NANOPQ_CREATED_BY, 1, strlen(NANOPQ_CREATED_BY), f);

    fputc(compute_start_byte(PQ_TTYPE_STOP, 0, 0), f);
    metadata_size += 1;

    CHECK(metadata_size <= (size_t)INT32_MAX, "Metadata too large: %zu", metadata_size);
    int32_t footer_size = (int32_t)metadata_size;
    fwrite(&footer_size, sizeof(footer_size), 1, f);
    fputs("PAR1", f);

}

// https://github.com/apache/parquet-format/blob/apache-parquet-format-2.12.0/src/main/thrift/parquet.thrift#L1289
static void write_appended_footer(FILE *f, const ExistingFooter *old_footer, ColumnMetaData *new_columns, int64_t new_num_rows, int64_t num_cols) {
    size_t metadata_size = 0;
    int current_field_id = 0;

    int64_t updated_num_rows = old_footer->num_rows + new_num_rows;
    int64_t updated_row_group_count = old_footer->row_group_count + 1;

    fputc(compute_start_byte(PQ_TTYPE_I32, current_field_id, 1), f);
    current_field_id = 1;
    metadata_size += 1 + uleb128_encode_to_file(zigzag_encode_64(old_footer->version), f);

    fputc(compute_start_byte(PQ_TTYPE_LIST, current_field_id, 2), f);
    current_field_id = 2;
    metadata_size += 1 + fwrite(old_footer->schema.data, 1, old_footer->schema.size, f);

    fputc(compute_start_byte(PQ_TTYPE_I64, current_field_id, 3), f);
    current_field_id = 3;
    metadata_size += 1 + uleb128_encode_to_file(zigzag_encode_64(updated_num_rows), f);

    fputc(compute_start_byte(PQ_TTYPE_LIST, current_field_id, 4), f);
    current_field_id = 4;
    metadata_size += 1 + pq_write_list_header(updated_row_group_count, PQ_TTYPE_STRUCT, f);
    metadata_size += fwrite(old_footer->row_groups.data, 1, old_footer->row_groups.size, f);
    metadata_size += pq_write_row_group(new_columns, num_cols, f);

    fputc(compute_start_byte(PQ_TTYPE_BINARY, current_field_id, 6), f);
    current_field_id = 6;
    metadata_size += 1 + uleb128_encode_to_file(strlen(NANOPQ_CREATED_BY), f);
    metadata_size += fwrite(NANOPQ_CREATED_BY, 1, strlen(NANOPQ_CREATED_BY), f);

    fputc(compute_start_byte(PQ_TTYPE_STOP, 0, 0), f);
    metadata_size += 1;

    CHECK(metadata_size <= (size_t)INT32_MAX, "Metadata too large: %zu", metadata_size);
    int32_t footer_size = (int32_t)metadata_size;
    fwrite(&footer_size, sizeof(footer_size), 1, f);
    fputs("PAR1", f);

}

// https://github.com/apache/parquet-format/blob/apache-parquet-format-2.12.0/src/main/thrift/parquet.thrift#L1289
static uint8_t * read_existing_footer_from_file(FILE *f, ExistingFooter *footer) {
    char magic[4];
    int32_t metadata_size = 0;

    CHECK(fseek(f, 0, SEEK_SET) == 0, "%s", "Failed to seek to start");
    if (fread(magic, 1, 4, f) != 4) return NULL;
    CHECK(memcmp(magic, "PAR1", 4) == 0, "%s", "Invalid magic number at start");

    CHECK(fseek(f, 0, SEEK_END) == 0, "%s", "Failed to seek to end");
    int64_t file_size = ftell(f);
    CHECK(file_size >= 8, "File too small: %ld", file_size);

    CHECK(fseek(f, -8L, SEEK_END) == 0, "%s", "Failed to seek to footer");
    CHECK(fread(&metadata_size, 4, 1, f) == 1, "%s", "Failed to read metadata size");
    CHECK(metadata_size > 0, "Invalid metadata size: %d", metadata_size);

    CHECK(fread(magic, 1, 4, f) == 4, "%s", "Failed to read footer magic");
    CHECK(memcmp(magic, "PAR1", 4) == 0, "%s", "Invalid magic number at end");

    int64_t metadata_start = file_size - 8 - metadata_size;
    CHECK(metadata_start >= 4, "Invalid metadata start: %ld", metadata_start);

    uint8_t *metadata_blob = malloc((size_t)metadata_size);
    CHECK(metadata_blob != NULL, "%s", "Failed to allocate metadata buffer");

    CHECK(fseek(f, metadata_start, SEEK_SET) == 0, "%s", "Failed to seek to metadata start");
    CHECK(fread(metadata_blob, 1, (size_t)metadata_size, f) == (size_t)metadata_size, "%s", "Failed to read metadata blob");
    parse_existing_footer(metadata_blob, (size_t)metadata_size, footer);

    CHECK(fseek(f, metadata_start, SEEK_SET) == 0, "%s", "Failed to seek to metadata start");

    return metadata_blob;
}

void nanopq_write_file(const char *out_path, int64_t num_rows, const NanopqInputColumn *input_columns, int64_t num_cols, NanopqWriteOptions options) {
    CHECK(out_path && input_columns, "%s", "Invalid arguments");
    CHECK(num_rows >= 0, "num_rows must be >= 0 (is %ld)", num_rows);
    CHECK(num_cols > 0 && num_cols <= NANOPQ_MAX_COLUMNS, "num_cols out of range: %ld", num_cols);
    CHECK(options.block_size >= (int64_t)sizeof(double), "block_size must be >= %zu (is %ld)", sizeof(double), options.block_size);

    uint8_t *compressed_buffer = NULL;
    size_t compressed_buffer_size = 0;


    for (int64_t i = 0; i < num_cols; i++) {
        CHECK(input_columns[i].name && input_columns[i].data, "Invalid column at index %ld", i);
    }

    if (options.compression_level != 0) {
#ifndef ZSTD
        UNSUPPORTED("%s", "Compression requested but writer was compiled without ZSTD support");
#else
        compressed_buffer_size = ZSTD_compressBound((size_t)options.block_size);
        compressed_buffer = malloc(compressed_buffer_size);
        CHECK(compressed_buffer != NULL, "%s", "Failed to allocate compression buffer");
#endif
    }

    FILE *f = fopen(out_path, "rb+");
    if (!f) f = fopen(out_path, "wb");
    CHECK(f != NULL, "Failed to open file: %s", out_path);

    ExistingFooter old_footer = {0};
    uint8_t *existing_metadata_blob = read_existing_footer_from_file(f, &old_footer);
    if (existing_metadata_blob == NULL) {
        CHECK(fseek(f, 0, SEEK_SET) == 0, "%s", "Failed to seek to start");
        CHECK(ftell(f) == 0, "%s", "Unexpected non-empty file without footer");
        fputs("PAR1", f);
    }

    int64_t values_per_block = options.block_size / (int64_t)sizeof(double);

    PqCompressionCodec codec = options.compression_level == 0 ? PQ_COMPRESSION_UNCOMPRESSED : PQ_COMPRESSION_ZSTD;

    ColumnMetaData columns[NANOPQ_MAX_COLUMNS] = {0};
    init_column_metadata(columns, input_columns, num_rows, num_cols, codec);

    write_row_group_data(
        f,
        input_columns,
        num_rows,
        num_cols,
        values_per_block,
        options.compression_level,
        compressed_buffer,
        compressed_buffer_size,
        columns
    );

    if (existing_metadata_blob == NULL) {
        write_new_footer(f, columns, num_rows, num_cols);
    } else {
        write_appended_footer(f, &old_footer, columns, num_rows, num_cols);
    }

    fclose(f);
    free(existing_metadata_blob);
    free(compressed_buffer);
}
