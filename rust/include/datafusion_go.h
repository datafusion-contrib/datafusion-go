#ifndef DATAFUSION_GO_H
#define DATAFUSION_GO_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

struct ArrowSchema {
  const char *format;
  const char *name;
  const char *metadata;
  int64_t flags;
  int64_t n_children;
  struct ArrowSchema **children;
  struct ArrowSchema *dictionary;
  void (*release)(struct ArrowSchema *);
  void *private_data;
};

struct ArrowArray {
  int64_t length;
  int64_t null_count;
  int64_t offset;
  int64_t n_buffers;
  int64_t n_children;
  const void **buffers;
  struct ArrowArray **children;
  struct ArrowArray *dictionary;
  void (*release)(struct ArrowArray *);
  void *private_data;
};

struct ArrowArrayStream {
  int (*get_schema)(struct ArrowArrayStream *, struct ArrowSchema *);
  int (*get_next)(struct ArrowArrayStream *, struct ArrowArray *);
  const char *(*get_last_error)(struct ArrowArrayStream *);
  void (*release)(struct ArrowArrayStream *);
  void *private_data;
};

typedef struct dfgo_database dfgo_database;
typedef struct dfgo_connection dfgo_connection;
typedef struct dfgo_statement dfgo_statement;
typedef struct dfgo_result_stream dfgo_result_stream;
typedef struct dfgo_cancel_token dfgo_cancel_token;
typedef struct dfgo_error dfgo_error;

/*
 * ABI ownership rules:
 * - Handles returned through out parameters are Rust-owned and must be returned
 *   exactly once through the matching dfgo_*_close function.
 * - Error handles returned through dfgo_error **err are Rust-owned and must be
 *   released with dfgo_error_free after reading kind/message pointers.
 * - Input strings must be valid UTF-8 where documented by the Go wrapper and
 *   remain live for the duration of the call.
 * - dfgo_connection_register_arrow_stream consumes a non-null ArrowArrayStream
 *   even when later validation or registration fails.
 */

int32_t dfgo_abi_version(void);
const char *dfgo_datafusion_version(void);

int dfgo_database_open(const char *dsn, dfgo_database **out, dfgo_error **err);
void dfgo_database_close(dfgo_database *db);

int dfgo_connection_open_isolated(dfgo_database *db, dfgo_connection **out, dfgo_error **err);
int dfgo_connection_open_shared(dfgo_database *db, dfgo_connection **out, dfgo_error **err);
void dfgo_connection_close(dfgo_connection *conn);
int dfgo_connection_register_arrow_ipc(dfgo_connection *conn, const char *name, const uint8_t *data, int64_t len, dfgo_error **err);
int dfgo_connection_register_arrow_stream(dfgo_connection *conn, const char *name, struct ArrowArrayStream *stream, dfgo_error **err);

int dfgo_prepare(dfgo_connection *conn, const char *query, dfgo_statement **out, dfgo_error **err);
void dfgo_statement_close(dfgo_statement *stmt);
int64_t dfgo_statement_num_params(dfgo_statement *stmt);
int dfgo_statement_serializes(dfgo_statement *stmt);

int dfgo_cancel_token_create(dfgo_cancel_token **out, dfgo_error **err);
void dfgo_cancel_token_cancel(dfgo_cancel_token *token);
void dfgo_cancel_token_close(dfgo_cancel_token *token);

int dfgo_statement_clear_bindings(dfgo_statement *stmt, dfgo_error **err);
int dfgo_statement_set_param_name(dfgo_statement *stmt, int64_t index, const char *name, dfgo_error **err);
int dfgo_statement_bind_null(dfgo_statement *stmt, int64_t index, dfgo_error **err);
int dfgo_statement_bind_bool(dfgo_statement *stmt, int64_t index, int value, dfgo_error **err);
int dfgo_statement_bind_int64(dfgo_statement *stmt, int64_t index, int64_t value, dfgo_error **err);
int dfgo_statement_bind_uint64(dfgo_statement *stmt, int64_t index, uint64_t value, dfgo_error **err);
int dfgo_statement_bind_float64(dfgo_statement *stmt, int64_t index, double value, dfgo_error **err);
int dfgo_statement_bind_date32(dfgo_statement *stmt, int64_t index, int32_t value, dfgo_error **err);
int dfgo_statement_bind_time64_ns(dfgo_statement *stmt, int64_t index, int64_t value, dfgo_error **err);
int dfgo_statement_bind_timestamp_ns(dfgo_statement *stmt, int64_t index, int64_t value, dfgo_error **err);
int dfgo_statement_bind_timestamp_ns_tz(dfgo_statement *stmt, int64_t index, int64_t value, const char *timezone, int64_t timezone_len, dfgo_error **err);
int dfgo_statement_bind_duration_ns(dfgo_statement *stmt, int64_t index, int64_t value, dfgo_error **err);
int dfgo_statement_bind_decimal128(dfgo_statement *stmt, int64_t index, const char *value, int64_t len, uint8_t precision, int8_t scale, dfgo_error **err);
int dfgo_statement_bind_typed_null(dfgo_statement *stmt, int64_t index, int32_t type_code, uint8_t precision, int8_t scale, const char *timezone, int64_t timezone_len, dfgo_error **err);
int dfgo_statement_bind_string(dfgo_statement *stmt, int64_t index, const char *value, int64_t len, dfgo_error **err);
int dfgo_statement_bind_binary(dfgo_statement *stmt, int64_t index, const uint8_t *value, int64_t len, dfgo_error **err);

int dfgo_statement_execute(dfgo_statement *stmt, dfgo_cancel_token *token, dfgo_result_stream **out, dfgo_error **err);
int dfgo_result_export_arrow_stream(dfgo_result_stream *result, struct ArrowArrayStream *out, dfgo_error **err);
void dfgo_result_cancel(dfgo_result_stream *result);
void dfgo_result_close(dfgo_result_stream *result);

const char *dfgo_error_message(const dfgo_error *err);
const char *dfgo_error_kind(const dfgo_error *err);
void dfgo_error_free(dfgo_error *err);

#ifdef __cplusplus
}
#endif

#endif
