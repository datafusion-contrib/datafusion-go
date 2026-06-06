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

typedef struct dfgo_parameter {
  int64_t index;
  const char *name;
  int64_t name_len;
  int32_t type_code;
  int32_t is_null;
  int64_t int64_value;
  uint64_t uint64_value;
  double float64_value;
  const uint8_t *data;
  int64_t data_len;
  const char *timezone;
  int64_t timezone_len;
  uint8_t precision;
  int8_t scale;
} dfgo_parameter;

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
 * - dfgo_statement_execute_with_params borrows params and all nested pointers
 *   only for the duration of the call. Parameters are per-call state, so
 *   concurrent executions with separate params arrays cannot interleave
 *   parameter values.
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

int dfgo_statement_execute_with_params(dfgo_statement *stmt, const dfgo_parameter *params, int64_t params_len, dfgo_cancel_token *token, dfgo_result_stream **out, dfgo_error **err);
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
