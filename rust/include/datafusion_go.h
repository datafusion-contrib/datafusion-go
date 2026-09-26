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
typedef struct dfgo_import dfgo_import;

/* Callback ABI v1/v2 (identical table layout). Rust copies this table. invoke returns 0 on success,
 * 1 on error (UTF-8 output); successful outputs are IPC/JSON per operation.
 * Every output byte allocation is released using free_bytes, in its allocating
 * module. Handles transfer only on success; registration consumes its input
 * handle on entry. Release (0) must be nonblocking and cannot fail.
 * Operation contexts (1=create, 2=cancel) are retained until all callbacks finish.
 * Opcodes: 3=table schema IPC, 4=open scan (JSON options -> schema IPC + reader
 * handle), 5=reader next (one-batch IPC, empty output means EOF), 6=UDF signature
 * IPC, 7=UDF evaluation (one-batch IPC), 8=catalog resolution (JSON reference ->
 * capability digit + provider handle), 9/10/11=append/overwrite/replace INSERT.
 * INSERT receives a mutable ArrowArrayStream as input, with input_len
 * equal to sizeof(struct ArrowArrayStream). The callback moves its ownership,
 * releases its reader before return, and returns the row count as decimal UTF-8.
 * v2 additionally supports 12=reader next and 13=UDF evaluation through a
 * mutable dfgo_arrow_exchange, input_len=sizeof(dfgo_arrow_exchange). The caller
 * initializes every member (empty output, native-owned input for UDFs). The
 * callback consumes input and exports C-owned output; EOF leaves output.release
 * NULL. Layouts needing the compatibility path instead return one-batch IPC
 * bytes and leave output empty. The caller releases every member even on failure or
 * query cancellation. No Go buffer may be retained by the exported output.
 * Other inputs are borrowed read-only. New child handles transfer only on
 * success, and callers release them even if cancellation abandons the result.
 */
typedef struct dfgo_callbacks {
  uint64_t version;
  int (*invoke)(uint64_t handle, uint64_t operation, int32_t opcode, const uint8_t *input, int64_t input_len, uint8_t **output, int64_t *output_len, uint64_t *output_handle);
  void (*free_bytes)(uint8_t *data);
} dfgo_callbacks;

typedef struct dfgo_arrow_exchange {
  struct ArrowArray input;
  struct ArrowSchema input_schema;
  struct ArrowArray output;
  struct ArrowSchema output_schema;
} dfgo_arrow_exchange;

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
 * Incremental imports retain owned decoded batches but publish no table until
 * commit succeeds. Append accepts a complete IPC stream (including schema-only
 * streams for empty tables). Commit does not consume the handle. Close releases
 * the handle after success or failure; callers must not append after commit.
 *
 * ABI ownership rules:
 * - Handles returned through out parameters are Rust-owned and must be returned
 *   exactly once through the matching dfgo_*_close function.
 * - Error handles returned through dfgo_error **err are Rust-owned and must be
 *   released with dfgo_error_free after reading kind/message pointers.
 * - Input strings must be valid UTF-8 where documented by the Go wrapper and
 *   remain live for the duration of the call.
 * - dfgo_connection_register_arrow_stream consumes a non-null ArrowArrayStream
 *   even when later validation or registration fails.
 * - dfgo_connection_register_ffi_table_provider borrows the FFI_TableProvider
 *   and clones it into the session; the caller retains ownership of the
 *   pointer and must free it through its producing library. The clone only
 *   bumps a refcount: the producing library must stay loaded and un-freed for
 *   as long as the table is registered and any dependent views, query plans,
 *   streams, or returned Arrow batches remain alive. Deregistration alone
 *   does not release these foreign callbacks. provider_datafusion_version is checked
 *   against this library's datafusion version before the provider is
 *   dereferenced; a mismatch is reported as an error rather than risking UB.
 * - dfgo_statement_execute_with_params borrows params and all nested pointers
 *   only for the duration of the call. Parameters are per-call state, so
 *   concurrent executions with separate params arrays cannot interleave
 *   parameter values.
 */

#ifndef DFGO_NO_FUNCTION_PROTOTYPES
int32_t dfgo_abi_version(void);
const char *dfgo_datafusion_version(void);

int dfgo_database_open(const char *dsn, dfgo_database **out, dfgo_error **err);
void dfgo_database_close(dfgo_database *db);

int dfgo_connection_open_isolated(dfgo_database *db, dfgo_connection **out, dfgo_error **err);
int dfgo_connection_open_shared(dfgo_database *db, dfgo_connection **out, dfgo_error **err);
void dfgo_connection_close(dfgo_connection *conn);
int dfgo_connection_register_arrow_ipc(dfgo_connection *conn, const char *name, const uint8_t *data, int64_t len, dfgo_error **err);
int dfgo_connection_register_arrow_stream(dfgo_connection *conn, const char *name, struct ArrowArrayStream *stream, dfgo_error **err);
int dfgo_connection_register_ffi_table_provider(dfgo_connection *conn, const char *name, const void *provider, const char *provider_datafusion_version, dfgo_error **err);
int dfgo_connection_deregister_table(dfgo_connection *conn, const char *name, dfgo_error **err);

int dfgo_import_open(dfgo_connection *conn, const char *name, dfgo_import **out, dfgo_error **err);
int dfgo_import_append(dfgo_import *importer, const uint8_t *data, int64_t len, dfgo_error **err);
int dfgo_import_commit(dfgo_connection *conn, dfgo_import *importer, dfgo_error **err);
void dfgo_import_close(dfgo_import *importer);

int dfgo_connection_register_go(dfgo_connection *conn, const char *name, int32_t kind, uint64_t handle, const void *callbacks, dfgo_error **err);
int dfgo_cancel_token_set_go(dfgo_cancel_token *token, uint64_t handle, const void *callbacks, dfgo_error **err);

int dfgo_prepare(dfgo_connection *conn, const char *query, dfgo_statement **out, dfgo_error **err);
void dfgo_statement_close(dfgo_statement *stmt);
int64_t dfgo_statement_num_params(dfgo_statement *stmt);
int dfgo_statement_serializes(dfgo_statement *stmt);

int dfgo_cancel_token_create(dfgo_cancel_token **out, dfgo_error **err);
void dfgo_cancel_token_cancel(dfgo_cancel_token *token);
void dfgo_cancel_token_close(dfgo_cancel_token *token);

int dfgo_statement_execute_with_params(dfgo_statement *stmt, const dfgo_parameter *params, int64_t params_len, dfgo_cancel_token *token, dfgo_result_stream **out, dfgo_error **err);
int dfgo_result_export_arrow_stream(dfgo_result_stream *result, struct ArrowArrayStream *out, dfgo_error **err);
const char *dfgo_result_error_kind(const dfgo_result_stream *result);
void dfgo_result_cancel(dfgo_result_stream *result);
void dfgo_result_close(dfgo_result_stream *result);

const char *dfgo_error_message(const dfgo_error *err);
const char *dfgo_error_kind(const dfgo_error *err);
void dfgo_error_free(dfgo_error *err);
#endif

#ifdef __cplusplus
}
#endif

#endif
