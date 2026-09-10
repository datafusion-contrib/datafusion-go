/* Exercise the actual loader and Arrow ownership protocol from a C consumer.
 * Run under Clang sanitizers and source coverage with scripts/test_native.py. */
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define DFGO_NO_FUNCTION_PROTOTYPES
#include "datafusion_go.h"
#include "dynamic_loader.h"

static void check(int rc, dfgo_error *error) {
  if (rc != 0) {
    fprintf(stderr, "%s: %s\n", dfgo_error_kind(error), dfgo_error_message(error));
    dfgo_error_free(error);
    exit(1);
  }
  assert(error == NULL);
}

static void query_once(int shared, int cancel, int64_t value) {
  dfgo_database *db = NULL;
  dfgo_connection *conn = NULL;
  dfgo_statement *stmt = NULL;
  dfgo_cancel_token *token = NULL;
  dfgo_result_stream *result = NULL;
  dfgo_error *error = NULL;
  int rc = dfgo_database_open(NULL, &db, &error);
  check(rc, error);
  rc = shared ? dfgo_connection_open_shared(db, &conn, &error)
              : dfgo_connection_open_isolated(db, &conn, &error);
  check(rc, error);
  rc = dfgo_prepare(conn, "select cast($1 as bigint) as value", &stmt, &error);
  check(rc, error);
  assert(dfgo_statement_num_params(stmt) == 1);
  assert(dfgo_statement_serializes(stmt) == 0);
  rc = dfgo_cancel_token_create(&token, &error);
  check(rc, error);
  dfgo_parameter parameter = {0};
  parameter.index = 1;
  parameter.type_code = 2; /* PARAMETER_INT64 */
  parameter.int64_value = value;
  if (cancel) {
    dfgo_cancel_token_cancel(token);
  }
  rc = dfgo_statement_execute_with_params(stmt, &parameter, 1, token, &result, &error);
  if (cancel) {
    assert(rc != 0 && result == NULL && error != NULL);
    assert(strcmp(dfgo_error_kind(error), "cancelled") == 0);
    assert(dfgo_error_message(error) != NULL);
    dfgo_error_free(error);
  } else {
    check(rc, error);
  }
  dfgo_statement_close(stmt);
  dfgo_connection_close(conn);
  dfgo_database_close(db);
  dfgo_cancel_token_close(token);
  if (cancel) {
    return;
  }

  struct ArrowArrayStream stream = {0};
  rc = dfgo_result_export_arrow_stream(result, &stream, &error);
  check(rc, error);
  struct ArrowSchema schema = {0};
  assert(stream.get_schema(&stream, &schema) == 0);
  assert(schema.n_children == 1);
  assert(strcmp(schema.children[0]->format, "l") == 0);
  schema.release(&schema);

  struct ArrowArray array = {0};
  assert(stream.get_next(&stream, &array) == 0);
  assert(array.release != NULL && array.length == 1 && array.n_children == 1);
  struct ArrowArray *column = array.children[0];
  assert(((const int64_t *)column->buffers[1])[column->offset] == value);
  /* Retained Arrow arrays survive releasing every originating handle. */
  dfgo_result_cancel(result);
  dfgo_result_close(result);
  stream.release(&stream);
  assert(((const int64_t *)column->buffers[1])[column->offset] == value);
  array.release(&array);
}

int main(int argc, char **argv) {
  if (argc != 2) {
    fprintf(stderr, "usage: abi-smoke /absolute/path/to/native/library\n");
    return 2;
  }
  assert(dfgo_native_uses_dynamic_loader() == 1);
  assert(dfgo_native_load_library(NULL) != 0);
  assert(strstr(dfgo_native_load_error(), "empty") != NULL);
  if (dfgo_native_load_library(argv[1]) != 0) {
    fprintf(stderr, "%s\n", dfgo_native_load_error());
    return 1;
  }
  assert(dfgo_abi_version() > 0);
  assert(strlen(dfgo_datafusion_version()) > 0);
  assert(dfgo_native_load_library(argv[1]) == 0);
  for (int i = 0; i < 32; ++i) {
    query_once(i % 2, i % 3 == 0, i - 16);
  }
  puts("C ABI lifecycle smoke passed");
  return 0;
}
