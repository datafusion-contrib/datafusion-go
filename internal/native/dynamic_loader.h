#ifndef DATAFUSION_GO_DYNAMIC_LOADER_H
#define DATAFUSION_GO_DYNAMIC_LOADER_H

#include <stdint.h>
#include <stdio.h>
#include <string.h>

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
static HMODULE dfgo_dynamic_handle = NULL;
#else
#include <dlfcn.h>
static void *dfgo_dynamic_handle = NULL;
#endif

static char dfgo_dynamic_error[1024];

typedef int32_t (*dfgo_abi_version_fn)(void);
typedef const char *(*dfgo_datafusion_version_fn)(void);
typedef int (*dfgo_database_open_fn)(const char *, dfgo_database **, dfgo_error **);
typedef void (*dfgo_database_close_fn)(dfgo_database *);
typedef int (*dfgo_connection_open_isolated_fn)(dfgo_database *, dfgo_connection **, dfgo_error **);
typedef int (*dfgo_connection_open_shared_fn)(dfgo_database *, dfgo_connection **, dfgo_error **);
typedef void (*dfgo_connection_close_fn)(dfgo_connection *);
typedef int (*dfgo_connection_register_arrow_ipc_fn)(dfgo_connection *, const char *, const uint8_t *, int64_t, dfgo_error **);
typedef int (*dfgo_connection_register_arrow_stream_fn)(dfgo_connection *, const char *, struct ArrowArrayStream *, dfgo_error **);
typedef int (*dfgo_prepare_fn)(dfgo_connection *, const char *, dfgo_statement **, dfgo_error **);
typedef void (*dfgo_statement_close_fn)(dfgo_statement *);
typedef int64_t (*dfgo_statement_num_params_fn)(dfgo_statement *);
typedef int (*dfgo_statement_serializes_fn)(dfgo_statement *);
typedef int (*dfgo_cancel_token_create_fn)(dfgo_cancel_token **, dfgo_error **);
typedef void (*dfgo_cancel_token_cancel_fn)(dfgo_cancel_token *);
typedef void (*dfgo_cancel_token_close_fn)(dfgo_cancel_token *);
typedef int (*dfgo_statement_execute_with_params_fn)(dfgo_statement *, const dfgo_parameter *, int64_t, dfgo_cancel_token *, dfgo_result_stream **, dfgo_error **);
typedef int (*dfgo_result_export_arrow_stream_fn)(dfgo_result_stream *, struct ArrowArrayStream *, dfgo_error **);
typedef void (*dfgo_result_cancel_fn)(dfgo_result_stream *);
typedef void (*dfgo_result_close_fn)(dfgo_result_stream *);
typedef const char *(*dfgo_error_message_fn)(const dfgo_error *);
typedef const char *(*dfgo_error_kind_fn)(const dfgo_error *);
typedef void (*dfgo_error_free_fn)(dfgo_error *);

static dfgo_abi_version_fn p_dfgo_abi_version;
static dfgo_datafusion_version_fn p_dfgo_datafusion_version;
static dfgo_database_open_fn p_dfgo_database_open;
static dfgo_database_close_fn p_dfgo_database_close;
static dfgo_connection_open_isolated_fn p_dfgo_connection_open_isolated;
static dfgo_connection_open_shared_fn p_dfgo_connection_open_shared;
static dfgo_connection_close_fn p_dfgo_connection_close;
static dfgo_connection_register_arrow_ipc_fn p_dfgo_connection_register_arrow_ipc;
static dfgo_connection_register_arrow_stream_fn p_dfgo_connection_register_arrow_stream;
static dfgo_prepare_fn p_dfgo_prepare;
static dfgo_statement_close_fn p_dfgo_statement_close;
static dfgo_statement_num_params_fn p_dfgo_statement_num_params;
static dfgo_statement_serializes_fn p_dfgo_statement_serializes;
static dfgo_cancel_token_create_fn p_dfgo_cancel_token_create;
static dfgo_cancel_token_cancel_fn p_dfgo_cancel_token_cancel;
static dfgo_cancel_token_close_fn p_dfgo_cancel_token_close;
static dfgo_statement_execute_with_params_fn p_dfgo_statement_execute_with_params;
static dfgo_result_export_arrow_stream_fn p_dfgo_result_export_arrow_stream;
static dfgo_result_cancel_fn p_dfgo_result_cancel;
static dfgo_result_close_fn p_dfgo_result_close;
static dfgo_error_message_fn p_dfgo_error_message;
static dfgo_error_kind_fn p_dfgo_error_kind;
static dfgo_error_free_fn p_dfgo_error_free;

static int dfgo_native_uses_dynamic_loader(void) {
	return 1;
}

static const char *dfgo_native_load_error(void) {
	return dfgo_dynamic_error;
}

static void dfgo_set_dynamic_error(const char *prefix, const char *detail) {
	if (detail == NULL) {
		detail = "unknown error";
	}
	snprintf(dfgo_dynamic_error, sizeof(dfgo_dynamic_error), "%s: %s", prefix, detail);
}

static int dfgo_load_symbol(void **slot, const char *name) {
#ifdef _WIN32
	FARPROC symbol = GetProcAddress(dfgo_dynamic_handle, name);
	if (symbol == NULL) {
		snprintf(dfgo_dynamic_error, sizeof(dfgo_dynamic_error), "missing symbol %s", name);
		return -1;
	}
	*slot = (void *)symbol;
#else
	dlerror();
	void *symbol = dlsym(dfgo_dynamic_handle, name);
	const char *err = dlerror();
	if (err != NULL) {
		dfgo_set_dynamic_error("missing symbol", err);
		return -1;
	}
	*slot = symbol;
#endif
	return 0;
}

#define DFGO_LOAD_SYMBOL(name) \
	do { \
		if (dfgo_load_symbol((void **)&p_##name, #name) != 0) { \
			return -1; \
		} \
	} while (0)

static int dfgo_native_load_library(const char *path) {
	if (dfgo_dynamic_handle != NULL) {
		return 0;
	}
	if (path == NULL || path[0] == '\0') {
		dfgo_set_dynamic_error("could not load library", "path is empty");
		return -1;
	}

#ifdef _WIN32
	dfgo_dynamic_handle = LoadLibraryA(path);
	if (dfgo_dynamic_handle == NULL) {
		snprintf(dfgo_dynamic_error, sizeof(dfgo_dynamic_error), "LoadLibraryA failed with error %lu", (unsigned long)GetLastError());
		return -1;
	}
#else
	dfgo_dynamic_handle = dlopen(path, RTLD_NOW | RTLD_LOCAL);
	if (dfgo_dynamic_handle == NULL) {
		dfgo_set_dynamic_error("dlopen failed", dlerror());
		return -1;
	}
#endif

	DFGO_LOAD_SYMBOL(dfgo_abi_version);
	DFGO_LOAD_SYMBOL(dfgo_datafusion_version);
	DFGO_LOAD_SYMBOL(dfgo_database_open);
	DFGO_LOAD_SYMBOL(dfgo_database_close);
	DFGO_LOAD_SYMBOL(dfgo_connection_open_isolated);
	DFGO_LOAD_SYMBOL(dfgo_connection_open_shared);
	DFGO_LOAD_SYMBOL(dfgo_connection_close);
	DFGO_LOAD_SYMBOL(dfgo_connection_register_arrow_ipc);
	DFGO_LOAD_SYMBOL(dfgo_connection_register_arrow_stream);
	DFGO_LOAD_SYMBOL(dfgo_prepare);
	DFGO_LOAD_SYMBOL(dfgo_statement_close);
	DFGO_LOAD_SYMBOL(dfgo_statement_num_params);
	DFGO_LOAD_SYMBOL(dfgo_statement_serializes);
	DFGO_LOAD_SYMBOL(dfgo_cancel_token_create);
	DFGO_LOAD_SYMBOL(dfgo_cancel_token_cancel);
	DFGO_LOAD_SYMBOL(dfgo_cancel_token_close);
	DFGO_LOAD_SYMBOL(dfgo_statement_execute_with_params);
	DFGO_LOAD_SYMBOL(dfgo_result_export_arrow_stream);
	DFGO_LOAD_SYMBOL(dfgo_result_cancel);
	DFGO_LOAD_SYMBOL(dfgo_result_close);
	DFGO_LOAD_SYMBOL(dfgo_error_message);
	DFGO_LOAD_SYMBOL(dfgo_error_kind);
	DFGO_LOAD_SYMBOL(dfgo_error_free);
	return 0;
}

static int32_t dfgo_abi_version(void) {
	return p_dfgo_abi_version();
}

static const char *dfgo_datafusion_version(void) {
	return p_dfgo_datafusion_version();
}

static int dfgo_database_open(const char *dsn, dfgo_database **out, dfgo_error **err) {
	return p_dfgo_database_open(dsn, out, err);
}

static void dfgo_database_close(dfgo_database *db) {
	p_dfgo_database_close(db);
}

static int dfgo_connection_open_isolated(dfgo_database *db, dfgo_connection **out, dfgo_error **err) {
	return p_dfgo_connection_open_isolated(db, out, err);
}

static int dfgo_connection_open_shared(dfgo_database *db, dfgo_connection **out, dfgo_error **err) {
	return p_dfgo_connection_open_shared(db, out, err);
}

static void dfgo_connection_close(dfgo_connection *conn) {
	p_dfgo_connection_close(conn);
}

static int dfgo_connection_register_arrow_ipc(dfgo_connection *conn, const char *name, const uint8_t *data, int64_t len, dfgo_error **err) {
	return p_dfgo_connection_register_arrow_ipc(conn, name, data, len, err);
}

static int dfgo_connection_register_arrow_stream(dfgo_connection *conn, const char *name, struct ArrowArrayStream *stream, dfgo_error **err) {
	return p_dfgo_connection_register_arrow_stream(conn, name, stream, err);
}

static int dfgo_prepare(dfgo_connection *conn, const char *query, dfgo_statement **out, dfgo_error **err) {
	return p_dfgo_prepare(conn, query, out, err);
}

static void dfgo_statement_close(dfgo_statement *stmt) {
	p_dfgo_statement_close(stmt);
}

static int64_t dfgo_statement_num_params(dfgo_statement *stmt) {
	return p_dfgo_statement_num_params(stmt);
}

static int dfgo_statement_serializes(dfgo_statement *stmt) {
	return p_dfgo_statement_serializes(stmt);
}

static int dfgo_cancel_token_create(dfgo_cancel_token **out, dfgo_error **err) {
	return p_dfgo_cancel_token_create(out, err);
}

static void dfgo_cancel_token_cancel(dfgo_cancel_token *token) {
	p_dfgo_cancel_token_cancel(token);
}

static void dfgo_cancel_token_close(dfgo_cancel_token *token) {
	p_dfgo_cancel_token_close(token);
}

static int dfgo_statement_execute_with_params(dfgo_statement *stmt, const dfgo_parameter *params, int64_t params_len, dfgo_cancel_token *token, dfgo_result_stream **out, dfgo_error **err) {
	return p_dfgo_statement_execute_with_params(stmt, params, params_len, token, out, err);
}

static int dfgo_result_export_arrow_stream(dfgo_result_stream *result, struct ArrowArrayStream *out, dfgo_error **err) {
	return p_dfgo_result_export_arrow_stream(result, out, err);
}

static void dfgo_result_cancel(dfgo_result_stream *result) {
	p_dfgo_result_cancel(result);
}

static void dfgo_result_close(dfgo_result_stream *result) {
	p_dfgo_result_close(result);
}

static const char *dfgo_error_message(const dfgo_error *err) {
	return p_dfgo_error_message(err);
}

static const char *dfgo_error_kind(const dfgo_error *err) {
	return p_dfgo_error_kind(err);
}

static void dfgo_error_free(dfgo_error *err) {
	p_dfgo_error_free(err);
}

#undef DFGO_LOAD_SYMBOL

#endif
