#ifndef DFGO_SQLLOGICTEST_H
#define DFGO_SQLLOGICTEST_H

#include <stdint.h>
#include <stdlib.h>
#include "../../rust/include/datafusion_go.h"

/* Test-only symbols are resolved from the same library that the production
 * loader has already checked and loaded. They are absent from release builds. */
#ifdef _WIN32
#include <windows.h>
static void *dfgo_slt_symbol(const char *path, const char *name) {
    int size = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, path, -1, NULL, 0);
    if (size <= 0) return NULL;
    wchar_t *wide = (wchar_t *)malloc((size_t)size * sizeof(wchar_t));
    if (wide == NULL) return NULL;
    MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, path, -1, wide, size);
    HMODULE module = GetModuleHandleW(wide);
    free(wide);
    return module == NULL ? NULL : (void *)GetProcAddress(module, name);
}
#else
#include <dlfcn.h>
static void *dfgo_slt_symbol(const char *path, const char *name) {
    void *module = dlopen(path, RTLD_NOW | RTLD_LOCAL | RTLD_NOLOAD);
    if (module == NULL) return NULL;
    void *symbol = dlsym(module, name);
    dlclose(module);
    return symbol;
}
#endif

typedef int (*dfgo_slt_query)(uintptr_t, char *, size_t, uint8_t **, size_t *);
static int (*dfgo_slt_setup_fn)(dfgo_connection *, const char *, void **, dfgo_error **);
static void (*dfgo_slt_context_free_fn)(void *);
static int (*dfgo_slt_run_fn)(const char *, const char *, uintptr_t, dfgo_slt_query,
                            void (*)(void *), char **, dfgo_error **);
static void (*dfgo_slt_free_fn)(char *);

static int dfgo_slt_load(const char *path) {
    dfgo_slt_setup_fn = (void *)dfgo_slt_symbol(path, "dfgo_test_slt_setup");
    dfgo_slt_context_free_fn = (void *)dfgo_slt_symbol(path, "dfgo_test_slt_context_free");
    dfgo_slt_run_fn = (void *)dfgo_slt_symbol(path, "dfgo_test_slt_run");
    dfgo_slt_free_fn = (void *)dfgo_slt_symbol(path, "dfgo_test_slt_free");
    return dfgo_slt_setup_fn && dfgo_slt_context_free_fn && dfgo_slt_run_fn && dfgo_slt_free_fn;
}

extern int dfgoGoSLTQuery(uintptr_t, char *, size_t, uint8_t **, size_t *);

static int dfgo_slt_setup(dfgo_connection *connection, const char *path, void **context, dfgo_error **err) {
    return dfgo_slt_setup_fn(connection, path, context, err);
}
static void dfgo_slt_context_free(void *context) { dfgo_slt_context_free_fn(context); }
static void dfgo_slt_free(char *text) { dfgo_slt_free_fn(text); }
static void dfgo_slt_stream_free(struct ArrowArrayStream *stream) {
    if (stream->release != NULL) stream->release(stream);
    free(stream);
}
static int dfgo_slt_run(const char *path, const char *workspace, uintptr_t handle, char **report, dfgo_error **err) {
    return dfgo_slt_run_fn(path, workspace, handle, dfgoGoSLTQuery, free, report, err);
}

#endif
