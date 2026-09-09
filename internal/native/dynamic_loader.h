#ifndef DATAFUSION_GO_DYNAMIC_LOADER_H
#define DATAFUSION_GO_DYNAMIC_LOADER_H

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
/* Available since Windows 8 / KB2533623; define for older toolchain headers. */
#ifndef LOAD_LIBRARY_SEARCH_SYSTEM32
#define LOAD_LIBRARY_SEARCH_SYSTEM32 0x00000800
#endif
#ifndef LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR
#define LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR 0x00000100
#endif
static HMODULE dfgo_dynamic_handle = NULL;
#else
#include <dlfcn.h>
static void *dfgo_dynamic_handle = NULL;
#endif

static char dfgo_dynamic_error[1024];

#include "abi_generated.h"

#define DFGO_DECLARE(result, name, args, values, ret) \
  typedef result (*name##_fn) args; \
  static name##_fn p_##name;
DFGO_FUNCTIONS(DFGO_DECLARE)
#undef DFGO_DECLARE

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
	/* Preserve UTF-8 paths and restrict dependencies to the DLL's directory
	 * and System32. */
	{
		int wide_len = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, path, -1, NULL, 0);
		wchar_t *wide = NULL;
		if (wide_len <= 0) {
			dfgo_set_dynamic_error("could not load library", "path is not valid UTF-8");
			return -1;
		}
		wide = (wchar_t *)malloc((size_t)wide_len * sizeof(wchar_t));
		if (wide == NULL) {
			dfgo_set_dynamic_error("could not load library", "out of memory");
			return -1;
		}
		MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, path, -1, wide, wide_len);
		dfgo_dynamic_handle = LoadLibraryExW(wide, NULL,
			LOAD_LIBRARY_SEARCH_SYSTEM32 | LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR);
		free(wide);
	}
	if (dfgo_dynamic_handle == NULL) {
		snprintf(dfgo_dynamic_error, sizeof(dfgo_dynamic_error), "LoadLibraryExW failed with error %lu", (unsigned long)GetLastError());
		return -1;
	}
#else
	dfgo_dynamic_handle = dlopen(path, RTLD_NOW | RTLD_LOCAL);
	if (dfgo_dynamic_handle == NULL) {
		dfgo_set_dynamic_error("dlopen failed", dlerror());
		return -1;
	}
#endif

#define DFGO_LOAD(result, name, args, values, ret) DFGO_LOAD_SYMBOL(name);
	DFGO_FUNCTIONS(DFGO_LOAD)
#undef DFGO_LOAD
	return 0;
}

#define DFGO_WRAPPER(result, name, args, values, ret) \
  static result name args { ret p_##name values; }
DFGO_FUNCTIONS(DFGO_WRAPPER)
#undef DFGO_WRAPPER

#undef DFGO_LOAD_SYMBOL

#endif
