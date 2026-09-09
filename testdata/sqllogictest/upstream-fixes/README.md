# Prepared upstream fixes

`arrow-go-zero-size-list.patch` targets Apache Arrow Go v18.8.0, commit
`72e9a5695337bab9ac01ec1e4d58177bc4949dc3`. It permits zero-length fixed-size
list types, imports their C schemas, and validates their arrays without dividing
by zero. Negative lengths and overflowing schema lengths remain invalid.
Tests cover constructors, null and empty lists, sliced lists, nested element
types, C export/import, and checked allocator cleanup.

Validated in an isolated checkout:

```sh
go test ./arrow ./arrow/array ./arrow/ipc
go test -tags=test ./arrow/cdata
```

The DataFusion `arrow_typeof.slt` file also passes all 62 records when an
isolated Go module file selects this patched checkout. Both zero-length list
queries fail with the unmodified v18.8.0 release.

This patch is not applied by the project build and has not been submitted
upstream. Production still uses the released Arrow Go dependency. A local
`replace` directive would not fix downstream users of this module, so it is
not a release solution. Integrate an upstream version containing the fix
before claiming those two assertions pass for consumers.
