// Package datafusion provides a database/sql driver backed by Apache DataFusion.
//
// The driver registers as "datafusion". It is intended for in-process analytic
// SQL over DataFusion's memory/session catalog and Arrow execution engine.
// Standard database/sql row scanning is supported for scalar Arrow types, and
// QueryArrowContext exposes native Arrow record batches for callers that need
// exact Arrow schemas or complex values. RegisterArrowReader registers Go Arrow
// record readers as DataFusion in-memory tables.
package datafusion
