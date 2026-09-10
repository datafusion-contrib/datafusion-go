//go:build datafusion_test_sqllogic && cgo

package datafusion

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/datafusion-contrib/datafusion-go/internal/native"
)

func TestSQLLogicHarness(t *testing.T) {
	t.Chdir(t.TempDir())
	db, err := sql.Open("datafusion", "")
	if err != nil {
		t.Fatal(err)
	}
	var cleanup func()
	t.Cleanup(func() {
		closeNoError(t, db)
		if cleanup != nil {
			cleanup()
		}
	})
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeNoError(t, conn) })
	if err := conn.Raw(func(driverConn any) error {
		var setupErr error
		cleanup, setupErr = driverConn.(*Conn).conn.SetupSQLLogicTest("harness.slt")
		return setupErr
	}); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name, script string
		callback     func(string) (array.RecordReader, error)
		wantFailure  bool
	}{
		{name: "correct rows", script: "query I\nSELECT 1\n----\n1\n"},
		{name: "wrong rows", script: "query I\nSELECT 1\n----\n2\n", wantFailure: true},
		{name: "wrong types", script: "query T\nSELECT 1\n----\n1\n", wantFailure: true},
		{name: "rowsort", script: "query I rowsort\nVALUES (2), (1)\n----\n1\n2\n"},
		{name: "missing include", script: "include absent.slt\n", wantFailure: true},
		{name: "unknown directive", script: "unsupported command\n", wantFailure: true},
		{name: "unknown engine", script: "onlyif Typo\nquery I\nSELECT 1\n----\n1\n", wantFailure: true},
		{name: "named connection", script: "connection second\nquery I\nSELECT 1\n----\n1\n", wantFailure: true},
		{name: "halt", script: "halt\n\nquery I\nSELECT 1\n----\n2\n", wantFailure: true},
		{name: "expected SQL error", script: "statement error expected\nSELECT 1\n", callback: func(string) (array.RecordReader, error) {
			return nil, errors.New("expected")
		}},
		{name: "adapter error cannot satisfy SQL error", script: "statement error expected\nSELECT 1\n", wantFailure: true, callback: func(string) (array.RecordReader, error) {
			return nil, &native.SQLLogicHarnessError{Err: errors.New("expected")}
		}},
		{name: "panic cannot satisfy SQL error", script: "statement error expected\nSELECT 1\n", wantFailure: true, callback: func(string) (array.RecordReader, error) {
			panic("expected")
		}},
		{name: "native stream panic cannot satisfy SQL error", script: "statement error .*\nSELECT 1\n", wantFailure: true, callback: func(string) (array.RecordReader, error) {
			return nil, sqlLogicError(errors.New("arrow stream failed with errno 5: External error: panic while reading query results across datafusion-go native boundary"))
		}},
		{name: "native stream SQL error keeps engine message", script: "statement error DataFusion error: expected\nSELECT 1\n", callback: func(string) (array.RecordReader, error) {
			return nil, sqlLogicError(errors.New("arrow stream failed with errno 5: External error: expected"))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "case.slt")
			if err := os.WriteFile(path, []byte(tc.script), 0o600); err != nil {
				t.Fatal(err)
			}
			callback := tc.callback
			if callback == nil {
				callback = func(query string) (array.RecordReader, error) {
					return sqlLogicArrow(context.Background(), conn, query)
				}
			}
			report, err := native.RunSQLLogicTest(path, filepath.Dir(path), callback)
			failed := err != nil || len(report.Errors) != 0
			if failed != tc.wantFailure {
				t.Fatalf("failure=%v, want %v: err=%v, report=%+v", failed, tc.wantFailure, err, report)
			}
			if !failed && report.Passed != report.Eligible {
				t.Fatalf("successful assertions were not counted: %+v", report)
			}
		})
	}
}
