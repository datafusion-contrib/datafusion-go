//go:build datafusion_test_sqllogic && cgo

package datafusion

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/datafusion-contrib/datafusion-go/internal/native"
)

// TestSQLLogic runs all upstream files. Test selection is handled by go test's
// normal -run filter; a missing corpus is an error, never a skipped green test.
func TestSQLLogic(t *testing.T) {
	manifest, err := os.ReadFile("testdata/sqllogictest/upstream.json")
	if err != nil {
		t.Fatal(err)
	}
	var inventory struct {
		CommentOnly []string          `json:"comment_only_files"`
		Files       map[string]string `json:"files"`
	}
	if err := json.Unmarshal(manifest, &inventory); err != nil {
		t.Fatal(err)
	}
	driverManifest, err := os.ReadFile("testdata/sqllogictest/driver.json")
	if err != nil {
		t.Fatal(err)
	}
	var driverFiles map[string]string
	if err := json.Unmarshal(driverManifest, &driverFiles); err != nil {
		t.Fatal(err)
	}
	for name, checksum := range driverFiles {
		inventory.Files["_driver/"+name] = checksum
	}
	commentOnly := make(map[string]bool, len(inventory.CommentOnly))
	for _, name := range inventory.CommentOnly {
		commentOnly[name] = true
	}
	source := os.Getenv("DFGO_SQLLOGICTEST_SOURCE")
	if source == "" {
		t.Fatal("DFGO_SQLLOGICTEST_SOURCE is required; use make test.sqllogic")
	}
	source, err = filepath.Abs(source)
	if err != nil {
		t.Fatal(err)
	}
	suite := filepath.Join(source, "datafusion", "sqllogictest")
	t.Chdir(suite)
	for name, checksum := range inventory.Files {
		data, err := os.ReadFile(filepath.Join("test_files", filepath.FromSlash(name)))
		if err != nil {
			t.Fatal(err)
		}
		if fmt.Sprintf("%x", sha256.Sum256(data)) != checksum {
			t.Fatalf("SQLLogicTest fixture differs from its reviewed checksum: %s", name)
		}
	}
	var files []string
	err = filepath.WalkDir("test_files", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && strings.HasSuffix(path, ".slt") {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatal("SQLLogicTest corpus is empty")
	}
	for _, file := range files {
		name := strings.TrimPrefix(filepath.ToSlash(file), "test_files/")
		if _, exists := inventory.Files[name]; !exists {
			t.Fatalf("unreviewed SQLLogicTest file: %s", name)
		}
		t.Run(name, func(t *testing.T) {
			var cleanup func()
			db, err := sql.Open("datafusion", "")
			if err != nil {
				t.Fatal(err)
			}
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
			err = conn.Raw(func(driverConn any) error {
				var setupErr error
				cleanup, setupErr = driverConn.(*Conn).conn.SetupSQLLogicTest(name)
				return setupErr
			})
			if err != nil {
				t.Fatal(err)
			}
			executed := 0
			report, err := native.RunSQLLogicTest(file, source, func(query string) (array.RecordReader, error) {
				executed++
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
				defer cancel()
				data, err := sqlLogicArrow(ctx, conn, query)
				var nativeError *native.Error
				if errors.As(err, &nativeError) {
					if nativeError.Kind == "panic" {
						return nil, &native.SQLLogicHarnessError{Err: err}
					}
					// Match upstream's error wrapper; preserve the engine message.
					return nil, errors.New("DataFusion error: " + nativeError.Message)
				}
				if err != nil && sqlLogicStreamError.MatchString(err.Error()) {
					return nil, errors.New("DataFusion error: " + sqlLogicStreamError.ReplaceAllString(err.Error(), ""))
				}
				if err != nil {
					return nil, &native.SQLLogicHarnessError{Err: err}
				}
				return data, err
			})
			if err != nil {
				t.Fatal(err)
			}
			if executed != report.Eligible {
				report.Errors = append(report.Errors, "SQL callback count differs from eligible upstream records")
			}
			if (report.Eligible == 0) != commentOnly[name] {
				report.Errors = append(report.Errors, "executable SQL presence differs from the reviewed upstream inventory")
			}
			if directory := os.Getenv("DFGO_SQLLOGICTEST_REPORT"); directory != "" {
				data, err := json.MarshalIndent(struct {
					File     string `json:"file"`
					Executed int    `json:"executed"`
					native.SQLLogicReport
				}{name, executed, report}, "", "  ")
				if err != nil {
					t.Fatal(err)
				}
				path := filepath.Join(directory, name+".json")
				if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, data, 0o644); err != nil {
					t.Fatal(err)
				}
			}
			t.Logf("%d statements, %d queries, %d executions", report.Statements, report.Queries, executed)
			for _, message := range report.Errors {
				t.Error(message)
			}
		})
	}
}

// Arrow's C stream adds its own transport prefix around DataFusion's message.
var sqlLogicStreamError = regexp.MustCompile(`^arrow stream failed with errno [0-9]+: External error: `)

func sqlLogicArrow(ctx context.Context, conn *sql.Conn, query string) (array.RecordReader, error) {
	reader, err := QueryArrowContext(ctx, conn, query)
	if err != nil {
		return nil, err
	}
	defer closeReader(reader)
	var batches []arrow.RecordBatch
	defer func() {
		for _, batch := range batches {
			batch.Release()
		}
	}()
	for {
		batch, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		batches = append(batches, batch)
	}
	if err := reader.Close(); err != nil {
		return nil, err
	}
	return array.NewRecordReader(reader.Schema(), batches)
}
