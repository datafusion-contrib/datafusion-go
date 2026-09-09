// Package conformance describes the query fixtures shared by the Go, Rust, and
// SQLite test runners. It is used by tests and development tools only.
package conformance

import (
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
)

type Parameter struct {
	Name  string `json:"name,omitempty"`
	Type  string `json:"type"`
	Value string `json:"value,omitempty"`
}

type Column struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	Nullable  bool   `json:"nullable"`
	SQLType   string `json:"sql_type,omitempty"`
	Precision int64  `json:"precision,omitempty"`
	Scale     int64  `json:"scale,omitempty"`
}

type Case struct {
	Name       string      `json:"name"`
	SQL        string      `json:"sql"`
	NativeSQL  string      `json:"native_sql,omitempty"`
	Setup      []string    `json:"setup,omitempty"`
	Parameters []Parameter `json:"parameters,omitempty"`
	Columns    []Column    `json:"columns,omitempty"`
	Rows       [][]*string `json:"rows,omitempty"`
	Error      string      `json:"error,omitempty"`
	SQLite     bool        `json:"sqlite,omitempty"`
}

func Load(files fs.FS) ([]Case, error) {
	paths, err := fs.Glob(files, "*.json")
	if err != nil {
		return nil, err
	}
	var cases []Case
	names := make(map[string]bool)
	for _, path := range paths {
		file, err := files.Open(path)
		if err != nil {
			return nil, err
		}
		var batch []Case
		decoder := json.NewDecoder(file)
		decoder.DisallowUnknownFields()
		err = decoder.Decode(&batch)
		if err == nil {
			var extra any
			if end := decoder.Decode(&extra); end != io.EOF {
				err = fmt.Errorf("expected end of file after case array, got %v", end)
			}
		}
		closeErr := file.Close()
		if err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		if closeErr != nil {
			return nil, closeErr
		}
		for _, c := range batch {
			if c.Name == "" || c.SQL == "" || names[c.Name] {
				return nil, fmt.Errorf("%s: empty or duplicate case name, or empty SQL: %q", path, c.Name)
			}
			for _, row := range c.Rows {
				if len(row) != len(c.Columns) {
					return nil, fmt.Errorf("%s: %s: row has %d values for %d columns", path, c.Name, len(row), len(c.Columns))
				}
			}
			names[c.Name] = true
		}
		cases = append(cases, batch...)
	}
	if len(cases) == 0 {
		return nil, fmt.Errorf("query corpus is empty")
	}
	return cases, nil
}
