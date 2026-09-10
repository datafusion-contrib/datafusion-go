//go:build cgo

package datafusion

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/datafusion-contrib/datafusion-go/internal/conformance"
)

func TestQueryCorpus(t *testing.T) {
	cases, err := conformance.Load(os.DirFS("testdata/conformance"))
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		for _, mode := range []string{"direct", "prepared", "arrow"} {
			for _, shared := range []bool{true, false} {
				t.Run(fmt.Sprintf("%s/%s/shared=%t", c.Name, mode, shared), func(t *testing.T) {
					connector, err := NewConnectorWithInitContext("", nil, WithSharedSession(shared))
					if err != nil {
						t.Fatal(err)
					}
					db := sql.OpenDB(connector)
					defer closeNoError(t, db)
					conn, err := db.Conn(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					defer closeNoError(t, conn)
					for _, setup := range c.Setup {
						if _, err := conn.ExecContext(context.Background(), setup); err != nil {
							t.Fatal(err)
						}
					}
					args := corpusParameters(t, c.Parameters)
					var got [][]*string
					if mode == "arrow" {
						got, err = corpusArrow(t, conn, c, args)
					} else {
						got, err = corpusSQL(t, conn, c, args, mode == "prepared")
					}
					if c.Error != "" {
						if err == nil || !strings.Contains(err.Error(), c.Error) {
							t.Fatalf("got error %v, want text %q", err, c.Error)
						}
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					if len(got) != len(c.Rows) || (len(got) != 0 && !reflect.DeepEqual(got, c.Rows)) {
						t.Fatalf("rows: got %s, want %s", corpusRows(got), corpusRows(c.Rows))
					}
				})
			}
		}
	}
}

func corpusParameters(t *testing.T, params []conformance.Parameter) []any {
	t.Helper()
	args := make([]any, len(params))
	for i, p := range params {
		var value any
		switch p.Type {
		case "int64":
			n, err := strconv.ParseInt(p.Value, 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			value = n
		case "string":
			value = p.Value
		case "uint64":
			n, err := strconv.ParseUint(p.Value, 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			value = UInt64(n)
		case "float64":
			n, err := strconv.ParseFloat(p.Value, 64)
			if err != nil {
				t.Fatal(err)
			}
			value = n
		case "bool":
			b, err := strconv.ParseBool(p.Value)
			if err != nil {
				t.Fatal(err)
			}
			value = b
		case "binary":
			b, err := hex.DecodeString(p.Value)
			if err != nil {
				t.Fatal(err)
			}
			value = b
		case "null_int64":
			value = NullOf(ParameterInt64)
		default:
			t.Fatalf("unsupported corpus parameter type %q", p.Type)
		}
		if p.Name != "" {
			value = sql.Named(p.Name, value)
		}
		args[i] = value
	}
	return args
}

func corpusSQL(t *testing.T, conn *sql.Conn, c conformance.Case, args []any, prepared bool) ([][]*string, error) {
	t.Helper()
	ctx := context.Background()
	query := conn.QueryContext
	if prepared {
		stmt, err := conn.PrepareContext(ctx, c.SQL)
		if err != nil {
			return nil, err
		}
		defer closeNoError(t, stmt)
		query = func(ctx context.Context, _ string, args ...any) (*sql.Rows, error) {
			return stmt.QueryContext(ctx, args...)
		}
	}
	rows, err := query(ctx, c.SQL, args...)
	if err != nil {
		return nil, err
	}
	defer closeNoError(t, rows)
	columns, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	if c.Error == "" {
		if len(columns) != len(c.Columns) {
			t.Fatalf("got %d columns, want %d", len(columns), len(c.Columns))
		}
		for i, column := range columns {
			want := c.Columns[i]
			nullable, ok := column.Nullable()
			if column.Name() != want.Name || !ok || nullable != want.Nullable {
				t.Errorf("column %d: name=%q nullable=%t/%t, want %+v", i, column.Name(), nullable, ok, want)
			}
			if want.SQLType != "" && column.DatabaseTypeName() != want.SQLType {
				t.Errorf("column %d: type=%q, want %q", i, column.DatabaseTypeName(), want.SQLType)
			}
			if want.Precision != 0 {
				precision, scale, ok := column.DecimalSize()
				if !ok || precision != want.Precision || scale != want.Scale {
					t.Errorf("column %d: precision/scale=%d/%d/%t, want %d/%d", i, precision, scale, ok, want.Precision, want.Scale)
				}
			}
		}
	}
	var result [][]*string
	for rows.Next() {
		values := make([]any, len(columns))
		dest := make([]any, len(columns))
		for i := range values {
			dest[i] = &values[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}
		row := make([]*string, len(values))
		for i, value := range values {
			if value != nil {
				cell := fmt.Sprint(value)
				if date, ok := value.(time.Time); ok && c.Columns[i].Type == "date32" {
					cell = date.Format("2006-01-02")
				}
				row[i] = &cell
			}
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

func corpusArrow(t *testing.T, conn *sql.Conn, c conformance.Case, args []any) ([][]*string, error) {
	t.Helper()
	reader, err := QueryArrowContext(context.Background(), conn, c.SQL, args...)
	if err != nil {
		return nil, err
	}
	defer closeNoError(t, reader)
	if c.Error == "" {
		fields := reader.Schema().Fields()
		if len(fields) != len(c.Columns) {
			t.Fatalf("got %d Arrow fields, want %d", len(fields), len(c.Columns))
		}
		for i, field := range fields {
			want := c.Columns[i]
			typeName := field.Type.Name()
			if dt, ok := field.Type.(arrow.DecimalType); ok {
				typeName = fmt.Sprintf("decimal128(%d,%d)", dt.GetPrecision(), dt.GetScale())
			}
			if field.Name != want.Name || typeName != want.Type || field.Nullable != want.Nullable {
				t.Errorf("field %d: got %s/%s/nullable=%t, want %+v", i, field.Name, typeName, field.Nullable, want)
			}
		}
	}
	var result [][]*string
	for {
		rec, err := reader.Read()
		if err == io.EOF {
			return result, nil
		}
		if err != nil {
			return nil, err
		}
		for r := 0; r < int(rec.NumRows()); r++ {
			row := make([]*string, rec.NumCols())
			for i, arr := range rec.Columns() {
				if !arr.IsNull(r) {
					// String arrays may return a view into the native batch. The
					// corpus retains values after releasing that batch.
					cell := strings.Clone(arr.ValueStr(r))
					row[i] = &cell
				}
			}
			result = append(result, row)
		}
		rec.Release()
	}
}

func corpusRows(rows [][]*string) string {
	var out strings.Builder
	for _, row := range rows {
		out.WriteString("[")
		for _, cell := range row {
			if cell == nil {
				out.WriteString("null ")
			} else {
				fmt.Fprintf(&out, "%q ", *cell)
			}
		}
		out.WriteString("]")
	}
	return out.String()
}
