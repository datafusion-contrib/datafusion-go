package datafusion

import (
	"database/sql/driver"
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/arrio"
)

type result struct {
	rowsAffected int64
}

func (r result) LastInsertId() (int64, error) {
	return 0, nil
}

func (r result) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

func execResult(reader arrio.Reader) (driver.Result, error) {
	defer closeReader(reader)

	var rowsAffected int64
	var countResult bool
	var sawBatch bool
	for {
		rec, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				return result{rowsAffected: rowsAffected}, nil
			}
			return nil, driverError(ErrorExecute, "could not execute DataFusion statement", err)
		}

		count, ok, err := rowsAffectedFromRecord(rec)
		rec.Release()
		if err != nil {
			return nil, driverError(ErrorExecute, "could not read DataFusion rows affected count", err)
		}
		if !sawBatch {
			countResult = ok
			sawBatch = true
		}
		if !countResult {
			// The deferred closeReader cancels the native stream before releasing it.
			return result{}, nil
		}
		if !ok {
			continue
		}
		if count > 0 && rowsAffected > math.MaxInt64-count {
			return nil, driverError(ErrorExecute, "DataFusion rows affected count overflowed int64", nil)
		}
		if count < 0 && rowsAffected < math.MinInt64-count {
			return nil, driverError(ErrorExecute, "DataFusion rows affected count overflowed int64", nil)
		}
		rowsAffected += count
	}
}

func rowsAffectedFromRecord(rec arrow.RecordBatch) (int64, bool, error) {
	if rec.NumCols() != 1 {
		return 0, false, nil
	}
	field := rec.Schema().Field(0)
	if !isRowsAffectedColumn(field.Name) {
		return 0, false, nil
	}
	return sumIntegerArray(rec.Column(0))
}

func isRowsAffectedColumn(name string) bool {
	switch strings.ToLower(name) {
	case "count", "rows_affected", "rowsaffected":
		return true
	default:
		return false
	}
}

func sumIntegerArray(values arrow.Array) (int64, bool, error) {
	var total int64
	for row := 0; row < values.Len(); row++ {
		if values.IsNull(row) {
			continue
		}
		value, err := integerValue(values, row)
		if err != nil {
			return 0, false, err
		}
		if value > 0 && total > math.MaxInt64-value {
			return 0, false, fmt.Errorf("count column overflowed int64")
		}
		if value < 0 && total < math.MinInt64-value {
			return 0, false, fmt.Errorf("count column overflowed int64")
		}
		total += value
	}
	return total, true, nil
}

func integerValue(values arrow.Array, row int) (int64, error) {
	switch arr := values.(type) {
	case *array.Int8:
		return int64(arr.Value(row)), nil
	case *array.Int16:
		return int64(arr.Value(row)), nil
	case *array.Int32:
		return int64(arr.Value(row)), nil
	case *array.Int64:
		return arr.Value(row), nil
	case *array.Uint8:
		return int64(arr.Value(row)), nil
	case *array.Uint16:
		return int64(arr.Value(row)), nil
	case *array.Uint32:
		return int64(arr.Value(row)), nil
	case *array.Uint64:
		value := arr.Value(row)
		if value > uint64(math.MaxInt64) {
			return 0, fmt.Errorf("count value %d exceeds int64 range", value)
		}
		return int64(value), nil
	default:
		return 0, fmt.Errorf("count column has non-integer Arrow type %s", values.DataType())
	}
}

var _ driver.Result = result{}
