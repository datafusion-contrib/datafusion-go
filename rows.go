package datafusion

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"math"
	"reflect"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/arrio"
)

type rows struct {
	reader arrio.Reader

	current arrow.RecordBatch
	row     int

	columns   []string
	scanTypes []reflect.Type
	dbTypes   []string
	nullable  []bool
	lengths   []columnLength
	scales    []columnPrecisionScale

	closed bool
}

type columnLength struct {
	value int64
	ok    bool
}

type columnPrecisionScale struct {
	precision int64
	scale     int64
	ok        bool
}

func newRows(reader arrio.Reader) (*rows, error) {
	r := &rows{reader: reader}
	if schemaReader, ok := reader.(interface{ Schema() *arrow.Schema }); ok {
		if schema := schemaReader.Schema(); schema != nil {
			if err := r.initMetadata(schema); err != nil {
				return nil, err
			}
		}
	}
	return r, nil
}

func (r *rows) Columns() []string {
	if err := r.ensureMetadata(); err != nil {
		return nil
	}
	out := make([]string, len(r.columns))
	copy(out, r.columns)
	return out
}

func (r *rows) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	closeReader(r.reader)
	return nil
}

func (r *rows) Next(dst []driver.Value) error {
	if r.closed {
		return driverError(ErrorClosed, "datafusion rows are closed", nil)
	}

	for r.current == nil || r.row >= int(r.current.NumRows()) {
		if r.current != nil {
			r.current.Release()
			r.current = nil
		}

		rec, err := r.reader.Read()
		if err != nil {
			if err == io.EOF {
				closeReader(r.reader)
				return io.EOF
			}
			return driverError(ErrorScan, "could not read DataFusion record batch", err)
		}
		if r.columns == nil {
			if err := r.initMetadata(rec.Schema()); err != nil {
				rec.Release()
				return driverError(ErrorScan, "could not read DataFusion schema", err)
			}
		}
		if rec.NumRows() == 0 {
			rec.Release()
			continue
		}

		r.current = rec
		r.row = 0
	}

	for col := range dst {
		value, err := arrowValue(r.current.Column(col), r.row)
		if err != nil {
			return driverError(ErrorScan, fmt.Sprintf("could not scan column %d", col), err)
		}
		dst[col] = value
	}

	r.row++
	return nil
}

func (r *rows) ColumnTypeScanType(index int) reflect.Type {
	if err := r.ensureMetadata(); err != nil || index < 0 || index >= len(r.scanTypes) {
		return nil
	}
	return r.scanTypes[index]
}

func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	if err := r.ensureMetadata(); err != nil || index < 0 || index >= len(r.dbTypes) {
		return ""
	}
	return r.dbTypes[index]
}

func (r *rows) ColumnTypeNullable(index int) (nullable, ok bool) {
	if err := r.ensureMetadata(); err != nil || index < 0 || index >= len(r.nullable) {
		return false, false
	}
	return r.nullable[index], true
}

func (r *rows) ColumnTypeLength(index int) (length int64, ok bool) {
	if err := r.ensureMetadata(); err != nil || index < 0 || index >= len(r.lengths) {
		return 0, false
	}
	return r.lengths[index].value, r.lengths[index].ok
}

func (r *rows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	if err := r.ensureMetadata(); err != nil || index < 0 || index >= len(r.scales) {
		return 0, 0, false
	}
	info := r.scales[index]
	return info.precision, info.scale, info.ok
}

func (r *rows) HasNextResultSet() bool {
	return false
}

func (r *rows) NextResultSet() error {
	return io.EOF
}

func (r *rows) ensureMetadata() error {
	if r.columns != nil {
		return nil
	}

	rec, err := r.reader.Read()
	if err != nil {
		if err == io.EOF {
			if err := r.initMetadata(arrow.NewSchema(nil, nil)); err != nil {
				return err
			}
			return nil
		}
		return err
	}
	if err := r.initMetadata(rec.Schema()); err != nil {
		rec.Release()
		return err
	}
	r.current = rec
	r.row = 0
	return nil
}

func (r *rows) initMetadata(schema *arrow.Schema) error {
	fields := schema.Fields()
	for _, field := range fields {
		if err := checkDatabaseSQLType(field.Type); err != nil {
			return fmt.Errorf("column %q: %w", field.Name, err)
		}
	}

	r.columns = make([]string, len(fields))
	r.scanTypes = make([]reflect.Type, len(fields))
	r.dbTypes = make([]string, len(fields))
	r.nullable = make([]bool, len(fields))
	r.lengths = make([]columnLength, len(fields))
	r.scales = make([]columnPrecisionScale, len(fields))

	for i, field := range fields {
		r.columns[i] = field.Name
		r.scanTypes[i] = scanType(field.Type, field.Nullable)
		r.dbTypes[i] = databaseTypeName(field.Type)
		r.nullable[i] = field.Nullable
		length, lengthOK := columnTypeLength(field.Type)
		r.lengths[i] = columnLength{value: length, ok: lengthOK}
		precision, scale, scaleOK := columnTypePrecisionScale(field.Type)
		r.scales[i] = columnPrecisionScale{precision: precision, scale: scale, ok: scaleOK}
	}
	return nil
}

func arrowValue(values arrow.Array, row int) (driver.Value, error) {
	if values.IsNull(row) {
		return nil, nil
	}

	switch arr := values.(type) {
	case *array.Null:
		return nil, nil
	case *array.Boolean:
		return arr.Value(row), nil
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
			return nil, fmt.Errorf("uint64 value %d exceeds int64 range", value)
		}
		return int64(value), nil
	case *array.Float16:
		return float64(arr.Value(row).Float32()), nil
	case *array.Float32:
		return float64(arr.Value(row)), nil
	case *array.Float64:
		return arr.Value(row), nil
	case *array.String:
		return strings.Clone(arr.Value(row)), nil
	case *array.LargeString:
		return strings.Clone(arr.Value(row)), nil
	case *array.StringView:
		return strings.Clone(arr.Value(row)), nil
	case *array.Binary:
		return copyBytes(arr.Value(row)), nil
	case *array.LargeBinary:
		return copyBytes(arr.Value(row)), nil
	case *array.FixedSizeBinary:
		return copyBytes(arr.Value(row)), nil
	case *array.BinaryView:
		return copyBytes(arr.Value(row)), nil
	case *array.Date32:
		return arr.Value(row).ToTime(), nil
	case *array.Date64:
		return arr.Value(row).ToTime(), nil
	case *array.Time32:
		unit := arr.DataType().(*arrow.Time32Type).Unit
		return arr.Value(row).ToTime(unit), nil
	case *array.Time64:
		unit := arr.DataType().(*arrow.Time64Type).Unit
		return arr.Value(row).ToTime(unit), nil
	case *array.Timestamp:
		toTime, err := arr.DataType().(*arrow.TimestampType).GetToTimeFunc()
		if err != nil {
			return nil, err
		}
		return toTime(arr.Value(row)), nil
	case *array.Duration:
		unit := arr.DataType().(*arrow.DurationType).Unit
		return durationNanos(int64(arr.Value(row)), int64(unit.Multiplier()))
	case *array.Decimal32:
		return arr.ValueStr(row), nil
	case *array.Decimal64:
		return arr.ValueStr(row), nil
	case *array.Decimal128:
		return arr.ValueStr(row), nil
	case *array.Decimal256:
		return arr.ValueStr(row), nil
	case *array.MonthInterval:
		return fmt.Sprintf("%d months", int32(arr.Value(row))), nil
	case *array.DayTimeInterval:
		value := arr.Value(row)
		return fmt.Sprintf("%d days %d milliseconds", value.Days, value.Milliseconds), nil
	case *array.MonthDayNanoInterval:
		value := arr.Value(row)
		return fmt.Sprintf("%d months %d days %d nanoseconds", value.Months, value.Days, value.Nanoseconds), nil
	default:
		return nil, fmt.Errorf("unsupported Arrow type %s", values.DataType())
	}
}

func copyBytes(in []byte) []byte {
	out := make([]byte, len(in))
	copy(out, in)
	return out
}

func durationNanos(value, multiplier int64) (int64, error) {
	if multiplier <= 0 {
		return 0, fmt.Errorf("duration unit multiplier must be positive, got %d", multiplier)
	}
	if value > 0 && value > math.MaxInt64/multiplier {
		return 0, fmt.Errorf("duration value %d overflows int64 nanoseconds", value)
	}
	if value < 0 && value < math.MinInt64/multiplier {
		return 0, fmt.Errorf("duration value %d overflows int64 nanoseconds", value)
	}
	return value * multiplier, nil
}

func checkDatabaseSQLType(dt arrow.DataType) error {
	switch dt.ID() {
	case arrow.NULL,
		arrow.BOOL,
		arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64,
		arrow.FLOAT16, arrow.FLOAT32, arrow.FLOAT64,
		arrow.STRING, arrow.LARGE_STRING, arrow.STRING_VIEW,
		arrow.BINARY, arrow.LARGE_BINARY, arrow.FIXED_SIZE_BINARY, arrow.BINARY_VIEW,
		arrow.DATE32, arrow.DATE64,
		arrow.TIME32, arrow.TIME64,
		arrow.TIMESTAMP,
		arrow.DURATION,
		arrow.DECIMAL32, arrow.DECIMAL64, arrow.DECIMAL128, arrow.DECIMAL256,
		arrow.INTERVAL_MONTHS, arrow.INTERVAL_DAY_TIME, arrow.INTERVAL_MONTH_DAY_NANO:
		return nil
	}
	return fmt.Errorf("database/sql conversion does not support Arrow %s type %s; use QueryArrowContext for exact Arrow data", unsupportedArrowFamily(dt), dt)
}

func unsupportedArrowFamily(dt arrow.DataType) string {
	switch dt.ID() {
	case arrow.LIST, arrow.LARGE_LIST, arrow.FIXED_SIZE_LIST, arrow.LIST_VIEW, arrow.LARGE_LIST_VIEW:
		return "list"
	case arrow.STRUCT:
		return "struct"
	case arrow.MAP:
		return "map"
	case arrow.SPARSE_UNION, arrow.DENSE_UNION:
		return "union"
	case arrow.DICTIONARY:
		return "dictionary"
	case arrow.EXTENSION:
		return "extension"
	case arrow.RUN_END_ENCODED:
		return "run-end encoded"
	default:
		return "unsupported"
	}
}

func scanType(dt arrow.DataType, nullable bool) reflect.Type {
	if nullable {
		switch dt.ID() {
		case arrow.BOOL:
			return reflect.TypeOf(sql.NullBool{})
		case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64:
			return reflect.TypeOf(sql.NullInt64{})
		case arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
			return reflect.TypeOf(sql.NullInt64{})
		case arrow.FLOAT16, arrow.FLOAT32, arrow.FLOAT64:
			return reflect.TypeOf(sql.NullFloat64{})
		case arrow.DATE32, arrow.DATE64, arrow.TIME32, arrow.TIME64, arrow.TIMESTAMP:
			return reflect.TypeOf(sql.NullTime{})
		case arrow.DURATION:
			return reflect.TypeOf(sql.NullInt64{})
		case arrow.STRING, arrow.LARGE_STRING, arrow.STRING_VIEW:
			return reflect.TypeOf(sql.NullString{})
		case arrow.DECIMAL32, arrow.DECIMAL64, arrow.DECIMAL128, arrow.DECIMAL256:
			return reflect.TypeOf(sql.NullString{})
		case arrow.INTERVAL_MONTHS, arrow.INTERVAL_DAY_TIME, arrow.INTERVAL_MONTH_DAY_NANO:
			return reflect.TypeOf(sql.NullString{})
		}
	}

	switch dt.ID() {
	case arrow.NULL:
		return reflect.TypeOf(new(any)).Elem()
	case arrow.BOOL:
		return reflect.TypeOf(false)
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64:
		return reflect.TypeOf(int64(0))
	case arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
		return reflect.TypeOf(int64(0))
	case arrow.FLOAT16, arrow.FLOAT32, arrow.FLOAT64:
		return reflect.TypeOf(float64(0))
	case arrow.DATE32, arrow.DATE64, arrow.TIME32, arrow.TIME64:
		return reflect.TypeOf(time.Time{})
	case arrow.DURATION:
		return reflect.TypeOf(int64(0))
	case arrow.STRING, arrow.LARGE_STRING, arrow.STRING_VIEW:
		return reflect.TypeOf("")
	case arrow.BINARY, arrow.LARGE_BINARY, arrow.FIXED_SIZE_BINARY, arrow.BINARY_VIEW:
		return reflect.TypeOf([]byte{})
	case arrow.TIMESTAMP:
		return reflect.TypeOf(time.Time{})
	case arrow.DECIMAL32, arrow.DECIMAL64, arrow.DECIMAL128, arrow.DECIMAL256:
		return reflect.TypeOf("")
	case arrow.INTERVAL_MONTHS, arrow.INTERVAL_DAY_TIME, arrow.INTERVAL_MONTH_DAY_NANO:
		return reflect.TypeOf("")
	default:
		return reflect.TypeOf(new(any)).Elem()
	}
}

func databaseTypeName(dt arrow.DataType) string {
	switch dt := dt.(type) {
	case *arrow.FixedSizeBinaryType:
		return fmt.Sprintf("FIXED_SIZE_BINARY(%d)", dt.ByteWidth)
	case *arrow.Date32Type:
		return "DATE32[day]"
	case *arrow.Date64Type:
		return "DATE64[ms]"
	case *arrow.Time32Type:
		return fmt.Sprintf("TIME32[%s]", dt.Unit)
	case *arrow.Time64Type:
		return fmt.Sprintf("TIME64[%s]", dt.Unit)
	case *arrow.TimestampType:
		if dt.TimeZone == "" {
			return fmt.Sprintf("TIMESTAMP[%s]", dt.Unit)
		}
		return fmt.Sprintf("TIMESTAMP[%s, tz=%s]", dt.Unit, dt.TimeZone)
	case *arrow.DurationType:
		return fmt.Sprintf("DURATION[%s]", dt.Unit)
	case *arrow.Decimal32Type:
		return fmt.Sprintf("DECIMAL32(%d,%d)", dt.Precision, dt.Scale)
	case *arrow.Decimal64Type:
		return fmt.Sprintf("DECIMAL64(%d,%d)", dt.Precision, dt.Scale)
	case *arrow.Decimal128Type:
		return fmt.Sprintf("DECIMAL(%d,%d)", dt.Precision, dt.Scale)
	case *arrow.Decimal256Type:
		return fmt.Sprintf("DECIMAL256(%d,%d)", dt.Precision, dt.Scale)
	case *arrow.MonthIntervalType:
		return "INTERVAL_MONTHS"
	case *arrow.DayTimeIntervalType:
		return "INTERVAL_DAY_TIME"
	case *arrow.MonthDayNanoIntervalType:
		return "INTERVAL_MONTH_DAY_NANO"
	default:
		return strings.ToUpper(dt.Name())
	}
}

func columnTypeLength(dt arrow.DataType) (int64, bool) {
	switch dt := dt.(type) {
	case *arrow.FixedSizeBinaryType:
		return int64(dt.ByteWidth), true
	default:
		return 0, false
	}
}

func columnTypePrecisionScale(dt arrow.DataType) (int64, int64, bool) {
	decimalType, ok := dt.(arrow.DecimalType)
	if !ok {
		return 0, 0, false
	}
	return int64(decimalType.GetPrecision()), int64(decimalType.GetScale()), true
}

var _ driver.Rows = (*rows)(nil)
var _ driver.RowsColumnTypeScanType = (*rows)(nil)
var _ driver.RowsColumnTypeDatabaseTypeName = (*rows)(nil)
var _ driver.RowsColumnTypeNullable = (*rows)(nil)
var _ driver.RowsColumnTypeLength = (*rows)(nil)
var _ driver.RowsColumnTypePrecisionScale = (*rows)(nil)
var _ driver.RowsNextResultSet = (*rows)(nil)
