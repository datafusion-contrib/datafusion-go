package datafusion

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/datafusion-contrib/datafusion-go/internal/native"
)

const (
	nanosPerDay   = int64(24 * time.Hour)
	secondsPerDay = int64((24 * time.Hour) / time.Second)
)

// ParameterType names a DataFusion scalar type for typed null parameters.
type ParameterType int

const (
	// ParameterBool identifies a boolean parameter.
	ParameterBool ParameterType = iota + 1
	// ParameterInt64 identifies a signed 64-bit integer parameter.
	ParameterInt64
	// ParameterUInt64 identifies an unsigned 64-bit integer parameter.
	ParameterUInt64
	// ParameterFloat64 identifies a 64-bit floating point parameter.
	ParameterFloat64
	// ParameterString identifies a UTF-8 string parameter.
	ParameterString
	// ParameterBinary identifies a binary parameter.
	ParameterBinary
	// ParameterDate identifies an Arrow Date32 parameter.
	ParameterDate
	// ParameterTime identifies an Arrow Time64 nanosecond parameter.
	ParameterTime
	// ParameterTimestamp identifies an Arrow timestamp parameter.
	ParameterTimestamp
	// ParameterDuration identifies an Arrow duration nanosecond parameter.
	ParameterDuration
	// ParameterDecimal identifies an Arrow Decimal128 parameter.
	ParameterDecimal
)

// UInt64 binds a parameter as an unsigned 64-bit integer.
type UInt64 uint64

// Uint64 returns the parameter value as a uint64.
func (value UInt64) Uint64() uint64 {
	return uint64(value)
}

// Date binds a parameter as an Arrow Date32 value.
type Date struct {
	days int32
}

// DateFromTime returns a Date using t's calendar date in t's location.
func DateFromTime(t time.Time) Date {
	year, month, day := t.Date()
	midnight := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
	return Date{days: int32(midnight.Unix() / secondsPerDay)}
}

// Days returns the Arrow Date32 day count since the Unix epoch.
func (value Date) Days() int32 {
	return value.days
}

// Time returns the date as a UTC time at midnight.
func (value Date) Time() time.Time {
	return time.Unix(int64(value.days)*secondsPerDay, 0).UTC()
}

// Time binds a parameter as an Arrow Time64 nanosecond value.
type Time struct {
	nanoseconds int64
}

// TimeFromTime returns a Time using only t's clock fields in t's location.
func TimeFromTime(t time.Time) Time {
	hour, minute, second := t.Clock()
	nanos := int64(hour)*int64(time.Hour) +
		int64(minute)*int64(time.Minute) +
		int64(second)*int64(time.Second) +
		int64(t.Nanosecond())
	return Time{nanoseconds: nanos}
}

// TimeNanos returns a Time from nanoseconds since midnight.
func TimeNanos(nanoseconds int64) Time {
	return Time{nanoseconds: nanoseconds}
}

// NewTimeNanos validates and returns a Time from nanoseconds since midnight.
func NewTimeNanos(nanoseconds int64) (Time, error) {
	if nanoseconds < 0 || nanoseconds >= nanosPerDay {
		return Time{}, fmt.Errorf("datafusion time parameter nanoseconds must be in [0,%d), got %d", nanosPerDay, nanoseconds)
	}
	return Time{nanoseconds: nanoseconds}, nil
}

// Nanoseconds returns the nanoseconds since midnight.
func (value Time) Nanoseconds() int64 {
	return value.nanoseconds
}

// Time returns the clock value as a UTC time on the Unix epoch date.
func (value Time) Time() time.Time {
	return time.Unix(0, value.nanoseconds).UTC()
}

// Timestamp binds a parameter as an Arrow timestamp with nanosecond precision.
type Timestamp struct {
	t        time.Time
	timeZone string
}

// TimestampFromTime returns a UTC timestamp parameter for t.
func TimestampFromTime(t time.Time) Timestamp {
	return Timestamp{t: t, timeZone: "UTC"}
}

// TimestampWithTimeZone returns a timestamp parameter with an explicit Arrow time zone string.
func TimestampWithTimeZone(t time.Time, timeZone string) Timestamp {
	return Timestamp{t: t, timeZone: timeZone}
}

// Time returns the timestamp value.
func (value Timestamp) Time() time.Time {
	return value.t
}

// TimeZone returns the Arrow timestamp timezone string.
func (value Timestamp) TimeZone() string {
	if value.timeZone == "" {
		return "UTC"
	}
	return value.timeZone
}

// Duration binds a parameter as an Arrow duration with nanosecond precision.
type Duration struct {
	nanoseconds int64
}

// DurationFromTime returns a Duration from a Go time.Duration.
func DurationFromTime(d time.Duration) Duration {
	return Duration{nanoseconds: int64(d)}
}

// DurationNanos returns a Duration from nanoseconds.
func DurationNanos(nanoseconds int64) Duration {
	return Duration{nanoseconds: nanoseconds}
}

// Nanoseconds returns the duration as nanoseconds.
func (value Duration) Nanoseconds() int64 {
	return value.nanoseconds
}

// Duration returns the value as a Go time.Duration.
func (value Duration) Duration() time.Duration {
	return time.Duration(value.nanoseconds)
}

// Decimal binds a parameter as an Arrow Decimal128 value.
type Decimal struct {
	value     string
	precision uint8
	scale     int8
}

// DecimalString returns a Decimal parameter from a base-10 string, precision, and scale.
func DecimalString(value string, precision uint8, scale int8) Decimal {
	return Decimal{value: value, precision: precision, scale: scale}
}

// NewDecimalString validates and returns a Decimal parameter from a base-10 string, precision, and scale.
func NewDecimalString(value string, precision uint8, scale int8) (Decimal, error) {
	if err := validateDecimalType(precision, scale); err != nil {
		return Decimal{}, err
	}
	if value == "" {
		return Decimal{}, fmt.Errorf("decimal value is empty")
	}
	return Decimal{value: value, precision: precision, scale: scale}, nil
}

// String returns the decimal value's base-10 representation.
func (value Decimal) String() string {
	return value.value
}

// Precision returns the Arrow decimal precision.
func (value Decimal) Precision() uint8 {
	return value.precision
}

// Scale returns the Arrow decimal scale.
func (value Decimal) Scale() int8 {
	return value.scale
}

// Null binds a typed null parameter.
type Null struct {
	typ       ParameterType
	precision uint8
	scale     int8
	timeZone  string
}

// NullOf returns a typed null for non-decimal parameter types.
func NullOf(typ ParameterType) Null {
	return Null{typ: typ, timeZone: "UTC"}
}

// NullDecimal returns a typed decimal null.
func NullDecimal(precision uint8, scale int8) Null {
	return Null{typ: ParameterDecimal, precision: precision, scale: scale}
}

// NullTimestamp returns a typed timestamp null with an explicit Arrow time zone string.
func NullTimestamp(timeZone string) Null {
	return Null{typ: ParameterTimestamp, timeZone: timeZone}
}

// Type returns the typed-null parameter type.
func (value Null) Type() ParameterType {
	return value.typ
}

// Precision returns the decimal precision for decimal typed nulls.
func (value Null) Precision() uint8 {
	return value.precision
}

// Scale returns the decimal scale for decimal typed nulls.
func (value Null) Scale() int8 {
	return value.scale
}

// TimeZone returns the Arrow timestamp timezone string for timestamp typed nulls.
func (value Null) TimeZone() string {
	if value.typ == ParameterTimestamp && value.timeZone == "" {
		return "UTC"
	}
	return value.timeZone
}

func checkNamedValue(nv *driver.NamedValue) error {
	value, err := normalizeParameterValue(nv.Value)
	if err != nil {
		return err
	}
	nv.Value = value
	return nil
}

func normalizeParameterValue(value any) (any, error) {
	switch value := value.(type) {
	case native.UInt64Parameter,
		native.DateParameter,
		native.TimeParameter,
		native.TimestampParameter,
		native.DurationParameter,
		native.DecimalParameter,
		native.NullParameter:
		return value, nil
	case UInt64:
		return native.UInt64Parameter{Value: uint64(value)}, nil
	case uint:
		return native.UInt64Parameter{Value: uint64(value)}, nil
	case uint8:
		return native.UInt64Parameter{Value: uint64(value)}, nil
	case uint16:
		return native.UInt64Parameter{Value: uint64(value)}, nil
	case uint32:
		return native.UInt64Parameter{Value: uint64(value)}, nil
	case uint64:
		return native.UInt64Parameter{Value: value}, nil
	case Date:
		return native.DateParameter{Days: value.days}, nil
	case Time:
		if value.nanoseconds < 0 || value.nanoseconds >= nanosPerDay {
			return nil, fmt.Errorf("datafusion time parameter nanoseconds must be in [0,%d), got %d", nanosPerDay, value.nanoseconds)
		}
		return native.TimeParameter{Nanoseconds: value.nanoseconds}, nil
	case Timestamp:
		param, err := timestampParameter(value.t, value.TimeZone())
		if err != nil {
			return nil, err
		}
		return param, nil
	case Duration:
		return native.DurationParameter{Nanoseconds: value.nanoseconds}, nil
	case time.Duration:
		return native.DurationParameter{Nanoseconds: int64(value)}, nil
	case Decimal:
		if err := validateDecimalType(value.precision, value.scale); err != nil {
			return nil, err
		}
		if strings.TrimSpace(value.value) == "" {
			return nil, fmt.Errorf("decimal value is empty")
		}
		return native.DecimalParameter{Value: value.value, Precision: value.precision, Scale: value.scale}, nil
	case Null:
		paramType, err := nativeParameterType(value.typ)
		if err != nil {
			return nil, err
		}
		if value.typ == ParameterDecimal {
			if value.precision == 0 {
				return nil, fmt.Errorf("datafusion NullOf(ParameterDecimal) has no decimal precision or scale; use NullDecimal(precision, scale)")
			}
			if err := validateDecimalType(value.precision, value.scale); err != nil {
				return nil, err
			}
		}
		timeZone := value.timeZone
		if value.typ == ParameterTimestamp && timeZone == "" {
			timeZone = "UTC"
		}
		return native.NullParameter{Type: paramType, Precision: value.precision, Scale: value.scale, TimeZone: timeZone}, nil
	}

	converted, err := driver.DefaultParameterConverter.ConvertValue(value)
	if err != nil {
		return nil, err
	}

	switch converted.(type) {
	case nil, bool, int64, float64, string, []byte, time.Time:
		if t, ok := converted.(time.Time); ok {
			param, err := timestampParameter(t, timeZoneFromLocation(t.Location()))
			if err != nil {
				return nil, err
			}
			return param, nil
		}
		return converted, nil
	default:
		return nil, fmt.Errorf("unsupported parameter type %T", converted)
	}
}

func normalizeNamedValueSlice(args []driver.NamedValue) ([]driver.NamedValue, error) {
	if len(args) == 0 {
		return nil, nil
	}

	named := make([]driver.NamedValue, len(args))
	for i, arg := range args {
		value, err := normalizeParameterValue(arg.Value)
		if err != nil {
			return nil, err
		}
		ordinal := arg.Ordinal
		if ordinal == 0 {
			ordinal = i + 1
		}
		named[i] = driver.NamedValue{Name: arg.Name, Ordinal: ordinal, Value: value}
	}
	return named, nil
}

func timestampNanos(value time.Time) (int64, error) {
	timestamp, err := arrow.TimestampFromTime(value, arrow.Nanosecond)
	if err != nil {
		return 0, err
	}
	return int64(timestamp), nil
}

func timestampParameter(value time.Time, timeZone string) (native.TimestampParameter, error) {
	nanos, err := timestampNanos(value)
	if err != nil {
		return native.TimestampParameter{}, err
	}
	if timeZone == "" {
		timeZone = "UTC"
	}
	return native.TimestampParameter{Nanoseconds: nanos, TimeZone: timeZone}, nil
}

func timeZoneFromLocation(location *time.Location) string {
	if location == nil {
		return "UTC"
	}
	name := location.String()
	if name == "" || name == "Local" {
		return "UTC"
	}
	if name == "UTC" {
		return name
	}
	if _, err := time.LoadLocation(name); err != nil {
		return "UTC"
	}
	return name
}

func validateDecimalType(precision uint8, scale int8) error {
	if precision == 0 || precision > 38 {
		return fmt.Errorf("decimal precision must be in [1,38], got %d", precision)
	}
	if scale < 0 || scale > int8(precision) {
		return fmt.Errorf("decimal scale must be in [0,%d], got %d", precision, scale)
	}
	return nil
}

func nativeParameterType(typ ParameterType) (native.ParameterType, error) {
	switch typ {
	case ParameterBool:
		return native.ParameterBool, nil
	case ParameterInt64:
		return native.ParameterInt64, nil
	case ParameterUInt64:
		return native.ParameterUInt64, nil
	case ParameterFloat64:
		return native.ParameterFloat64, nil
	case ParameterString:
		return native.ParameterString, nil
	case ParameterBinary:
		return native.ParameterBinary, nil
	case ParameterDate:
		return native.ParameterDate, nil
	case ParameterTime:
		return native.ParameterTime, nil
	case ParameterTimestamp:
		return native.ParameterTimestamp, nil
	case ParameterDuration:
		return native.ParameterDuration, nil
	case ParameterDecimal:
		return native.ParameterDecimal, nil
	default:
		return 0, fmt.Errorf("unsupported parameter type %d", typ)
	}
}
