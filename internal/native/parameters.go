package native

type ParameterType int32

const (
	ParameterBool ParameterType = iota + 1
	ParameterInt64
	ParameterUInt64
	ParameterFloat64
	ParameterString
	ParameterBinary
	ParameterDate
	ParameterTime
	ParameterTimestamp
	ParameterDuration
	ParameterDecimal
)

type UInt64Parameter struct {
	Value uint64
}

type DateParameter struct {
	Days int32
}

type TimeParameter struct {
	Nanoseconds int64
}

type TimestampParameter struct {
	Nanoseconds int64
	TimeZone    string
}

type DurationParameter struct {
	Nanoseconds int64
}

type DecimalParameter struct {
	Value     string
	Precision uint8
	Scale     int8
}

type NullParameter struct {
	Type      ParameterType
	Precision uint8
	Scale     int8
	TimeZone  string
}
