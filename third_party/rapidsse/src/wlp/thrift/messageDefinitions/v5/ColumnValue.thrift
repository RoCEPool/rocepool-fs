
namespace java com.rapidsdata.wirelineprotocol.messages.v5
namespace cpp  com.rapidsdata.wirelineprotocol.messages.v5

// Definition of a timestamp, used below.
struct Timestamp {
  1:  required i32 year
  2:  required i16 month
  3:  required i16 day
  4:  required i16 hour
  5:  required i16 minute
  6:  required i16 second
  7:  required i32 nanoseconds
  8:  optional i8 offsetHours;
  9:  optional i8 offsetMinutes;
  10: optional string tzName;   // IANA name.
}

struct Date {
  1: required i32 year
  2: required i16 month
  3: required i16 day
  4: optional i8 offsetHours;
  5: optional i8 offsetMinutes;
  6: optional string tzName;   // IANA name.
}

struct Time {
  1: required i16 hour
  2: required i16 minute
  3: required i16 second
  4: required i32 nanoseconds
  5: optional i8 offsetHours;
  6: optional i8 offsetMinutes;
  7: optional string tzName;   // IANA name.
}

// The class of interval.
enum IntervalType {
  YEAR,
  MONTH,
  YEAR_TO_MONTH,
  DAY,
  HOUR,
  MINUTE,
  SECOND,
  DAY_TO_HOUR,
  DAY_TO_MINUTE,
  DAY_TO_SECOND,
  HOUR_TO_MINUTE,
  HOUR_TO_SECOND,
  MINUTE_TO_SECOND
}

// Definition of an interval, used below.
struct Interval {
  1: required IntervalType type
  2: optional bool isNegative
  3: optional i64  year
  4: optional i64  month
  5: optional i64  day
  6: optional i64  hour
  7: optional i64  minute
  8: optional i64  second
  9: optional i64  nanoseconds
}

// Definition of a column value, used below.
struct ColumnValue {
  1:  optional bool      isNull
  2:  optional bool      booleanVal
  3:  optional i16       tinyIntVal
  4:  optional i16       smallIntVal
  5:  optional i32       integerVal
  6:  optional i64       bigintVal
  7:  optional double    doubleVal
  8:  optional string    bigDecimalVal
  9:  optional Timestamp timestampVal
  10: optional Time      timeVal
  11: optional Date      dateVal
  12: optional Interval  intervalVal
  13: optional string    stringVal
  14: optional binary    binaryVal
}
