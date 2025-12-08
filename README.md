# go.aerospike

This library handles marshaling and unmarshaling from aerospike BinMaps using "as" tags.
Fields without these tags will be ignored.

Important notes and limitations:
- This library only supports structs as targets for marshalling and unmarshalling
- Aerospike always stores ints as int64, floats as float64, unsigned integers are not supported
- Time is stored as Unix timestamp, and will be unmarshalled in UTC timezone

You can use *omitempty* to skip fields with zero value:
```go
type MyStruct {
	NotOmitted int `as:"no_omit"`        // will result in a bin containing 0
	Omitted    int `as:"omit,omitempty"` // bin will be skipped
}
```
