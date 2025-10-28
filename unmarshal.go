package aerospike

import (
	"fmt"
	"reflect"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"golang.org/x/exp/constraints"
)

// Unmarshal parses aerospike record into a struct using "as" tags as bin names.
func Unmarshal(record *aerospike.Record, v any) error {
	if record == nil {
		return nil
	}

	rv := reflect.ValueOf(v)
	indirect := reflect.Indirect(rv)
	if rv.Kind() != reflect.Pointer || indirect.Kind() != reflect.Struct || rv.IsNil() {
		return errInputType
	}

	err := unmarshalStruct(indirect, map[string]any(record.Bins))
	if err != nil {
		return err
	}

	return nil
}

func unmarshalStruct(field reflect.Value, v any) error {
	mapVal := reflect.ValueOf(v)
	if mapVal.Kind() != reflect.Map {
		return errInputType
	}

	var (
		marshaled any
		err       error
	)
	for i := range field.NumField() {
		marshaled = nil
		tag := field.Type().Field(i).Tag.Get(structTag)
		if tag == "" {
			continue
		}
		rawVal := mapVal.MapIndex(reflect.ValueOf(tag))
		if !rawVal.IsValid() {
			continue
		}
		val := rawVal.Interface()
		fieldVal := field.Field(i)
		isPointer := fieldVal.Kind() == reflect.Ptr
		indirect := reflect.Indirect(fieldVal)
		if isPointer {
			indirect = reflect.Indirect(reflect.New(fieldVal.Type().Elem()))
		}
		switch {
		case indirect.Type().PkgPath() == "time" && indirect.Type().Name() == "Time":
			marshaled, err = parseTime(val)
		case indirect.Kind() == reflect.Map:
			marshaled, err = unmarshalMap(indirect, val)
		case indirect.Kind() == reflect.Slice:
			marshaled, err = unmarshalSlice(indirect, val)
		case indirect.Kind() == reflect.Struct:
			err = unmarshalStruct(indirect, val)
		default:
			marshaled, err = unmarshalScalarToKind(indirect.Kind(), val)
		}
		if err != nil {
			return fmt.Errorf("error while parsing field %s: %w", field.Type().Field(i).Name, err)
		}
		if marshaled == nil {
			continue
		}
		if isPointer {
			fieldVal.Set(reflect.New(fieldVal.Type().Elem()))

			fieldVal.Elem().Set(reflect.ValueOf(marshaled).Convert(fieldVal.Type().Elem()))
			continue
		}
		if !reflect.ValueOf(marshaled).CanConvert(fieldVal.Type()) {
			return fmt.Errorf("cannot convert %v to %v: %w", marshaled, fieldVal.Type(), errInputType)
		}
		fieldVal.Set(reflect.ValueOf(marshaled).Convert(fieldVal.Type()))
	}

	return nil
}

func unmarshalSlice(field reflect.Value, v any) (any, error) {
	if v == nil {
		return nil, nil //nolint:nilnil
	}
	val := reflect.ValueOf(v)

	slice := reflect.MakeSlice(field.Type(), val.Len(), val.Cap())
	for i := range val.Len() {
		if !val.Index(i).Elem().CanConvert(slice.Index(i).Type()) {
			return nil, fmt.Errorf("cannot convert %v to %v: %w",
				val.Index(i).Type(),
				slice.Index(i).Type(),
				errInputType,
			)
		}

		slice.Index(i).Set(val.Index(i).Elem().Convert(slice.Index(i).Type()))
	}

	return slice.Interface(), nil
}

func unmarshalMap(field reflect.Value, v any) (any, error) {
	if v == nil {
		return nil, nil //nolint:nilnil
	}

	mapVal := reflect.ValueOf(v)
	out := reflect.MakeMapWithSize(field.Type(), mapVal.Len())
	iter := mapVal.MapRange()
	for iter.Next() {
		key, err := unmarshalScalarToKind(field.Type().Key().Kind(), iter.Key().Elem().Interface())
		if err != nil {
			return nil, err
		}
		val, err := unmarshalScalarToKind(field.Type().Elem().Kind(), iter.Value().Elem().Interface())
		if err != nil {
			return nil, err
		}

		out.SetMapIndex(reflect.ValueOf(key), reflect.ValueOf(val))
	}
	field.Set(out)

	return out.Interface(), nil
}

func unmarshalScalarToKind(kind reflect.Kind, value any) (any, error) {
	switch kind {
	case reflect.String:
		return value, nil
	case reflect.Uint64:
		return toIntVal[uint64](value), nil
	case reflect.Int64:
		return toIntVal[int64](value), nil
	case reflect.Int32:
		return toIntVal[int32](value), nil
	case reflect.Uint32:
		return toIntVal[uint32](value), nil
	case reflect.Int16:
		return toIntVal[int16](value), nil
	case reflect.Uint16:
		return toIntVal[uint16](value), nil
	case reflect.Int8:
		return toIntVal[int8](value), nil
	case reflect.Uint8:
		return toIntVal[uint8](value), nil
	case reflect.Int:
		return toIntVal[int](value), nil
	case reflect.Uint:
		return toIntVal[uint](value), nil
	case reflect.Float32:
		return toFloatVal[float32](value), nil
	case reflect.Float64:
		return toFloatVal[float64](value), nil
	default:
		return nil, fmt.Errorf("unsupported scalar value type: %s: %w", kind.String(), errInputType)
	}
}

func toFloatVal[T constraints.Float](value any) T { //nolint:ireturn
	intVal, _ := value.(float64)
	return T(intVal)
}

func toIntVal[T constraints.Integer](value any) T { //nolint:ireturn
	intVal, _ := value.(int)
	return T(intVal)
}

func parseTime(v any) (time.Time, error) {
	if v == nil {
		return time.Time{}, nil
	}
	timestamp, ok := v.(int)
	if !ok {
		return time.Time{}, fmt.Errorf("time must be an int: %w", errInputType)
	}

	return time.Unix(int64(timestamp), 0).UTC(), nil
}
