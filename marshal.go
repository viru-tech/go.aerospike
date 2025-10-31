package aerospike

import (
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
)

const structTag = "as"

var errInputType = errors.New("wrong variable provided")

// Marshal converts struct into bin map using "as" tags as bin names.
func Marshal(v any) (aerospike.BinMap, error) {
	if v == nil {
		return aerospike.BinMap{}, nil
	}

	rv := reflect.ValueOf(v)
	indirect := reflect.Indirect(rv)
	if rv.Kind() != reflect.Pointer || indirect.Kind() != reflect.Struct || rv.IsNil() {
		return aerospike.BinMap{}, fmt.Errorf("the provided variable must be a non-nil pointer to a struct: %w", errInputType)
	}

	binMap, err := marshalStruct(indirect)
	if err != nil {
		return nil, err
	}

	return binMap, nil
}

func marshalStruct(v reflect.Value) (map[string]any, error) {
	if v.Kind() == reflect.Pointer {
		v = reflect.Indirect(v)
	}

	out := make(map[string]any)
	var err error
	for i := range v.Type().NumField() {
		tag := v.Type().Field(i).Tag.Get(structTag)
		if tag == "" {
			continue
		}

		field := v.Field(i)
		if field.IsZero() {
			continue
		}
		if field.Kind() == reflect.Ptr {
			field = reflect.Indirect(field)
		}

		out[tag], err = convertValue(field)
		if err != nil {
			return nil, fmt.Errorf("failed to convert field %v: %w", v.Type().NumField(), err)
		}
	}

	return out, nil
}

func convertValue(v reflect.Value) (any, error) {
	realValue := v.Interface()
	if timeVal, ok := realValue.(time.Time); ok {
		return timeVal.Unix(), nil
	}

	switch v.Kind() {
	case reflect.Bool:
		return v.Bool(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		unsigned := v.Uint()
		return int64(unsigned), nil //nolint:gosec
	case reflect.Float32, reflect.Float64:
		return v.Float(), nil
	case reflect.String:
		return v.String(), nil
	case reflect.Map:
		return marshalMap(v)
	case reflect.Slice:
		return marshalSlice(v)
	case reflect.Struct:
		return marshalStruct(v)
	default:
		return nil, fmt.Errorf(
			"type %s is not supported: %w",
			v.Kind().String(),
			errInputType,
		)
	}
}

func marshalSlice(v reflect.Value) ([]any, error) {
	out := make([]any, v.Len())
	var err error
	for i := range v.Len() {
		out[i], err = convertValue(v.Index(i))
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal slice: %w", err)
		}
	}

	return out, nil
}

func marshalMap(in reflect.Value) (any, error) {
	iter := in.MapRange()
	out := make(map[any]any, len(in.MapKeys()))
	for iter.Next() {
		iterKey, err := getMapKey(iter.Key())
		if err != nil {
			return nil, fmt.Errorf("failed to marshal map key %v: %w", iter.Key().Interface(), err)
		}
		iterVal, err := convertValue(iter.Value())
		if err != nil {
			return nil, fmt.Errorf("failed to marshal map value in key %s %v: %w", iter.Key().Interface(), iter.Value().Interface(), err)
		}

		out[iterKey] = iterVal
	}

	return out, nil
}

func getMapKey(val reflect.Value) (any, error) {
	switch val.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return val.Int(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return val.Uint(), nil
	case reflect.String:
		return val.String(), nil
	default:
		return nil, fmt.Errorf("type %s is not supported: %w", val.Kind().String(), errInputType)
	}
}
