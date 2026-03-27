package aerospike

import (
	"reflect"
)

// GetBinKeys extracts bin names from structs.
func GetBinKeys(v any) ([]string, error) {
	rv := reflect.ValueOf(v)
	indirect := reflect.Indirect(rv)
	switch indirect.Kind() {
	case reflect.Struct:
		return getStructBinKeys(indirect), nil
	case reflect.Map:
		return getMapBinKeys(indirect)
	default:
		return nil, errInputType
	}
}

func getStructBinKeys(field reflect.Value) []string {
	fields := make([]string, 0, field.NumField())
	for i := range field.NumField() {
		tag := field.Type().Field(i).Tag.Get(structTag)
		if tag == "" {
			continue
		}

		fields = append(fields, tag)
	}

	return fields
}

func getMapBinKeys(field reflect.Value) ([]string, error) {
	keys := field.MapKeys()
	fields := make([]string, 0, len(keys))
	for i := range keys {
		if keys[i].Kind() != reflect.String {
			return nil, errInputType
		}

		fields = append(fields, keys[i].String())
	}

	return fields, nil
}
