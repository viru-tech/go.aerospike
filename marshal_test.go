package aerospike

import (
	"testing"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/stretchr/testify/require"
)

func TestMarshal(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		in      any
		want    aerospike.BinMap
		wantErr error
	}{
		{
			name: "ints",
			in: &struct {
				Int    int    `as:"int"`
				Int32  int32  `as:"int32"`
				UInt32 uint32 `as:"uint32"`
				Int64  int64  `as:"int64"`
				UInt64 uint64 `as:"uint64"`
			}{
				Int:    1,
				Int32:  2,
				UInt32: 3,
				Int64:  4,
				UInt64: 5,
			},
			want: map[string]any{
				"int":    int64(1),
				"int32":  int64(2),
				"uint32": int64(3),
				"int64":  int64(4),
				"uint64": int64(5),
			},
		},
		{
			name: "floats",
			in: &struct {
				Float32 float32 `as:"float32"`
				Float64 float64 `as:"float64"`
			}{
				Float32: 67,
				Float64: 8.9,
			},
			want: map[string]any{
				"float32": float64(67),
				"float64": float64(8.9),
			},
		},
		{
			name: "bool",
			in: &struct {
				Bool bool `as:"bool"`
			}{
				Bool: true,
			},
			want: map[string]any{
				"bool": true,
			},
		},
		{
			name: "string and string alias",
			in: &struct {
				Text  string    `as:"text"`
				Alias aliasType `as:"alias"`
			}{
				Text:  "string",
				Alias: "alias",
			},
			want: map[string]any{
				"text":  "string",
				"alias": "alias",
			},
		},
		{
			name: "maps",
			in: &struct {
				MapStrStr map[string]string `as:"map_str_str"`
				MapInt    map[int]int       `as:"map_int"`
			}{
				MapStrStr: map[string]string{
					"key": "value",
				},
				MapInt: map[int]int{
					1: 2,
				},
			},
			want: map[string]any{
				"map_str_str": map[any]any{"key": "value"},
				"map_int": map[any]any{
					int64(1): int64(2),
				},
			},
		},
		{
			name: "slices",
			in: &struct {
				Slice []int `as:"slice"`
			}{
				Slice: []int{1, 2, 3},
			},
			want: map[string]any{
				"slice": []any{
					int64(1),
					int64(2),
					int64(3),
				},
			},
		},
		{
			name: "time and duration",
			in: &struct {
				Time     time.Time     `as:"time"`
				Duration time.Duration `as:"duration"`
			}{
				Time:     time.Date(2025, 10, 17, 12, 51, 0, 0, time.UTC),
				Duration: time.Hour,
			},
			want: map[string]any{
				"time":     time.Date(2025, 10, 17, 12, 51, 0, 0, time.UTC).Unix(),
				"duration": int64(3600000000000),
			},
		},
		{
			name: "pointer",
			want: aerospike.BinMap{
				"text": "string",
			},
			in: &struct {
				Text *string `as:"text"`
			}{
				Text: func() *string {
					variable := "string"
					return &variable
				}(),
			},
		},
		{
			name: "nested scalars",
			in: &struct {
				Nested innerStruct `as:"nested"`
			}{
				Nested: innerStruct{
					Int:      10,
					Int32:    11,
					UInt32:   12,
					Int64:    13,
					UInt64:   14,
					Float32:  15,
					Float64:  16,
					Text:     "17",
					Time:     time.Date(2025, 10, 24, 12, 51, 0, 0, time.UTC),
					Duration: time.Minute,
					Alias:    "alias2",
				},
			},
			want: map[string]any{
				"nested": map[string]any{
					"int":      int64(10),
					"int32":    int64(11),
					"uint32":   int64(12),
					"int64":    int64(13),
					"uint64":   int64(14),
					"float32":  float64(15),
					"float64":  float64(16),
					"text":     "17",
					"time":     time.Date(2025, 10, 24, 12, 51, 0, 0, time.UTC).Unix(),
					"duration": int64(60000000000),
					"alias":    "alias2",
				},
			},
		},
		{
			name: "nested maps",
			in: &testStruct{
				Nested: innerStruct{
					MapStrStr: map[string]string{
						"key2": "value2",
					},
					MapInt: map[int]int{
						3: 4,
					},
					Slice: []int{4, 5, 6},
				},
			},
			want: map[string]any{
				"nested": map[string]any{
					"map_str_str": map[any]any{"key2": "value2"},
					"map_int":     map[any]any{int64(3): int64(4)},
					"slice":       []any{int64(4), int64(5), int64(6)},
				},
			},
		},
		{
			name: "nested slices",
			in: &testStruct{
				Nested: innerStruct{
					MapInt: map[int]int{
						3: 4,
					},
					Slice: []int{4, 5, 6},
				},
			},
			want: map[string]any{
				"nested": map[string]any{
					"map_int": map[any]any{int64(3): int64(4)},
					"slice":   []any{int64(4), int64(5), int64(6)},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			gotMap, err := Marshal(tt.in)
			require.Equal(t, tt.wantErr, err)
			require.Equal(t, tt.want, gotMap)
		})
	}
}

func BenchmarkMarshal(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		_, _ = Marshal(&allFieldsStruct)
	}
}
