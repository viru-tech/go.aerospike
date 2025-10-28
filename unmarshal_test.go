package aerospike

import (
	"testing"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/stretchr/testify/require"
)

func TestUnmarshal(t *testing.T) {
	t.Parallel()
	type args struct {
		record *aerospike.Record
		v      any
	}
	tests := []struct {
		name    string
		args    args
		want    any
		wantErr error
	}{
		{
			name: "ints",
			want: &struct {
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
			args: args{
				v: &struct {
					Int    int    `as:"int"`
					Int32  int32  `as:"int32"`
					UInt32 uint32 `as:"uint32"`
					Int64  int64  `as:"int64"`
					UInt64 uint64 `as:"uint64"`
				}{},
				record: &aerospike.Record{
					Bins: map[string]any{
						"int":    1,
						"int32":  2,
						"uint32": 3,
						"int64":  4,
						"uint64": 5,
					},
				},
			},
		},
		{
			name: "floats",
			want: &struct {
				Float32 float32 `as:"float32"`
				Float64 float64 `as:"float64"`
			}{
				Float32: 6,
				Float64: 8.9,
			},
			args: args{
				v: &struct {
					Float32 float32 `as:"float32"`
					Float64 float64 `as:"float64"`
				}{},
				record: &aerospike.Record{
					Bins: map[string]any{
						"float32": float64(6),
						"float64": float64(8.9),
					},
				},
			},
		},
		{
			name: "string and string alias",
			want: &struct {
				Text  string    `as:"text"`
				Alias aliasType `as:"alias"`
			}{
				Text:  "string",
				Alias: "alias",
			},
			args: args{
				v: &struct {
					Text  string    `as:"text"`
					Alias aliasType `as:"alias"`
				}{},
				record: &aerospike.Record{
					Bins: map[string]any{
						"text":  "string",
						"alias": "alias",
					},
				},
			},
		},
		{
			name: "pointer",
			want: &struct {
				Text *string `as:"text"`
			}{
				Text: func() *string {
					variable := "string"
					return &variable
				}(),
			},
			args: args{
				v: &struct {
					Text *string `as:"text"`
				}{},
				record: &aerospike.Record{
					Bins: map[string]any{
						"text": "string",
					},
				},
			},
		},
		{
			name: "maps",
			want: &struct {
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
			args: args{
				v: &struct {
					MapStrStr map[string]string `as:"map_str_str"`
					MapInt    map[int]int       `as:"map_int"`
				}{},
				record: &aerospike.Record{
					Bins: map[string]any{
						"map_str_str": map[any]any{"key": "value"},
						"map_int": map[any]any{
							1: 2,
						},
					},
				},
			},
		},
		{
			name: "slices",
			want: &struct {
				Slice []int `as:"slice"`
			}{
				Slice: []int{1, 2, 3},
			},
			args: args{
				v: &struct {
					Slice []int `as:"slice"`
				}{},
				record: &aerospike.Record{
					Bins: map[string]any{
						"slice": []any{
							1,
							2,
							3,
						},
					},
				},
			},
		},
		{
			name: "time and duration",
			want: &struct {
				Time     time.Time     `as:"time"`
				Duration time.Duration `as:"duration"`
			}{
				Time:     time.Date(2025, 10, 17, 12, 51, 0, 0, time.UTC),
				Duration: time.Hour,
			},
			args: args{
				v: &struct {
					Time     time.Time     `as:"time"`
					Duration time.Duration `as:"duration"`
				}{},
				record: &aerospike.Record{
					Bins: map[string]any{
						"time":     int(time.Date(2025, 10, 17, 12, 51, 0, 0, time.UTC).Unix()),
						"duration": 3600000000000,
					},
				},
			},
		},
		{
			name: "nested scalars",
			want: &struct {
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
			args: args{
				v: &struct {
					Nested innerStruct `as:"nested"`
				}{},
				record: &aerospike.Record{
					Bins: map[string]any{
						"nested": map[any]any{
							"int":      10,
							"int32":    11,
							"uint32":   12,
							"int64":    13,
							"uint64":   14,
							"float32":  float64(15),
							"float64":  float64(16),
							"text":     "17",
							"time":     int(time.Date(2025, 10, 24, 12, 51, 0, 0, time.UTC).Unix()),
							"duration": 60000000000,
							"alias":    "alias2",
						},
					},
				},
			},
		},
		{
			name: "nested maps",
			want: &struct {
				Nested innerStruct `as:"nested"`
			}{
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
			args: args{
				v: &struct {
					Nested innerStruct `as:"nested"`
				}{},
				record: &aerospike.Record{
					Bins: map[string]any{
						"nested": map[any]any{
							"map_str_str": map[any]any{"key2": "value2"},
							"map_int":     map[any]any{3: 4},
							"slice":       []any{4, 5, 6},
						},
					},
				},
			},
		},
		{
			name: "nested slices",
			want: &struct {
				Nested innerStruct `as:"nested"`
			}{
				Nested: innerStruct{
					MapInt: map[int]int{
						3: 4,
					},
					Slice: []int{4, 5, 6},
				},
			},
			args: args{
				v: &struct {
					Nested innerStruct `as:"nested"`
				}{},
				record: &aerospike.Record{
					Bins: map[string]any{
						"nested": map[any]any{
							"map_int": map[any]any{3: 4},
							"slice":   []any{4, 5, 6},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := Unmarshal(tt.args.record, tt.args.v)
			require.Equal(t, tt.wantErr, err)
			require.Equal(t, tt.want, tt.args.v)
		})
	}
}

func BenchmarkUnmarshal(b *testing.B) {
	bins, err := Marshal(&allFieldsStruct)
	require.NoError(b, err)
	record := &aerospike.Record{
		Bins: bins,
	}
	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		out := testStruct{}
		_ = Unmarshal(record, &out)
	}
}
