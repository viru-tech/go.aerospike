package aerospike

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetBinKeys(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		in      any
		want    []string
		wantErr bool
	}{
		{
			name: "get struct bin keys",
			in: &struct {
				Text string `as:"text"`
			}{
				Text: "text",
			},
			want: []string{"text"},
		},
		{
			name: "get map bin keys",
			in: map[string]any{
				"key": "value",
			},
			want: []string{"key"},
		},
		{
			name: "non string map key",
			in: map[any]any{
				1: "value",
			},
			wantErr: true,
		},
		{
			name:    "incorrect type",
			in:      1,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := GetBinKeys(tt.in)
			require.Equal(t, tt.wantErr, err != nil)
			require.Equal(t, tt.want, got)
		})
	}
}
