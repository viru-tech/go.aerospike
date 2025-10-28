package aerospike

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type aliasType string

type testStruct struct {
	Int       int               `as:"int"`
	Int32     int32             `as:"int32"`
	UInt32    uint32            `as:"uint32"`
	Int64     int64             `as:"int64"`
	UInt64    uint64            `as:"uint64"`
	Float32   float32           `as:"float32"`
	Float64   float64           `as:"float64"`
	Bool      bool              `as:"bool"`
	Text      string            `as:"text"`
	MapStrStr map[string]string `as:"map_str_str"`
	MapInt    map[int]int       `as:"map_int"`
	Time      time.Time         `as:"time"`
	Duration  time.Duration     `as:"duration"`
	Nested    innerStruct       `as:"nested"`
	Alias     aliasType         `as:"alias"`
	Slice     []int             `as:"slice"`
}

type innerStruct struct {
	Int       int               `as:"int"`
	Int32     int32             `as:"int32"`
	UInt32    uint32            `as:"uint32"`
	Int64     int64             `as:"int64"`
	UInt64    uint64            `as:"uint64"`
	Float32   float32           `as:"float32"`
	Float64   float64           `as:"float64"`
	Bool      bool              `as:"bool"`
	Text      string            `as:"text"`
	MapStrStr map[string]string `as:"map_str_str"`
	MapInt    map[int]int       `as:"map_int"`
	Time      time.Time         `as:"time"`
	Duration  time.Duration     `as:"duration"`
	Alias     aliasType         `as:"alias"`
	Slice     []int             `as:"slice"`
}

var allFieldsStruct = testStruct{
	Int:     1,
	Int32:   2,
	UInt32:  3,
	Int64:   4,
	UInt64:  5,
	Float32: 6.7,
	Float64: 8.9,
	Bool:    true,
	Text:    "string",
	MapStrStr: map[string]string{
		"key": "value",
	},
	MapInt: map[int]int{
		1: 2,
	},
	Time:     time.Date(2025, 10, 17, 12, 51, 0, 0, time.UTC),
	Duration: time.Hour,
	Nested: innerStruct{
		Int:     10,
		Int32:   11,
		UInt32:  12,
		Int64:   13,
		UInt64:  14,
		Float32: 15,
		Float64: 16,
		Bool:    true,
		Text:    "17",
		MapStrStr: map[string]string{
			"key2": "value2",
		},
		MapInt: map[int]int{
			3: 4,
		},
		Time:     time.Date(2025, 10, 24, 12, 51, 0, 0, time.UTC),
		Duration: time.Minute,
		Alias:    "alias2",
		Slice:    []int{4, 5, 6},
	},
	Alias: "alias",
	Slice: []int{1, 2, 3},
}

func TestAerospike(t *testing.T) {
	t.Parallel()
	client, cleanup, err := setupAerospike()
	require.NoError(t, err)
	defer cleanup()

	bins, err := Marshal(&allFieldsStruct)
	require.NoError(t, err)

	key, err := aerospike.NewKey("test", "test", uuid.NewString())
	require.NoError(t, err)

	err = client.Put(nil, key, bins)
	require.NoError(t, err)

	gotBins, err := client.Get(nil, key)
	require.NoError(t, err)

	got := testStruct{}
	err = Unmarshal(gotBins, &got)
	require.NoError(t, err)
	require.Equal(t, allFieldsStruct, got)
}

func setupAerospike() (*aerospike.Client, func(), error) {
	container, err := testcontainers.GenericContainer(context.Background(), testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "aerospike:ce-8.0.0.9",
			ExposedPorts: []string{"3000/tcp"},
			Files:        []testcontainers.ContainerFile{},
			WaitingFor: wait.ForAll(
				wait.ForListeningPort("3000/tcp"),
				wait.ForLog("migrations: complete"),
			),
		},
		Started: true,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to start aerospike container: %w", err)
	}

	cleanup := func() {
		_ = container.Terminate(context.Background())
	}

	host, err := container.Host(context.Background())
	if err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("failed to get aerospike IP: %w", err)
	}
	port, err := container.MappedPort(context.Background(), "3000")
	if err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("failed to get aerospike port: %w", err)
	}

	client, err := aerospike.NewClient(host, port.Int())
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create client: %w", err)
	}

	client.DefaultQueryPolicy.SendKey = true
	client.DefaultWritePolicy.SendKey = true

	return client, cleanup, nil
}
