package toy_test

import (
	"context"
	cr "crypto/rand"
	"encoding/hex"
	"io"
	mr "math/rand"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/d5/toy"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"
)

type IDItem struct {
	ID   string
	Item toy.Item
}

func TestStore(t *testing.T) {
	ctx := context.Background()
	conn := redis.NewClient(
		&redis.Options{
			Addr: "localhost:6379",
		},
	)
	baseKeyPrefix := randomID() + ":"
	defer func() {
		keys, err := conn.Keys(ctx, baseKeyPrefix+"*").Result()
		require.NoError(t, err)
		for _, key := range keys {
			_ = conn.Del(ctx, key)
		}
	}()
	store := toy.NewStore(conn, baseKeyPrefix)
	mr.Seed(time.Now().UnixNano())

	// define "type1"
	err := store.DefineType(ctx, "type1", []string{"field1"})
	require.NoError(t, err)

	// add item "item1" of "type1"
	err = store.Add(
		ctx,
		"type1",
		"item1",
		toy.Item{
			"field1": 123,
			"field2": "foo bar",
		},
	)
	require.NoError(t, err)

	// get item "item1" of "type1"
	t1Item1, ok, err := store.Get(ctx, "type1", "item1")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 123, t1Item1["field1"])
	require.Equal(t, "foo bar", t1Item1["field2"])

	// add more items of "type1"
	t1Items := []*IDItem{
		{ID: "item1", Item: t1Item1},
	}
	for i := 0; i < 9; i++ {
		id := randomID()
		err := store.Add(
			ctx,
			"type1",
			id,
			toy.Item{
				"field1": mr.Intn(100),
				"field2": randomID(),
			},
		)
		require.NoError(t, err)
		item, ok, err := store.Get(ctx, "type1", id)
		require.NoError(t, err)
		require.True(t, ok)
		require.NotNil(t, item)
		t1Items = append(t1Items, &IDItem{ID: id, Item: item})
	}
	sort.Slice(
		t1Items,
		func(i, j int) bool {
			f1i := t1Items[i].Item["field1"].(int)
			f1j := t1Items[j].Item["field1"].(int)
			if f1i == f1j {
				return strings.Compare(t1Items[i].ID, t1Items[j].ID) < 0
			}
			return f1i < f1j
		},
	)
	t1Rev := append([]*IDItem{}, t1Items...)
	sort.Slice(
		t1Rev,
		func(i, j int) bool {
			f1i := t1Rev[i].Item["field1"].(int)
			f1j := t1Rev[j].Item["field1"].(int)
			if f1i == f1j {
				return strings.Compare(t1Rev[i].ID, t1Rev[j].ID) > 0
			}
			return f1i > f1j
		},
	)
	// for idx, item := range t1Items {
	// 	fmt.Println(idx, item.ID, item.Item["field1"])
	// }

	// list items of "type1"
	items, hasMore, err := store.List(ctx, "type1", "field1", false, 0, 3)
	require.NoError(t, err)
	require.True(t, hasMore)
	require.Len(t, items, 3)
	require.Equal(t, t1Items[0].Item, items[0])
	require.Equal(t, t1Items[1].Item, items[1])
	require.Equal(t, t1Items[2].Item, items[2])
	items, hasMore, err = store.List(ctx, "type1", "field1", false, 0, 10)
	require.NoError(t, err)
	require.False(t, hasMore)
	require.Len(t, items, 10)
	for idx, item := range items {
		require.Equal(t, t1Items[idx].Item, item)
	}
	items, hasMore, err = store.List(ctx, "type1", "field1", true, 0, 10)
	require.NoError(t, err)
	require.False(t, hasMore)
	require.Len(t, items, 10)
	for idx, item := range items {
		require.Equal(t, t1Rev[idx].Item, item)
	}
	items, hasMore, err = store.List(ctx, "type1", "field1", false, 3, 4)
	require.NoError(t, err)
	require.True(t, hasMore)
	require.Len(t, items, 4)
	require.Equal(t, t1Items[3].Item, items[0])
	require.Equal(t, t1Items[4].Item, items[1])
	require.Equal(t, t1Items[5].Item, items[2])
	require.Equal(t, t1Items[6].Item, items[3])
	items, hasMore, err = store.List(ctx, "type1", "field1", true, 3, 4)
	require.NoError(t, err)
	require.True(t, hasMore)
	require.Len(t, items, 4)
	require.Equal(t, t1Rev[3].Item, items[0])
	require.Equal(t, t1Rev[4].Item, items[1])
	require.Equal(t, t1Rev[5].Item, items[2])
	require.Equal(t, t1Rev[6].Item, items[3])

	// define "type2"
	err = store.DefineType(ctx, "type2", []string{"field2", "field3"})
	require.NoError(t, err)

	// add item "item1" of "type2"
	err = store.Add(
		ctx,
		"type2",
		"item1",
		toy.Item{
			"field1": "1Q84",
			"field2": 1984,
			"field3": 92,
		},
	)
	require.NoError(t, err)

	// get item "item1" of "type2"
	t2Item1, ok, err := store.Get(ctx, "type2", "item1")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "1Q84", t2Item1["field1"])
	require.Equal(t, 1984, t2Item1["field2"])
	require.Equal(t, 92, t2Item1["field3"])

	// get item "item1" of "type1"
	t1Item1, ok, err = store.Get(ctx, "type1", "item1")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 123, t1Item1["field1"])
	require.Equal(t, "foo bar", t1Item1["field2"])

	// getting non-existing item
	_, ok, err = store.Get(ctx, "type1", randomID())
	require.NoError(t, err)
	require.False(t, ok)
	_, ok, err = store.Get(ctx, "type2", t1Items[1].ID)
	require.NoError(t, err)
	require.False(t, ok)

	// cannot add item of undefined type
	err = store.Add(
		ctx,
		"type3",
		"item1",
		toy.Item{
			"field1": 123,
		},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "type not defined")

	// cannot add item with missing indexed fields
	err = store.Add(
		ctx,
		"type1",
		randomID(),
		toy.Item{
			"field2": "hello",
		},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "must include indexed field")
	err = store.Add(
		ctx,
		"type2",
		randomID(),
		toy.Item{
			"field2": 123,
		},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "must include indexed field")

	// cannot add item with non-number indexed field value
	err = store.Add(
		ctx,
		"type1",
		randomID(),
		toy.Item{
			"field1": "hello",
		},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "value must be a number")

	// cannot list with sort by of non-indexed field
	_, _, err = store.List(ctx, "type1", "field2", true, 0, 3)
	require.Error(t, err)
	require.Contains(t, err.Error(), "sort by field must be indexed")
}

func randomID() string {
	src := make([]byte, 16)
	if _, err := io.ReadFull(cr.Reader, src); err != nil {
		panic(err)
	}
	src[6] = (src[6] & 0x0f) | 0x40 // Version 4
	src[8] = (src[8] & 0x3f) | 0x80 // Variant is 10
	dst := make([]byte, 32)
	_ = hex.Encode(dst, src)
	return string(dst)
}
