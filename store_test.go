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

func TestStore(t *testing.T) {
	ctx := context.Background()
	conn := redis.NewClient(
		&redis.Options{
			Addr: ":6379",
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

	// add "item1"
	err := store.Add(
		ctx,
		toy.NewItem("kind1", "item1").
			AddInt64Indexed("field1", 123).
			AddString("field2", "foo bar"),
	)
	require.NoError(t, err)

	// get "item1" of "kind1"
	t1Item1, ok, err := store.Get(ctx, "kind1", "item1")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(123), t1Item1.GetInt64("field1"))
	require.Equal(t, "foo bar", t1Item1.GetString("field2"))

	// remove "item1" of "kind1"
	deleted, err := store.Remove(ctx, "kind1", "item1")
	require.NoError(t, err)
	require.True(t, deleted)
	deleted, err = store.Remove(ctx, "kind1", "item1")
	require.NoError(t, err)
	require.False(t, deleted) // already deleted
	deleted, err = store.Remove(ctx, "kind1", "item2")
	require.NoError(t, err)
	require.False(t, deleted) // does not exists
	deleted, err = store.Remove(ctx, "kind2", "item1")
	require.NoError(t, err)
	require.False(t, deleted) // does not exists

	// add more items of "kind1"
	var t1Items []*toy.Item
	for i := 0; i < 10; i++ {
		id := randomID()
		err := store.Add(
			ctx,
			toy.NewItem("kind1", id).
				AddInt64Indexed("field1", mr.Int63n(100)).
				AddString("field2", randomID()),
		)
		require.NoError(t, err)
		item, ok, err := store.Get(ctx, "kind1", id)
		require.NoError(t, err)
		require.True(t, ok)
		require.NotNil(t, item)
		t1Items = append(t1Items, item)
	}
	sort.Slice(
		t1Items,
		func(i, j int) bool {
			f1i := t1Items[i].GetInt64("field1")
			f1j := t1Items[j].GetInt64("field1")
			if f1i == f1j {
				return strings.Compare(t1Items[i].ID(), t1Items[j].ID()) < 0
			}
			return f1i < f1j
		},
	)
	t1Rev := append([]*toy.Item{}, t1Items...)
	sort.Slice(
		t1Rev,
		func(i, j int) bool {
			f1i := t1Rev[i].GetInt64("field1")
			f1j := t1Rev[j].GetInt64("field1")
			if f1i == f1j {
				return strings.Compare(t1Rev[i].ID(), t1Rev[j].ID()) > 0
			}
			return f1i > f1j
		},
	)
	// add another item but with "field1" is not indexed: should be excluded
	// from the list results.
	err = store.Add(
		ctx,
		toy.NewItem("kind1", randomID()).
			AddInt64("field1", mr.Int63n(100)).
			AddString("field2", randomID()),
	)
	require.NoError(t, err)

	// list items of "kind1"
	items, hasMore, err := store.List(ctx, "kind1", "field1", false, 0, 3)
	require.NoError(t, err)
	require.True(t, hasMore)
	require.Len(t, items, 3)
	require.Equal(t, t1Items[0], items[0])
	require.Equal(t, t1Items[1], items[1])
	require.Equal(t, t1Items[2], items[2])
	items, hasMore, err = store.List(ctx, "kind1", "field1", false, 0, 10)
	require.NoError(t, err)
	require.False(t, hasMore)
	require.Len(t, items, 10)
	for idx, item := range items {
		require.Equal(t, t1Items[idx], item)
	}
	items, hasMore, err = store.List(ctx, "kind1", "field1", true, 0, 10)
	require.NoError(t, err)
	require.False(t, hasMore)
	require.Len(t, items, 10)
	for idx, item := range items {
		require.Equal(t, t1Rev[idx], item)
	}
	items, hasMore, err = store.List(ctx, "kind1", "field1", false, 3, 4)
	require.NoError(t, err)
	require.True(t, hasMore)
	require.Len(t, items, 4)
	require.Equal(t, t1Items[3], items[0])
	require.Equal(t, t1Items[4], items[1])
	require.Equal(t, t1Items[5], items[2])
	require.Equal(t, t1Items[6], items[3])
	items, hasMore, err = store.List(ctx, "kind1", "field1", true, 3, 4)
	require.NoError(t, err)
	require.True(t, hasMore)
	require.Len(t, items, 4)
	require.Equal(t, t1Rev[3], items[0])
	require.Equal(t, t1Rev[4], items[1])
	require.Equal(t, t1Rev[5], items[2])
	require.Equal(t, t1Rev[6], items[3])

	// add item "item1" of "kind2"
	err = store.Add(
		ctx,
		toy.NewItem("kind2", "item1").
			AddString("field1", "1Q84").
			AddInt64Indexed("field2", 1984).
			AddInt64Indexed("field3", 92),
	)
	require.NoError(t, err)

	// get item "item1" of "kind2"
	t2Item1, ok, err := store.Get(ctx, "kind2", "item1")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "1Q84", t2Item1.GetString("field1"))
	require.Equal(t, int64(1984), t2Item1.GetInt64("field2"))
	require.Equal(t, int64(92), t2Item1.GetInt64("field3"))

	// get item "item1" of "kind1": deleted
	t1Item1, ok, err = store.Get(ctx, "kind1", "item1")
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, t1Item1)

	// getting non-existing item
	_, ok, err = store.Get(ctx, "kind1", randomID())
	require.NoError(t, err)
	require.False(t, ok)
	_, ok, err = store.Get(ctx, "kind2", t1Items[1].ID())
	require.NoError(t, err)
	require.False(t, ok)

	// empty results on non-indexed field
	empty, hasMore, err := store.List(ctx, "kind1", "field2", true, 0, 3)
	require.NoError(t, err)
	require.False(t, hasMore)
	require.Empty(t, empty)
}

func TestStoreList(t *testing.T) {
	ctx := context.Background()
	conn := redis.NewClient(
		&redis.Options{
			Addr: ":6379",
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
	kind1 := "kind1"
	fields := []string{
		"field1",
		"field2",
		"field3",
		"field4",
	}

	var items []*toy.Item
	for i := 0; i < 200; i++ {
		item := toy.NewItem(kind1, randomID())
		numFields := mr.Intn(len(fields))
		for _, fieldIdx := range mr.Perm(len(fields))[:numFields] {
			switch mr.Intn(5) {
			case 0:
				item = item.AddInt64(
					fields[fieldIdx],
					mr.Int63n(10000),
				)
			case 1:
				item = item.AddInt64Indexed(
					fields[fieldIdx],
					mr.Int63n(10000),
				)
			case 2:
				item = item.AddFloat64(
					fields[fieldIdx],
					mr.Float64()*10000.0,
				)
			case 3:
				item = item.AddFloat64Indexed(
					fields[fieldIdx],
					mr.Float64()*10000.0,
				)
			case 4:
				item = item.AddString(
					fields[fieldIdx],
					randomID()+randomID(),
				)
			}
		}
		err := store.Add(ctx, item)
		require.NoError(t, err)
		items = append(items, item)
	}

	list := func(
		fieldName string,
		descending bool,
	) {
		var expected []*toy.Item
		for _, item := range items {
			field := item.GetField(fieldName)
			if field != nil &&
				field.Indexed {
				expected = append(expected, item)
			}
		}
		sort.Slice(expected, func(i, j int) bool {
			var si, sj float64
			score, ok := expected[i].LookupInt64(fieldName)
			if ok {
				si = float64(score)
			} else {
				si = expected[i].GetFloat64(fieldName)
			}
			score, ok = expected[j].LookupInt64(fieldName)
			if ok {
				sj = float64(score)
			} else {
				sj = expected[j].GetFloat64(fieldName)
			}
			if si == sj {
				sc := strings.Compare(expected[i].ID(), expected[j].ID())
				if descending {
					return sc > 0
				}
				return sc < 0
			}
			if descending {
				return si > sj
			}
			return si < sj
		})

		var actual []*toy.Item
		offset, limit := int64(0), int64(10)
		for {
			res, hasMore, err := store.List(
				ctx,
				kind1,
				fieldName,
				descending,
				offset,
				limit,
			)
			require.NoError(t, err)
			actual = append(actual, res...)
			if !hasMore {
				break
			}
			offset += limit
		}

		require.Equal(t, len(expected), len(actual))
		for i, e := range expected {
			a := actual[i]
			require.Equal(t, e, a)
		}
	}

	for _, field := range fields {
		list(field, false)
		list(field, true)
	}
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
