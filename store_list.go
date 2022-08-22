package toy

import (
	"context"
	"fmt"
)

// TODO: this implementation won't work in clustered deployments because it's
//  accessing keys using programmatically-generated names inside the script.
// KEYS:
//  [1] index key
// ARGV:
//  [1] range func name
//  [2] offset
//  [3] limit
//  [4] item key prefix
var scriptList = `
local hasMore = false
local itemIDs = redis.call(ARGV[1], KEYS[1], ARGV[2], ARGV[2]+ARGV[3])
local res = {}
if #itemIDs > tonumber(ARGV[3]) then
	table.insert(res, true)
	table.remove(itemIDs, #itemIDs)
else
	table.insert(res, false)
end
for idx=1,#itemIDs do
	local item = redis.call('GET', ARGV[4]..itemIDs[idx])
	table.insert(res, item)
end
return res
`

func (s *Store) List(
	ctx context.Context,
	typeName string,
	sortBy string,
	descending bool,
	offset, limit int64,
) (items []Item, hasMore bool, err error) {
	if typeName == "" {
		err = fmt.Errorf("type name must not be empty")
		return
	}
	if sortBy == "" {
		err = fmt.Errorf("sort by must not be empty")
		return
	}
	if offset < 0 {
		err = fmt.Errorf("invalid offset: %d", offset)
		return
	}
	if limit < 0 {
		err = fmt.Errorf("invalid limit: %d", limit)
		return
	}

	// get type
	typ, err := s.getType(ctx, typeName)
	if err != nil {
		err = fmt.Errorf("get type: %w", err)
		return
	}
	var isIndexed bool
	for _, fieldName := range typ.Indexes {
		if fieldName == sortBy {
			isIndexed = true
			break
		}
	}
	if !isIndexed {
		err = fmt.Errorf("sort by field must be indexed: %s", sortBy)
		return
	}

	indexKey := s.baseKeyPrefix + keyPrefixIndex + typeName + ":" + sortBy
	rangeFuncName := "ZRANGE"
	if descending {
		rangeFuncName = "ZREVRANGE"
	}

	res, err := s.scriptList.Run(
		ctx,
		s.conn,
		[]string{
			indexKey,
		},
		[]interface{}{
			rangeFuncName,
			offset,
			limit,
			s.baseKeyPrefix + keyPrefixItem + typeName + ":",
		},
	).Result()
	if err != nil {
		err = fmt.Errorf("redis script run: %w", err)
		return
	}
	resList := res.([]interface{})
	hasMore = resList[0] != nil
	for _, entry := range resList[1:] {
		var item Item
		item, err = s.decodeItem(entry.(string))
		if err != nil {
			err = fmt.Errorf("decode item: %w", err)
			return
		}
		items = append(items, item)
	}
	return
}
