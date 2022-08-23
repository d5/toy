package toy

type ItemValueType int

const (
	ItemValueTypeAny     ItemValueType = 1
	ItemValueTypeInt64   ItemValueType = 2
	ItemValueTypeFloat64 ItemValueType = 3
	ItemValueTypeString  ItemValueType = 4
)

func (t ItemValueType) String() string {
	switch t {
	case ItemValueTypeAny:
		return "any"
	case ItemValueTypeInt64:
		return "int64"
	case ItemValueTypeFloat64:
		return "float64"
	case ItemValueTypeString:
		return "string"
	}
	return ""
}
