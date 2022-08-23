package toy

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"strings"
)

type Item struct {
	kind   string
	id     string
	fields map[string]*ItemField
}

func NewItem(kind, id string) *Item {
	return &Item{
		kind:   kind,
		id:     id,
		fields: make(map[string]*ItemField),
	}
}

func DeserializeItem(s string) (*Item, error) {
	var item Item
	dec := gob.NewDecoder(strings.NewReader(s))
	err := dec.Decode(&item.kind)
	if err != nil {
		return nil, fmt.Errorf("decode kind: %w", err)
	}
	err = dec.Decode(&item.id)
	if err != nil {
		return nil, fmt.Errorf("decode id: %w", err)
	}
	err = dec.Decode(&item.fields)
	if err != nil {
		return nil, fmt.Errorf("decode fields: %w", err)
	}
	return &item, nil
}

func (i *Item) Kind() string {
	return i.kind
}

func (i *Item) ID() string {
	return i.id
}

func (i *Item) Validate() error {
	if i.kind == "" {
		return fmt.Errorf("kind must not be empty")
	}
	if i.id == "" {
		return fmt.Errorf("id must not be empty")
	}
	return nil
}

func (i *Item) AddInt64(
	name string,
	value int64,
) *Item {
	i.fields[name] = &ItemField{
		Value:     value,
		ValueType: ItemValueTypeInt64,
		Indexed:   false,
	}
	return i
}

func (i *Item) AddFloat64(
	name string,
	value float64,
) *Item {
	i.fields[name] = &ItemField{
		Value:     value,
		ValueType: ItemValueTypeFloat64,
		Indexed:   false,
	}
	return i
}

func (i *Item) AddString(
	name string,
	value string,
) *Item {
	i.fields[name] = &ItemField{
		Value:     value,
		ValueType: ItemValueTypeString,
		Indexed:   false,
	}
	return i
}

func (i *Item) AddAny(
	name string,
	value interface{},
) *Item {
	i.fields[name] = &ItemField{
		Value:     value,
		ValueType: ItemValueTypeAny,
		Indexed:   false,
	}
	return i
}

func (i *Item) AddInt64Indexed(
	name string,
	value int64,
) *Item {
	i.fields[name] = &ItemField{
		Value:     value,
		ValueType: ItemValueTypeInt64,
		Indexed:   true,
	}
	return i
}

func (i *Item) AddFloat64Indexed(
	name string,
	value float64,
) *Item {
	i.fields[name] = &ItemField{
		Value:     value,
		ValueType: ItemValueTypeFloat64,
		Indexed:   true,
	}
	return i
}

func (i *Item) GetField(
	fieldName string,
) *ItemField {
	return i.fields[fieldName]
}

func (i *Item) LookupInt64(
	fieldName string,
) (int64, bool) {
	f := i.fields[fieldName]
	if f != nil &&
		f.ValueType == ItemValueTypeInt64 {
		return f.Value.(int64), true
	}
	return 0, false
}

func (i *Item) GetInt64(
	fieldName string,
) int64 {
	f := i.fields[fieldName]
	if f != nil &&
		f.ValueType == ItemValueTypeInt64 {
		return f.Value.(int64)
	}
	return 0
}

func (i *Item) LookupFloat64(
	fieldName string,
) (float64, bool) {
	f := i.fields[fieldName]
	if f != nil &&
		f.ValueType == ItemValueTypeFloat64 {
		return f.Value.(float64), true
	}
	return 0, false
}

func (i *Item) GetFloat64(
	fieldName string,
) float64 {
	f := i.fields[fieldName]
	if f != nil &&
		f.ValueType == ItemValueTypeFloat64 {
		return f.Value.(float64)
	}
	return 0
}

func (i *Item) LookupString(
	fieldName string,
) (string, bool) {
	f := i.fields[fieldName]
	if f != nil &&
		f.ValueType == ItemValueTypeString {
		return f.Value.(string), true
	}
	return "", false
}

func (i *Item) GetString(
	fieldName string,
) string {
	f := i.fields[fieldName]
	if f != nil &&
		f.ValueType == ItemValueTypeString {
		return f.Value.(string)
	}
	return ""
}

func (i *Item) LookupAny(
	fieldName string,
) (interface{}, bool) {
	f := i.fields[fieldName]
	if f != nil &&
		f.ValueType == ItemValueTypeAny {
		return f.Value.(string), true
	}
	return nil, false
}

func (i *Item) GetAny(
	fieldName string,
) interface{} {
	f := i.fields[fieldName]
	if f != nil &&
		f.ValueType == ItemValueTypeAny {
		return f.Value.(string)
	}
	return nil
}

func (i *Item) FieldNames() []string {
	var names []string
	for n := range i.fields {
		names = append(names, n)
	}
	return names
}

func (i *Item) Serialize() (string, error) {
	var b bytes.Buffer
	enc := gob.NewEncoder(&b)
	err := enc.Encode(i.kind)
	if err != nil {
		return "", fmt.Errorf("encode kind: %w", err)
	}
	err = enc.Encode(i.id)
	if err != nil {
		return "", fmt.Errorf("encode id: %w", err)
	}
	err = enc.Encode(i.fields)
	if err != nil {
		return "", fmt.Errorf("encode fields: %w", err)
	}
	return b.String(), nil
}
