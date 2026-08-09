package storage

import (
	"strconv"

	"github.com/sirrobot01/appendstore"
)

// The hot fields carried in the store index. appendstore keeps them as a
// generic attribute map rather than named columns, so every value is a string
// and the numeric ones are parsed back on the way out.
const (
	attributeCategory  = "category"
	attributeProvider  = "provider"
	attributeStatus    = "status"
	attributeName      = "name"
	attributeTotalSize = "total_size"
	attributeProtocol  = "protocol"
	attributeBad       = "bad"
	attributeAddedOn   = "added_on"
)

func entryPutOptions(entry *Entry) *appendstore.PutOptions {
	return &appendstore.PutOptions{Attributes: map[string]string{
		attributeCategory:  entry.Category,
		attributeProvider:  entry.ActiveProvider,
		attributeStatus:    string(entry.Status),
		attributeName:      entry.GetFolder(), // computed folder name, for fast listings
		attributeTotalSize: strconv.FormatInt(entry.Size, 10),
		attributeProtocol:  string(entry.Protocol),
		attributeBad:       strconv.FormatBool(entry.Bad),
		attributeAddedOn:   strconv.FormatInt(entry.AddedOn.Unix(), 10),
	}}
}

// indexedPutOptions carries the attributes already in the index forward across
// a re-Put that only rewrites the value. Put replaces the whole attribute set,
// so a rewrite that passed nil would silently strip the hot fields from every
// row it touched.
//
// The map is safe to hand over: appendstore returns a defensive copy from
// GetMetadata, so nothing else holds a reference to it.
func indexedPutOptions(meta *appendstore.Metadata) *appendstore.PutOptions {
	if meta == nil {
		return nil
	}
	return &appendstore.PutOptions{Attributes: meta.Attributes}
}

func metadataInt64(meta *appendstore.Metadata, attribute string) int64 {
	value, _ := strconv.ParseInt(meta.Attribute(attribute), 10, 64)
	return value
}

func metadataBool(meta *appendstore.Metadata, attribute string) bool {
	value, _ := strconv.ParseBool(meta.Attribute(attribute))
	return value
}
