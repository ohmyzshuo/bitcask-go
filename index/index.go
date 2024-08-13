package index

import (
	"bitcask-go/data"
	"bytes"
	"github.com/google/btree"
)

// Indexer 抽象索引接口
type Indexer interface {
	// Put 向索引中存儲 key 對應的數據位置信息
	Put(key []byte, pos *data.LogRecordPos) bool

	// Get 根據 key 取出對應的索引位置信息
	Get(key []byte) *data.LogRecordPos

	// Delete 根據 key 刪除對應的索引位置信息
	Delete(key []byte) bool
}

type IndexType = int8

const (
	// Btree 索引
	Btree IndexType = iota + 1

	// ART 自適應基數樹索引
	ART
)

// NewIndexer 根據類型初始化索引
func NewIndexer(typ IndexType) Indexer {
	switch typ {
	case Btree:
		return NewBTree()

	case ART:
		// TODO
		return nil
	default:
		panic("unsupported index type")
	}
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (ia *Item) Less(ib btree.Item) bool {
	return bytes.Compare(ia.key, ib.(*Item).key) == -1
}
