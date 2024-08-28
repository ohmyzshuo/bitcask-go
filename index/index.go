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

	// Iterator 返回索引迭代器
	Iterator(reverse bool) Iterator
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

// Iterator 通用索引迭代器
type Iterator interface {
	// Rewind 重新回到迭代器的起點，即第一個數據
	Rewind()

	// Seek 根據傳入的 key 查找到第一個大於(或小於)等於的目標 key，從這個 key 開始遍歷
	Seek(key []byte)

	// Next 跳轉到下一個 key
	Next()

	// Valid 是否有效，即是否已經遍歷完了所有的 key，用於退出遍歷
	Valid() bool

	// Key 當前遍歷位置的 Key 數據
	Key() []byte

	// Value 當前遍歷位置的 Value 數據
	Value() *data.LogRecordPos

	// Close 關閉迭代器，釋放相應資源
	Close()
}
