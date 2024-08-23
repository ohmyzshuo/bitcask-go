package index

import (
	"bitcask-go/data"
	"github.com/google/btree"
	"sync"
)

// BTree 索引
type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32), // 葉子節點的數量
		lock: new(sync.RWMutex),
	}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := &Item{key, pos}
	bt.lock.Lock()

	// 如果 key 不存在直接插入
	// 如果 key 存在，則用新的 pos 替換舊的 pos
	bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	return true
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}

	btreeItem := bt.tree.Get(it)
	if btreeItem == nil {
		return nil
	}

	// btreeItem 是 Item 接口類型，因此要強制轉換
	return btreeItem.(*Item).pos
}

func (bt *BTree) Delete(key []byte) bool {
	it := &Item{
		key: key,
	}
	bt.lock.Lock()
	oldItem := bt.tree.Delete(it) // 右側返回的是刪除的那個值
	bt.lock.Unlock()

	// 刪除的 item 為空，則刪除失敗
	if oldItem == nil {
		return false
	}
	return true
}
