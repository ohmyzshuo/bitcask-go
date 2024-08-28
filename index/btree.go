package index

import (
	"bitcask-go/data"
	"bytes"
	"github.com/google/btree"
	"sort"
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

func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return newBTreeIterator(bt.tree, reverse)
}

// BTree 索引迭代器
type bTreeIterator struct {
	curIndex int     // 當前遍歷的下標位置
	reverse  bool    // 是否反向遍歷
	values   []*Item // key 與 位置索引信息
}

func newBTreeIterator(tree *btree.BTree, reverse bool) *bTreeIterator {
	var idx int
	values := make([]*Item, tree.Len())

	// 將所有的數據存放到數組中
	saveValues := func(it btree.Item) bool {
		values[idx] = it.(*Item)
		idx++
		return true
	}
	if reverse {
		tree.Descend(saveValues)
	} else {
		tree.Ascend(saveValues)
	}

	return &bTreeIterator{
		curIndex: 0,
		reverse:  reverse,
		values:   values,
	}
}

func (bti bTreeIterator) Rewind() {
	bti.curIndex = 0
}

func (bti bTreeIterator) Seek(key []byte) {
	if bti.reverse {
		bti.curIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) >= 0
		})
	} else {
		bti.curIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) <= 0
		})
	}
}

func (bti bTreeIterator) Next() {
	bti.curIndex++
}

func (bti bTreeIterator) Valid() bool {
	return bti.curIndex < len(bti.values)
}

func (bti bTreeIterator) Key() []byte {
	return bti.values[bti.curIndex].key
}

func (bti bTreeIterator) Value() *data.LogRecordPos {
	return bti.values[bti.curIndex].pos
}

func (bti bTreeIterator) Close() {
	bti.values = nil
}
