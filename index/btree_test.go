package index

import (
	"bitcask-go/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBTree_Put(t *testing.T) {
	tree := NewBTree()

	res1 := tree.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})

	assert.True(t, res1)
	res2 := tree.Put([]byte("assert"), &data.LogRecordPos{Fid: 1, Offset: 50})
	assert.True(t, res2)
}

func TestBTree_Get(t *testing.T) {
	tree := NewBTree()

	tree.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})

	pos1 := tree.Get(nil)
	assert.Equal(t, uint32(1), pos1.Fid)

	res2 := tree.Put([]byte("assert"), &data.LogRecordPos{Fid: 1, Offset: 50})
	assert.True(t, res2)

	pos2 := tree.Get([]byte("assert"))

	assert.Equal(t, pos2.Fid, uint32(1))
	assert.Equal(t, pos2.Offset, int64(50))
}

func TestBTree_Delete(t *testing.T) {
	tree := NewBTree()

	tree.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})

	pos1 := tree.Get(nil)
	assert.Equal(t, uint32(1), pos1.Fid)

	res2 := tree.Put([]byte("assert"), &data.LogRecordPos{Fid: 1, Offset: 50})
	assert.True(t, res2)

	pos2 := tree.Get([]byte("assert"))
	assert.Equal(t, pos2.Fid, uint32(1))
	assert.Equal(t, pos2.Offset, int64(50))

	res4 := tree.Delete(nil)
	assert.True(t, res4)

	res5 := tree.Delete([]byte("assert"))
	assert.True(t, res5)
}
