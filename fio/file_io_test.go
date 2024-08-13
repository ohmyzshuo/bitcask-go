package fio

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func destroyFile(name string) {
	if err := os.RemoveAll(name); err != nil {
		panic(err)
	}
}
func TestNewFileIOManager(t *testing.T) {
	path := filepath.Join("/tmp", "a.data")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)

	assert.Nil(t, err)

	assert.NotNil(t, fio)
}

func TestFileIO_Write(t *testing.T) {
	path := filepath.Join("/tmp", "a.data")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)
	assert.Nil(t, err)

	assert.NotNil(t, fio)

	n, err := fio.Write([]byte(""))

	assert.Equal(t, 0, n)
	assert.Nil(t, err)
}

func TestFileIO_Read(t *testing.T) {
	path := filepath.Join("/tmp", "a.data")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)
	assert.Nil(t, err)

	assert.NotNil(t, fio)

	// Write data to the file
	_, err = fio.Write([]byte("key-a"))
	assert.Nil(t, err)
	_, err = fio.Write([]byte("key-b"))
	assert.Nil(t, err)

	// Create a buffer to read data into
	buf := make([]byte, 5)
	n, err := fio.Read(buf, 0)
	assert.Nil(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("key-a"), buf)

}
