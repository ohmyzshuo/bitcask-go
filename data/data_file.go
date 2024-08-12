package data

import (
	"bitcask-go/fio"
)

// DataFile 數據文檔
type DataFile struct {
	FileId      uint32        // 文檔 id
	WriteOffset int64         // 文檔寫到了哪個位置
	IOManager   fio.IOManager // io 讀寫
}

func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	return nil, nil
}

func (file *DataFile) ReadLogRecord(offset int64) (*LogRecord, error) {
	return nil, nil
}
func (file *DataFile) Sync() error {
	return nil
}

func (file *DataFile) Write(buf []byte) error {
	return nil
}
