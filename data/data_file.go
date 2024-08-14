package data

import (
	"bitcask-go/fio"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
)

var (
	ErrInValidCRC = errors.New("invalid crc value, log record may be corrupted")
)

const DataFileNameSuffix = ".data"

// DataFile 數據文件
type DataFile struct {
	FileId      uint32        // 文件 id
	WriteOffset int64         // 文件寫到了哪個位置
	IOManager   fio.IOManager // io 讀寫
}

// OpenDataFile 打開新的數據文件
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	// 拼出有路徑的文件名
	fileName := filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)
	// 初始化 IOManager 接口
	ioManager, err := fio.NewIOManager(fileName)

	if err != nil {
		return nil, err
	}
	return &DataFile{
		FileId:      fileId,
		WriteOffset: 0,
		IOManager:   ioManager,
	}, nil
}

// ReadLogRecord 根據 offset 從數據文件中讀取 LogRecord
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	fileSize, err := df.IOManager.Size()
	if err != nil {
		return nil, 0, err
	}
	// 如果讀取的最大 header 長度已經超過了文件的長度，則只需要讀取到文件的末尾即可
	var headerBytes int64 = maxLogRecordHeaderSize
	if offset+maxLogRecordHeaderSize > fileSize {
		headerBytes = fileSize - offset
	}

	// 讀取 header 信息
	headerBuf, err := df.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}

	header, headerSize := DecodeLogRecordHeader(headerBuf)

	// 表示讀取到了文件末尾，直接返回 EOF 錯誤
	if header == nil {
		return nil, 0, io.EOF
	}
	// 表示讀取到了文件末尾，直接返回 EOF 錯誤
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	// 取出對應的 key 和 value 的長度
	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	var recordSize = headerSize + keySize + valueSize

	record := &LogRecord{Type: header.recordType}

	// 開始讀取用戶實際存儲的 key-value 數據
	if keySize > 0 || valueSize > 0 {
		kvBuf, err := df.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}
		// 解出 key 和 value
		record.Key = kvBuf[:keySize]
		record.Value = kvBuf[keySize:]
	}

	// 校驗數據的有效性
	crc := getLogRecordCRC(record, headerBuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, ErrInValidCRC
	}

	return record, recordSize, nil
}
func (df *DataFile) Sync() error {
	return df.IOManager.Sync()
}

func (df *DataFile) Close() error {
	return df.IOManager.Close()
}

func (df *DataFile) Write(buf []byte) error {
	n, err := df.IOManager.Write(buf)
	if err != nil {
		return err
	}
	df.WriteOffset += int64(n)
	return nil
}

func (df *DataFile) readNBytes(n int64, offset int64) (b []byte, err error) {
	b = make([]byte, n)
	_, err = df.IOManager.Read(b, offset)
	return
}
