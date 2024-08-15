package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

// CRC type keySize valueSize
// 4 + 1  + 5   +   5        = 15
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 4 + 1

// LogRecord 寫入到數據文件的記錄
// 因為數據文件中的數據是追加寫入的，類似日誌，所以命名為日誌
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// LogRecordHeader LogRecord 的頭部信息
type LogRecordHeader struct {
	crc        uint32        // crc 校驗值
	recordType LogRecordType // 標識 LogRecord 的類型
	keySize    uint32        // Key 的長度
	valueSize  uint32        // Value 的長度
}

// LogRecordPos 數據內存索引，描述數據在磁盤上的位置
type LogRecordPos struct {
	Fid    uint32 // 文件 id，表示將數據存儲到了哪個文件中
	Offset int64  // 偏移量，表示將數據存儲到了文件件哪個位置
}

// EncodeLogRecord 對 LogRecord 進行編碼，返回字節數組及其長度
func EncodeLogRecord(record *LogRecord) ([]byte, int64) {
	// 初始化一個 header 部分的字節數組
	header := make([]byte, maxLogRecordHeaderSize)

	// 在第五個字節儲存 Type
	header[4] = record.Type
	var index = 5
	// 第五個字節後，存儲的是 key 和 value 的長度信息
	// 使用變長類型，節省空間
	index += binary.PutVarint(header[index:], int64(len(record.Key)))
	index += binary.PutVarint(header[index:], int64(len(record.Value)))

	var size = index + len(record.Key) + len(record.Value)

	encodedBytes := make([]byte, size)

	// 將 header 部分的數據拷貝過來
	copy(encodedBytes[:index], header[:index])

	// 將 Key 和 Value 數據拷貝到字節數組中
	copy(encodedBytes[index:], record.Key)
	index += len(record.Key)
	copy(encodedBytes[index:], record.Value)

	// 對整個 LogRecord 的數據進行 crc 校驗
	// 校驗的內容從第 5 個字節開始
	crc := crc32.ChecksumIEEE(encodedBytes[4:])
	// 小端序存儲數據
	binary.LittleEndian.PutUint32(encodedBytes[:4], crc)

	return encodedBytes, int64(size)
}

// DecodeLogRecordHeader 對字節數組中的 header 信息進行解碼
func DecodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}

	header := &LogRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
		keySize:    0,
		valueSize:  0,
	}

	// 第五個字節後，存儲的是 key 和 value 的長度信息
	var index = 5

	// 取出實際的 key size
	keySize, n := binary.Varint(buf[index:])
	header.keySize = uint32(keySize)
	index += n

	// 取出實際的 value size
	valueSize, n := binary.Varint(buf[index:])
	header.valueSize = uint32(valueSize)
	index += n

	return header, int64(index)
}

func getLogRecordCRC(record *LogRecord, header []byte) uint32 {
	if record == nil {
		return 0
	}

	crc := crc32.ChecksumIEEE(header[:])
	crc = crc32.Update(crc, crc32.IEEETable, record.Key)
	crc = crc32.Update(crc, crc32.IEEETable, record.Value)

	return crc
}
