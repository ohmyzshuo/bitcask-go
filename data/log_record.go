package data

import "encoding/binary"

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

// CRC type keySize valueSize
// 4 + 1  + 5   +   5        = 15
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

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
	return nil, 0
}

// DecodeLogRecordHeader 對字節數組中的 header 信息進行解碼
func DecodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64) {
	return nil, 0
}

func getLogRecordCRC(record *LogRecord, header []byte) uint32 {
	return 0
}
