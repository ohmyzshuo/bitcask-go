package data

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

// LogRecord 寫入到數據文檔的記錄
// 因為數據文檔中的數據是追加寫入的，類似日誌，所以命名為日誌
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// LogRecordPos 數據內存索引，描述數據在磁盤上的位置
type LogRecordPos struct {
	Fid    uint32 // 文檔 id，表示將數據存儲到了哪個文檔中
	Offset int64  // 偏移量，表示將數據存儲到了文檔檔哪個位置
}

// EncodeLogRecord 對 LogRecord 進行編碼，返回字節數組及其長度
func EncodeLogRecord(record *LogRecord) ([]byte, int64) {
	return nil, 0
}
