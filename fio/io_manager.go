package fio

const DataFilePerm = 0644

// IOManager 抽象 IO 管理接口， 可以接入不同的 IO 類型
type IOManager interface {
	// Read 從文檔給定的位置讀取對應的數據
	Read([]byte, int64) (int, error)

	// Write 寫入字節數組到文檔中
	Write([]byte) (int, error)

	// Sync 將內存緩衝區的數據持久化到磁盤中
	Sync() error

	// Close 關閉文件
	Close() error
}
