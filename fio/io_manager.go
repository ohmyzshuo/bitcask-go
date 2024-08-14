package fio

const DataFilePerm = 0644

// IOManager 抽象 IO 管理接口， 可以接入不同的 IO 類型
type IOManager interface {
	// Read 從文件給定的位置讀取對應的數據
	Read([]byte, int64) (int, error)

	// Write 寫入字節數組到文件中
	Write([]byte) (int, error)

	// Sync 將內存緩衝區的數據持久化到磁盤中
	Sync() error

	// Close 關閉文件
	Close() error

	// Size 獲取文件大小
	Size() (int64, error)
}

// NewIOManager 初始化 IOManager， 目前只支持標準 FileID
func NewIOManager(fileName string) (IOManager, error) {
	return NewFileIOManager(fileName)
}
