package bitcask_go

import (
	"bitcask-go/data"
	"bitcask-go/index"
	"sync"
)

// DB bitcask 數據引擎實例
type DB struct {
	options    Options
	mu         *sync.RWMutex
	activeFile *data.DataFile            // 當前活躍數據文檔，可以用於寫入
	olderFiles map[uint32]*data.DataFile // 舊的數據文檔，只讀
	index      index.Indexer             // 內存索引
}

// Put 寫入 key-value 數據 (key 非空)
func (db *DB) Put(key []byte, value []byte) error {
	// 判斷 key 是否有效
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 構造 LogRecord 結構體
	record := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 追加寫入到當前活躍數據文檔中
	pos, err := db.appendLogRecord(record)
	if err != nil {
		return err
	}

	// 更新內存索引
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {

	// 因為是讀操作，所以用 RLock
	db.mu.RLock()
	defer db.mu.RUnlock()

	// 判斷 key 有效
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	// 從內存數據結構中取出 key 對應的索引信息
	pos := db.index.Get(key)
	// 如果索引信息不存在，說明 key 不在數據庫中
	if pos == nil {
		return nil, ErrKeyNotFound
	}

	// 根據文檔 id 找到對應的數據文檔
	var file *data.DataFile
	if db.activeFile.FileId == pos.Fid {
		file = db.activeFile
	} else {
		file = db.olderFiles[pos.Fid]
	}

	// 數據文件為空
	if file == nil {
		return nil, ErrDataFileNotFound
	}

	// 根據 offset 讀取對應的數據
	record, err := file.ReadLogRecord(pos.Offset)
	if err != nil {
		return nil, err
	}
	if record.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}
	return record.Value, nil
}

// appendLogRecord 追加寫數據到活躍文檔中
func (db *DB) appendLogRecord(record *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// 判斷當前活躍數據文檔是否存在
	// 如果為空，則初始化數據文檔
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// 寫入數據編碼
	encodedRecord, size := data.EncodeLogRecord(record)

	// 如果寫入的數據已經到達了活躍文檔大小檔閾值，則關閉活躍文檔，並打開新的文檔
	if db.activeFile.WriteOffset+size > db.options.DataFileSize {
		// 先持久化數據文檔，保證已有的數據保存在磁盤中
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		// 將當前活躍文檔轉換為舊的數據文檔
		db.olderFiles[db.activeFile.FileId] = db.activeFile

		// 打開新的數據文檔
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	offset := db.activeFile.WriteOffset

	if err := db.activeFile.Write(encodedRecord); err != nil {
		return nil, err
	}

	// 根據用戶配置決定是否持久化
	if db.options.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	// 構造內存索引信息
	pos := &data.LogRecordPos{
		Fid:    db.activeFile.FileId,
		Offset: offset,
	}
	return pos, nil
}

// 設置當前活躍文檔
// 在訪問此方法前必須持有互斥鎖
func (db *DB) setActiveDataFile() error {
	var initialFileId uint32 = 0
	if db.activeFile != nil {
		initialFileId = db.activeFile.FileId + 1
	}

	// 打開新的數據文件
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId)
	if err != nil {
		return err
	}

	db.activeFile = dataFile

	return nil
}
