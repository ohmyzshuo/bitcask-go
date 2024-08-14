package bitcask_go

import (
	"bitcask-go/data"
	"bitcask-go/index"
	"errors"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// DB bitcask 數據引擎實例
type DB struct {
	options    Options                   // 用戶配置項
	mu         *sync.RWMutex             // 讀寫互斥鎖
	activeFile *data.DataFile            // 當前活躍數據文件，可以用於寫入
	olderFiles map[uint32]*data.DataFile // 舊的數據文件，只讀
	index      index.Indexer             // 內存索引
	fileIds    []int                     // 文件 ID， 只能在加載索引時使用，其他情況禁止
}

// Open 開啟數據庫
func Open(options Options) (*DB, error) {
	// 對用戶傳入的配置項進行校驗
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	// 判斷數據目錄是否存在，若不存在則創建
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 初始化 DB 實例結構體
	db := &DB{
		options:    options,
		mu:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.indexType),
	}

	// 加載對應的數據文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}
	// 從數據文件中加載索引
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}
	return db, nil
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

	// 追加寫入到當前活躍數據文件中
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

	// 根據文件 id 找到對應的數據文件
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
	record, _, err := file.ReadLogRecord(pos.Offset)
	if err != nil {
		return nil, err
	}
	if record.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}
	return record.Value, nil
}

func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	// 先檢查 key 是否存在，如果不存在直接返回
	if pos := db.index.Get(key); pos == nil {
		return nil
	}
	// 構造 LogRecord，標示其是被刪除的
	record := &data.LogRecord{Key: key, Type: data.LogRecordDeleted}
	// 寫入到數據文件中
	_, err := db.appendLogRecord(record)
	if err != nil {
		return err
	}

	ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

// appendLogRecord 追加寫數據到活躍文件中
func (db *DB) appendLogRecord(record *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	// 判斷當前活躍數據文件是否存在
	// 如果為空，則初始化數據文件
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// 寫入數據編碼
	encodedRecord, size := data.EncodeLogRecord(record)

	// 如果寫入的數據已經到達了活躍文件大小的閾值，則關閉活躍文件，並打開新的文件
	if db.activeFile.WriteOffset+size > db.options.DataFileSize {
		// 先持久化數據文件，保證已有的數據保存在磁盤中
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		// 將當前活躍文件轉換為舊的數據文件
		db.olderFiles[db.activeFile.FileId] = db.activeFile

		// 打開新的數據文件
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

// setActiveDataFile 設置當前活躍文件
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

// 從磁盤中加載數據到文件
func (db *DB) loadDataFiles() error {
	// 讀取目錄
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}
	var fileIds []int

	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			// 分割取 . 前面的 如 1351.data 取 1351
			splitNames := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])
			// 數據目錄有可能損壞
			if err != nil {
				return ErrDataDirectoryCorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}
	// 對文件 ID 進行排序，從小到大依次加載
	sort.Ints(fileIds)

	db.fileIds = fileIds

	// 遍歷每個文件 ID， 打開對應檔數據文件
	for i, fid := range fileIds {
		file, err := data.OpenDataFile(db.options.DirPath, uint32(fid))
		if err != nil {
			return err
		}

		if i == len(fileIds)-1 {
			// 最後一個， ID 是最大檔，說明是當前活躍文件
			db.activeFile = file
		} else {
			// 說明是舊的數據文件
			db.olderFiles[uint32(fid)] = file
		}
	}
	return nil
}

// loadIndexFromDataFiles 從數據文件中加載索引
// 遍歷文件中所有的記錄，並更新到內存索引中
func (db *DB) loadIndexFromDataFiles() error {
	// 沒有文件，說明數據庫是空的，直接返回
	if len(db.fileIds) == 0 {
		return nil
	}
	// 遍歷所有的文件 ID，處理文件中的記錄
	for i, fid := range db.fileIds {
		var fileId = uint32(fid)
		var file *data.DataFile
		if fileId == db.activeFile.FileId {
			file = db.activeFile
		} else {
			file = db.olderFiles[fileId]
		}
		var offset int64 = 0
		for {
			record, size, err := file.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			// 構造內存索引並保存
			pos := &data.LogRecordPos{
				Fid:    fileId,
				Offset: offset,
			}
			var ok bool
			if record.Type == data.LogRecordDeleted {
				ok = db.index.Delete(record.Key)
			} else {
				ok = db.index.Put(record.Key, pos)
			}
			if !ok {
				return ErrIndexUpdateFailed
			}
			// 遞增 offset，下一次從新的位置讀取
			offset += size
		}
		// 如果是最後一個文件，即當前活躍文件，更新這個文件的 WriteOffset
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOffset = offset
		}
	}
	return nil
}

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database directory path is invalid")
	}
	if options.DataFileSize == 0 {
		return errors.New("data file size must be greater than 0")
	}
	return nil
}
