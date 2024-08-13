package bitcask_go

type Options struct {
	// 數據庫檔數據目錄
	DirPath string

	// 數據文檔的大小
	DataFileSize int64

	//每次寫數據是否持久化
	SyncWrites bool

	// 索引類型
	indexType IndexerType
}

type IndexerType = int8

const (
	// Btree 索引
	Btree IndexerType = iota + 1

	// ART 自適應基數樹索引
	ART
)
