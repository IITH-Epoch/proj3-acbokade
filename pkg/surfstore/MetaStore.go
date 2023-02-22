package surfstore

import (
	context "context"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
	UnimplementedMetaStoreServer
}

// Returns mapping of files and its metadata (version, filename and hashlist)
func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	var fileInfoMap *FileInfoMap = &FileInfoMap{FileInfoMap: m.FileMetaMap}
	return fileInfoMap, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	fileName := fileMetaData.Filename
	fileVersion := fileMetaData.Version
	_, exists := m.FileMetaMap[fileName]
	if exists {
		curFileVersion := m.FileMetaMap[fileName].Version
		// Replace the metadata only if the version is 1 greater than current file version
		if fileVersion == 1+curFileVersion {
			m.FileMetaMap[fileName] = fileMetaData
		} else {
			// Else send version -1 to the client
			return &Version{Version: -1}, nil
		}
	} else {
		m.FileMetaMap[fileName] = fileMetaData
	}
	return &Version{Version: fileVersion}, nil
}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	var blockStoreAddr *BlockStoreAddr = &BlockStoreAddr{Addr: m.BlockStoreAddr}
	return blockStoreAddr, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
