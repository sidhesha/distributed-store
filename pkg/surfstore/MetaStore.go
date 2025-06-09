package surfstore

import (
	context "context"
	"log"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	map_to_return := FileInfoMap{FileInfoMap: m.FileMetaMap}
	return (&map_to_return), nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	to_change_metadata, is_present := m.FileMetaMap[fileMetaData.Filename]
	if is_present {
		log.Println(to_change_metadata.Version, ": ", fileMetaData.Version)
		if to_change_metadata.Version+1 == fileMetaData.Version {
			m.FileMetaMap[fileMetaData.Filename] = fileMetaData
			curr_version := m.FileMetaMap[fileMetaData.Filename].Version
			v := Version{Version: curr_version}
			log.Println("file ", fileMetaData.Filename, "updated to version: ", v.Version)
			return &v, nil
		} else {
			v := Version{Version: int32(-1)}
			log.Println("File seems tooo old!!")
			return &v, nil
		}
	}
	// now the file is not present so have to create a new one
	m.FileMetaMap[fileMetaData.Filename] = fileMetaData
	v := Version{Version: fileMetaData.Version}
	log.Println("new file ", fileMetaData.Filename, "created with version: ", v.Version)
	// PrintMetaMap(m.FileMetaMap)
	return &v, nil
}

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	// panic("todo")
	to_return := BlockStoreMap{
		BlockStoreMap: map[string]*BlockHashes{},
	}
	for _, curr_server_add := range m.BlockStoreAddrs {
		temp_block_hash := BlockHashes{
			Hashes: make([]string, 0),
		}
		to_return.BlockStoreMap[curr_server_add] = &temp_block_hash
	}
	for _, curr_hash := range blockHashesIn.Hashes {
		server_add := m.ConsistentHashRing.GetResponsibleServer(curr_hash)
		to_return.BlockStoreMap[server_add].Hashes = append(to_return.BlockStoreMap[server_add].Hashes, curr_hash)
	}
	return &to_return, nil
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	// panic("todo")
	to_return_1 := m.BlockStoreAddrs
	final_to_return := BlockStoreAddrs{BlockStoreAddrs: to_return_1}
	return &final_to_return, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
