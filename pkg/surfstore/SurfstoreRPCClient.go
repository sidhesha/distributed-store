package surfstore

import (
	context "context"
	"fmt"
	"log"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

func (surfClient *RPCClient) GetBlockHashes(blockStoreAddr string, blockHashes *[]string) error {
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	c := NewBlockStoreClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	dummy_empty := emptypb.Empty{}
	to_return, err := c.GetBlockHashes(ctx, &dummy_empty)
	if err != nil {
		log.Println(err)
		return err
	}
	*blockHashes = to_return.Hashes
	return nil
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	// start_time := time.Now()
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	// time_1 := time.Now()
	// log.Println("grpc getblock time1:", time_1.Sub(start_time))
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}

	// time_2 := time.Now()
	// log.Println("grpc getblock time2:", time_2.Sub(time_1))
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize
	// time_3 := time.Now()
	// log.Println("grpc getblock time3:", time_3.Sub(time_2))
	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	c := NewBlockStoreClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = c.PutBlock(ctx, block)
	if err != nil {
		log.Println("error in PutBlock", err)
		return err
	}

	return nil
}

func (surfClient *RPCClient) MissingBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	log.Println("In RPCclient MissingBlocks")
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	c := NewBlockStoreClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	to_send := BlockHashes{Hashes: blockHashesIn}
	got_hashes, err := c.MissingBlocks(ctx, &to_send)
	if err != nil {
		log.Println("error in MissingBlocks", err)
		return err
	}
	*blockHashesOut = got_hashes.Hashes
	return nil

}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	log.Println("In RPCclient GetFileInfoMap")
	for _, curr_meta_store := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(curr_meta_store, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		// defer conn.Close()
		c := NewRaftSurfstoreClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		// defer cancel()
		dummy_empty := emptypb.Empty{}
		got_map, err := c.GetFileInfoMap(ctx, &dummy_empty)
		if err != nil {
			conn.Close()
			cancel()
			log.Println(err)
			continue
		}
		*serverFileInfoMap = got_map.FileInfoMap
		conn.Close()
		cancel()
		return nil
	}
	return fmt.Errorf("no leader found")
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	log.Println("In RPCclient UpdateFile")
	for _, curr_meta_store := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(curr_meta_store, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		// defer conn.Close()
		c := NewRaftSurfstoreClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		// defer cancel()
		got_version, err := c.UpdateFile(ctx, fileMetaData)
		if err != nil {
			conn.Close()
			cancel()
			log.Println("oodoasdasfsafsad asdasd: ", err)
			continue
		}
		*latestVersion = got_version.Version
		cancel()
		conn.Close()
		return nil
	}
	return fmt.Errorf("no leader found")
}

func (surfClient *RPCClient) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {
	log.Println("In RPCclient GetBlockStoreMap")
	// panic("todo")
	for _, curr_meta_store := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(curr_meta_store, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		// defer conn.Close()
		c := NewRaftSurfstoreClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		// defer cancel()
		temp := BlockHashes{
			Hashes: blockHashesIn,
		}
		to_return, err := c.GetBlockStoreMap(ctx, &temp)
		if err != nil {
			conn.Close()
			cancel()
			log.Println(err)
			continue
		}
		to_return_2 := to_return.BlockStoreMap
		final_to_return := make(map[string][]string)
		for serve_add, b_hashes := range to_return_2 {
			final_to_return[serve_add] = b_hashes.Hashes
		}
		*blockStoreMap = final_to_return
		conn.Close()
		cancel()
		return nil
	}
	return fmt.Errorf("no leader found")
}

func (surfClient *RPCClient) GetBlockStoreAddrs(blockStoreAddrs *[]string) error {
	for _, curr_meta_store := range surfClient.MetaStoreAddrs {
		conn, err := grpc.Dial(curr_meta_store, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		// defer conn.Close()
		c := NewRaftSurfstoreClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		// defer cancel()
		dummy_empty := emptypb.Empty{}
		to_return, err := c.GetBlockStoreAddrs(ctx, &dummy_empty)
		if err != nil {
			conn.Close()
			cancel()
			log.Println(err)
			continue
		}
		*blockStoreAddrs = to_return.BlockStoreAddrs
		conn.Close()
		cancel()
		return nil
	}
	return fmt.Errorf("no leader found")
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {

	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
