package surfstore

import (
	context "context"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	block_to_return, _ := bs.BlockMap[blockHash.Hash]
	// log.Println(bs.BlockMap)
	return block_to_return, nil
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {

	h_string := GetBlockHashString(block.BlockData)
	// log.Println("putting block", h_string)
	bs.BlockMap[h_string] = block
	suc := Success{Flag: true}
	return &suc, nil
}

// Given a list of hashes “in”, returns a list containing the
// hashes that are not stored in the key-value store
func (bs *BlockStore) MissingBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	var hash_out_arr []string
	for idx := 0; idx < len(blockHashesIn.Hashes); idx++ {
		hash_in := blockHashesIn.Hashes[idx]
		_, is_already_present := bs.BlockMap[hash_in]
		if is_already_present {
			continue
		} else {
			hash_out_arr = append(hash_out_arr, hash_in)
		}
	}
	var block_hashes_to_return = BlockHashes{Hashes: hash_out_arr}
	// log.Println("missing blocks hashes: ", hash_out_arr)
	return (&block_hashes_to_return), nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}

// Return a list containing all blockHashes on this block server
func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
	// panic("todo")
	var to_return []string
	for curr_hash, _ := range bs.BlockMap {
		to_return = append(to_return, curr_hash)
	}
	var block_hashes_to_return = BlockHashes{Hashes: to_return}
	return &block_hashes_to_return, nil
}
