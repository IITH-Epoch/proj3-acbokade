package surfstore

import (
	context "context"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	// Given the identifier (hash), returns the block
	block := bs.BlockMap[blockHash.Hash]
	return block, nil
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	// Compute hash, and add the block to the map
	hash := GetBlockHashString(block.BlockData)
	bs.BlockMap[hash] = block
	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	outHashes := make([]string, 0)
	// Iterate over the input hashes and check if it exists in the map or not
	for _, blockHash := range blockHashesIn.Hashes {
		_, exists := bs.BlockMap[blockHash]
		if exists {
			outHashes = append(outHashes, blockHash)
		}
	}
	return &BlockHashes{Hashes: outHashes}, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
