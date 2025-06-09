package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap map[string]string
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	// panic("todo")
	var to_sort []string
	for hashed_val, _ := range c.ServerMap {
		to_sort = append(to_sort, hashed_val)
	}
	sort.Strings(to_sort)
	var ans_server string
	found := false
	// log.Println("server is hashed here", to_sort, c.ServerMap)
	for _, server_hash := range to_sort {
		if blockId < server_hash {
			ans_server = c.ServerMap[server_hash]
			found = true
			break
		}
	}
	if !found {
		ans_server = c.ServerMap[to_sort[0]]
	}
	return ans_server
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))

}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	// panic("todo")
	// map key hash, value original server name
	curr_hash_ring := ConsistentHashRing{
		ServerMap: map[string]string{},
	}
	for _, server_add := range serverAddrs {
		updated_server_add := "blockstore" + server_add
		hashed_val := curr_hash_ring.Hash(updated_server_add)
		curr_hash_ring.ServerMap[hashed_val] = server_add
	}
	return &curr_hash_ring
}
