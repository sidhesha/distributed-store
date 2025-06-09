package surfstore

import (
	"log"
	"os"
	"path/filepath"
	"time"
)

func get_block_hashes_for_file(file_path string, block_size int) ([]string, error) {
	var to_return []string
	file_data, err := os.ReadFile(file_path)
	if err != nil {
		// log.Println("error reading file:", err)
		return to_return, err
	}
	// empty file case
	if len(file_data) == 0 {
		to_return = append(to_return, EMPTYFILE_HASHVALUE)
		return to_return, nil
	}
	for idx := 0; idx < len(file_data); idx += block_size {
		end := idx + block_size
		end = min(len(file_data), end)
		curr_block := file_data[idx:end]
		curr_block_hash := GetBlockHashString(curr_block)
		// log.Println("block->hash mapping", curr_block_hash, file_path, idx, end)
		to_return = append(to_return, curr_block_hash)
	}
	return to_return, nil
}

func is_hash_list_same(a []string, b []string) bool {
	count := make(map[string]int)
	for _, h := range a {
		count[h] += 1
	}
	for _, h := range b {
		count[h] -= 1
	}
	same := true
	for _, freq := range count {
		if freq != 0 {
			same = false
			break
		}
	}
	return ((len(a) == len(b)) && same)
}

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	//load index.db first
	local_index_db_data, err := LoadMetaFromMetaFile(client.BaseDir)

	if err != nil {
		log.Println("os readdir error: ", err)
	}
	// PrintMetaMap(local_index_db_data)
	// first lets check for file deleted locally but still valid record in index.db
	for curr_file_name, curr_file_meta_data := range local_index_db_data {
		curr_file_path, _ := filepath.Abs(ConcatPath(client.BaseDir, curr_file_name))
		_, e := os.Stat(curr_file_path)
		if os.IsNotExist(e) && curr_file_meta_data.BlockHashList[0] != TOMBSTONE_HASHVALUE {
			curr_file_meta_data.Version = curr_file_meta_data.Version + 1
			var deleted_hash_val []string
			deleted_hash_val = append(deleted_hash_val, TOMBSTONE_HASHVALUE)
			curr_file_meta_data.BlockHashList = deleted_hash_val
		}
	}
	// now lets scan local dir for changes
	// read base directory
	local_file_vec, err := os.ReadDir(client.BaseDir)
	if err != nil {
		log.Println("os readdir error: ", err)
	}
	// log.Println("directory list", local_file_vec)
	for _, curr_file := range local_file_vec {
		if curr_file.Name() != DEFAULT_META_FILENAME {
			// divide into blocks and store block hashes in the map
			curr_file_path, _ := filepath.Abs(ConcatPath(client.BaseDir, curr_file.Name()))
			if err != nil {
				log.Println("error concatenating  file path", curr_file.Name(), err)
			}
			// log.Println("hashing input:", curr_file_path, client.BlockSize)
			block_hash_list, err := get_block_hashes_for_file(curr_file_path, client.BlockSize)
			if err != nil {
				log.Println("error dividing file into blocks", curr_file.Name(), err)
			}
			// now compare this to index.db data
			local_data_curr_file, is_present := local_index_db_data[curr_file.Name()]
			// if is not present then create new file
			if !is_present {
				// log.Println("New file found locally, updating local indexdb", curr_file.Name())
				local_index_db_data[curr_file.Name()] = &FileMetaData{
					Filename:      curr_file.Name(),
					Version:       int32(VERSION_INDEX),
					BlockHashList: block_hash_list,
				}
			} else if is_present && !is_hash_list_same(local_data_curr_file.BlockHashList, block_hash_list) {
				// if is present but file is different then update locally
				// log.Println("Updated file found locally, updating local indexdb", curr_file.Name())
				local_index_db_data[curr_file.Name()] = &FileMetaData{
					Filename:      curr_file.Name(),
					Version:       int32(local_data_curr_file.Version + 1),
					BlockHashList: block_hash_list,
				}
			}
		}
	}
	// Next, the client should connect to the server and download an updated FileInfoMap. For the purposes of this discussion, let’s call this the “remote index.”
	remote_index_meta_data := map[string]*FileMetaData{}
	err = client.GetFileInfoMap(&remote_index_meta_data)
	if err != nil {
		log.Println("got error fetching remote meta data", err)
	}
	// PrintMetaMap(remote_index_meta_data)
	// The client should now compare the local index (and any changes to local files not reflected in the local index) with the remote index
	for rem_file_name, rem_file_meta_data := range remote_index_meta_data {
		// First, it is possible that the remote index refers to a file not present in the local index
		local_file_meta_data, is_present_locally := local_index_db_data[rem_file_name]
		have_to_get_from_cloud := false
		// first if it is not present locally
		if !is_present_locally {
			have_to_get_from_cloud = true
		}
		// either cloud has better version
		if is_present_locally && rem_file_meta_data.Version > local_file_meta_data.Version {
			have_to_get_from_cloud = true
		}
		// or updated file got synced to cloud first so this client loses and gets from cloud
		if is_present_locally && (rem_file_meta_data.Version == local_file_meta_data.Version && !is_hash_list_same(local_file_meta_data.BlockHashList, rem_file_meta_data.BlockHashList)) {
			have_to_get_from_cloud = true
		}
		// log.Println("raeched here download part", have_to_get_from_cloud)
		// finally
		if have_to_get_from_cloud {
			curr_file_path, err := filepath.Abs(ConcatPath(client.BaseDir, rem_file_name))
			// log.Println("raeched here download part", curr_file_path)
			// this should come first otherwise empty file case would go wrong.
			created_file, err := os.Create(curr_file_path)
			if err != nil {
				log.Println("error creating file", curr_file_path)
			}
			defer created_file.Close()
			// handle deleted file from remote case
			if rem_file_meta_data.BlockHashList[0] == TOMBSTONE_HASHVALUE {
				os.Remove(curr_file_path)
				local_index_db_data[rem_file_meta_data.Filename] = rem_file_meta_data
				continue
			}
			if rem_file_meta_data.BlockHashList[0] == EMPTYFILE_HASHVALUE {
				local_index_db_data[rem_file_meta_data.Filename] = rem_file_meta_data
				continue
			}
			// first get blocks then meta
			var block_store_map map[string][]string
			// lololol.
			start_time := time.Now()
			err = client.GetBlockStoreMap(rem_file_meta_data.BlockHashList, &block_store_map)
			if err != nil {
				log.Println("error getting blockstoreaddresses")
			}
			hash_to_blockstore := make(map[string]string)

			for curr_block_store, curr_hash_list := range block_store_map {
				for _, curr_hash := range curr_hash_list {
					hash_to_blockstore[curr_hash] = curr_block_store
				}
			}
			end_time := time.Now()
			log.Println("time taken for processing blockstoremap", end_time.Sub(start_time))
			var completed_block []byte
			for _, curr_hash := range rem_file_meta_data.BlockHashList {
				curr_block := Block{
					BlockData: make([]byte, 0),
					BlockSize: 0,
				}
				// log.Println("raeched here download part", rem_file_meta_data)
				block_store_addr := hash_to_blockstore[curr_hash]
				time_internal_1 := time.Now()
				err := client.GetBlock(curr_hash, block_store_addr, &curr_block)
				if err != nil {
					log.Println("error getting block", err)
				}
				time_internal_2 := time.Now()
				log.Println("this block took:", time_internal_2.Sub(time_internal_1))
				completed_block = append(completed_block, curr_block.BlockData...)

			}
			end_time2 := time.Now()
			log.Println("time taken for getting blocks", end_time2.Sub(end_time))
			_, err = created_file.Write(completed_block)
			if err != nil {
				log.Println("error writing to file", curr_file_path)
			} else {
				log.Println("file write completd")
			}
			end_time3 := time.Now()
			log.Println("time taken for writing files", end_time3.Sub(end_time2))
			// now finally update metadata
			local_index_db_data[rem_file_meta_data.Filename] = rem_file_meta_data
		}
	}
	// update the local index.db using sql query
	WriteMetaFile(local_index_db_data, client.BaseDir)
	// Next, it is possible that there are new files in the local base directory that aren’t in the local index or in the remote index.
	// The client should upload the blocks corresponding to this file to the server, then update the server with the new FileInfo
	for local_file_name, local_file_meta_data := range local_index_db_data {
		rem_file_meta_data, is_present_remotely := remote_index_meta_data[local_file_name]
		// PrintMetaMap(remote_index_meta_data)
		// PrintMetaMap(local_index_db_data)
		have_to_put_to_cloud := false
		if !is_present_remotely {
			have_to_put_to_cloud = true
			// need to check on this
		}
		if is_present_remotely && local_file_meta_data.Version == rem_file_meta_data.Version+1 {
			have_to_put_to_cloud = true
		}
		if is_present_remotely && local_file_meta_data.Version == rem_file_meta_data.Version+1 {
			have_to_put_to_cloud = true
		}
		log.Println("reached herer put", have_to_put_to_cloud)
		// finally have to put data to cloud
		if have_to_put_to_cloud {
			// update blocks first
			// lololol.
			var block_store_map map[string][]string
			err = client.GetBlockStoreMap(local_file_meta_data.BlockHashList, &block_store_map)
			if err != nil {
				log.Println("error getting blockstoreaddresses")
			}
			missing_block_hashes_map := make(map[string]string)
			for block_store_addr, curr_hash_list := range block_store_map {
				var missing_block_hashes []string
				err = client.MissingBlocks(curr_hash_list, block_store_addr, &missing_block_hashes)
				if err != nil {
					log.Println("Error getting missing blocks", err)
				}
				// log.Println(missing_block_hashes)
				for _, curr_hash := range missing_block_hashes {
					missing_block_hashes_map[curr_hash] = block_store_addr
				}
			}
			// log.Println("blcokstore map", block_store_map)
			// log.Println("missing hashes map", missing_block_hashes_map)
			// log.Println("block_store_address is:", &block_store_addr)
			curr_file_path, _ := filepath.Abs(ConcatPath(client.BaseDir, local_file_name))
			_, err = os.Stat(curr_file_path)
			// if file is deleted
			if os.IsNotExist(err) {
				// file deleted so just update metadata after else
			} else {
				curr_file_data, err := os.ReadFile(curr_file_path)
				if err != nil {
					log.Println("error reading file: 1 - ", err)
				}
				for idx := 0; idx < len(curr_file_data); idx += client.BlockSize {
					end := idx + client.BlockSize
					end = min(len(curr_file_data), end)
					curr_block := curr_file_data[idx:end]
					curr_block_hash := GetBlockHashString(curr_block)
					block_store_addr, is_present := missing_block_hashes_map[curr_block_hash]
					if is_present {
						// need to put it in blockstore
						curr_block := Block{
							BlockSize: int32(len(curr_block)),
							BlockData: curr_block,
						}
						succ := true
						client.PutBlock(&curr_block, block_store_addr, &succ)
					}
				}
			}
			// now update meta_data
			latest_version := local_file_meta_data.Version
			err = client.UpdateFile(local_file_meta_data, &latest_version)
			if err != nil {
				log.Println("error while updating metadata", err)
			}
			if latest_version == -1 {
				log.Println("file old hence not updated")
			}
		}

	}
}
