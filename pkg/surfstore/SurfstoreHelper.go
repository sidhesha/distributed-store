package surfstore

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

/* Hash Related */
func GetBlockHashBytes(blockData []byte) []byte {
	h := sha256.New()
	h.Write(blockData)
	return h.Sum(nil)
}

func GetBlockHashString(blockData []byte) string {
	blockHash := GetBlockHashBytes(blockData)
	return hex.EncodeToString(blockHash)
}

/* File Path Related */
func ConcatPath(baseDir, fileDir string) string {
	return baseDir + "/" + fileDir
}

/*
	Writing Local Metadata File Related
*/

const createTable string = `create table if not exists indexes (
		fileName TEXT, 
		version INT,
		hashIndex INT,
		hashValue TEXT
	);`

const insertTuple string = `insert into indexes (fileName,version,hashIndex,hashValue) VALUES (?,?,?,?);`

// WriteMetaFile writes the file meta map back to local metadata file index.db
func WriteMetaFile(fileMetas map[string]*FileMetaData, baseDir string) error {
	// remove index.db file if it exists
	outputMetaPath := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(outputMetaPath); err == nil {
		e := os.Remove(outputMetaPath)
		if e != nil {
			log.Fatal("Error During Meta Write Back")
		}
	}
	db, err := sql.Open("sqlite3", outputMetaPath)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	defer db.Close()
	statement, err := db.Prepare(createTable)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement.Exec()
	for file_name, meta_data := range fileMetas {
		curr_name := file_name
		curr_version := meta_data.Version
		for curr_hash_idx, curr_hash_val := range meta_data.BlockHashList {
			statement, err := db.Prepare(insertTuple)
			if err != nil {
				log.Fatal("db prepare WriteMetaFile error: ", err)
			}
			statement.Exec(curr_name, curr_version, curr_hash_idx, curr_hash_val)
		}
	}
	return nil
}

/*
Reading Local Metadata File Related
*/
const getDistinctFileName string = `select distinct fileName From indexes;`

const getTuplesByFileName string = `select * from indexes where fileName=?`

// LoadMetaFromMetaFile loads the local metadata file into a file meta map.
// The key is the file's name and the value is the file's metadata.
// You can use this function to load the index.db file in this project.
func LoadMetaFromMetaFile(baseDir string) (fileMetaMap map[string]*FileMetaData, e error) {
	metaFilePath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))
	fileMetaMap = make(map[string]*FileMetaData)
	metaFileStats, e := os.Stat(metaFilePath)
	if e != nil || metaFileStats.IsDir() {
		return fileMetaMap, nil
	}
	db, err := sql.Open("sqlite3", metaFilePath)
	if err != nil {
		log.Fatal("Error When Opening Meta")
	}
	defer db.Close()
	// panic("todo")
	statement, err := db.Prepare(createTable)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement.Exec()
	// PrintMetaMap(fileMetaMap)
	var file_names []string
	file_rows, err := db.Query(getDistinctFileName)
	if err != nil {
		log.Println("got error while making filename query", err)
		return fileMetaMap, err
	}
	for file_rows.Next() {
		var curr_file_name string
		file_rows.Scan(&curr_file_name)
		file_names = append(file_names, curr_file_name)
		curr_meta_data := FileMetaData{Filename: curr_file_name, BlockHashList: make([]string, 0)}
		fileMetaMap[curr_file_name] = &curr_meta_data
	}
	// PrintMetaMap(fileMetaMap)
	for _, curr_file_name := range file_names {
		file_data, err := db.Query(getTuplesByFileName, curr_file_name)
		if err != nil {
			log.Println("got error while making getTuplesByFileName query")
			return fileMetaMap, err
		}
		// log.Println("finding: ", file_data)
		for file_data.Next() {
			var curr_name string
			var curr_version int32
			var curr_hash_idx int32
			var curr_hash_val string
			file_data.Scan(&curr_name, &curr_version, &curr_hash_idx, &curr_hash_val)
			// log.Println(curr_name, curr_version, curr_hash_idx, curr_hash_val)
			fileMetaMap[curr_name].Version = curr_version
			fileMetaMap[curr_name].BlockHashList = append(fileMetaMap[curr_name].BlockHashList, curr_hash_val)
		}
	}
	// PrintMetaMap(fileMetaMap)
	return fileMetaMap, nil
}

/*
	Debugging Related
*/

// PrintMetaMap prints the contents of the metadata map.
// You might find this function useful for debugging.
func PrintMetaMap(metaMap map[string]*FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version)
		for _, blockHash := range filemeta.BlockHashList {
			fmt.Println("\t", blockHash)
		}
	}

	fmt.Println("---------END PRINT MAP--------")

}
