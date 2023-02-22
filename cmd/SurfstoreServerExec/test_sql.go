package main

import (
	"cse224/proj4/pkg/surfstore"
    "database/sql"
    "fmt"
	"log"

    _ "github.com/mattn/go-sqlite3"
)

const getDistinctFileName string = `select distinct fileName, version from indexes;`

const getTuplesByFileName string = `select fileName, version, hashIndex, hashValue from indexes where fileName="%s" AND version=%d order by hashIndex ASC`

type fileNameVersion struct {
	fileName string 
	version int32
}
// select * from indexes where fileName='image.jpg' group by version order by hashIndex ASC

func main() {
	db, err := sql.Open("sqlite3", "../../../test_index.db")
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	rows, err := db.Query(getDistinctFileName)
	if err != nil {
		log.Fatal("Error while querying distinct file names", err)
	}
	defer rows.Close()
	var distinctFileNames []fileNameVersion = make([]fileNameVersion, 0)
	for rows.Next() {
		var fileName string
		var version int32
		err := rows.Scan(&fileName, &version)
		if err != nil {
			log.Fatal("Error while scanning the distinct file names rows")
		}
		distinctFileNames = append(distinctFileNames, fileNameVersion{fileName, version})
	}
	fmt.Println(distinctFileNames)
	var fileMetaData []surfstore.FileMetaData = make([]surfstore.FileMetaData, 0)
	for _, fileNameVersion := range distinctFileNames {
		rows_, err := db.Query(fmt.Sprintf(getTuplesByFileName, fileNameVersion.fileName, fileNameVersion.version))
		if err != nil {
			log.Fatal("Error while querying tuples of file", err)
		}
		defer rows_.Close()
		var hashValues []string = make([]string, 0)
		for rows_.Next() {
			var fileName string
			var version int 
			var hashIndex int
			var hashValue string
			err := rows_.Scan(&fileName, &version, &hashIndex, &hashValue)
			if err != nil {
				log.Fatal("Error while scanning the ", err)
			}
			hashValues = append(hashValues, hashValue)
		}
		fileMetaData = append(fileMetaData, surfstore.FileMetaData{Filename: fileNameVersion.fileName, Version: fileNameVersion.version, BlockHashList: hashValues})
	}
	fmt.Println(fileMetaData)
}