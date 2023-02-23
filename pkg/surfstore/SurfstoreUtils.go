package surfstore

import (
	"io/ioutil"
	"log"
	"math"
	"os"
	"path/filepath"
)

// Returns number of blocks occupied by the file
func getNumberOfBlocks(fileSize int64, blockSize int) int {
	return int(math.Ceil(float64(fileSize) / float64(blockSize)))
}

func areEqualHashLists(first, second []string) bool {
	if len(first) != len(second) {
		return false
	}
	for idx, value := range first {
		if value != second[idx] {
			return false
		}
	}
	return true
}

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	// Scan each file in the base directory and compute file's hash list.
	filesHashListMap := make(map[string][]string) // key - fileName, value - hashlist
	allFiles, err := ioutil.ReadDir(client.BaseDir)
	if err != nil {
		log.Println("Error while ReadDir call", err)
	}
	for _, file := range allFiles {
		// Ignore index.db file
		if file.Name() == DEFAULT_META_FILENAME {
			continue
		}
		fileName := file.Name()
		fileSize := file.Size()
		numBlocks := getNumberOfBlocks(fileSize, client.BlockSize)
		filePath := filepath.Join(client.BaseDir, fileName)
		file, err := os.Open(filePath)
		if err != nil {
			log.Println("Error opening file", err)
		}
		hashList := make([]string, 0)
		// Read each block and find its hash and append to hashList
		for i := 0; i < numBlocks; i++ {
			block := make([]byte, client.BlockSize)
			bytesRead, err := file.Read(block)
			if err != nil {
				log.Println("Error while reading from file ", err)
			}
			block = block[:bytesRead]
			blockHash := GetBlockHashString(block)
			hashList = append(hashList, blockHash)
		}
		filesHashListMap[fileName] = hashList
	}

	// Load local index data from local db file
	localIndex, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		log.Println("Error while loading metadata from database", err)
	}

	// Connect to server and download update FileInfoMap (remote index)
	var remoteIndex = make(map[string]*FileMetaData)
	client.GetFileInfoMap(&remoteIndex)

	// Files which are present in remoteIndex and not in localIndex needs to be downloaded
	filesToDownload := make(map[string]bool)
	for fileName := range remoteIndex {
		_, exists := localIndex[fileName]
		if !exists {
			filesToDownload[fileName] = true
		} else {
			// File exists in local but outdated version
			if remoteIndex[fileName].Version > localIndex[fileName].Version {
				filesToDownload[fileName] = true
			}
		}
	}
	// Get BlockStoreAddr
	var blockStoreAddr string
	client.GetBlockStoreAddr(&blockStoreAddr)

	// Check the blocks to be downloaded
	for fileToDownload := range filesToDownload {
		downloadFile(fileToDownload, client, remoteIndex, localIndex, blockStoreAddr)
	}

	// Check the files which are newly added or edited
	newFilesAdded := make([]string, 0)
	editedFiles := make([]string, 0)
	for fileName := range filesHashListMap {
		_, exists := localIndex[fileName]
		_, remoteExists := remoteIndex[fileName]
		if !exists && !remoteExists {
			// File exists now in the local, but doesn't exist in the local index
			// Also, file doesn't exist in the remote index
			newFilesAdded = append(newFilesAdded, fileName)
		}
		if exists && remoteExists {
			// File exists in the local index, but has been changed since then
			// Version of the file in the local index and remote index are same
			if localIndex[fileName].Version == remoteIndex[fileName].Version {
				// Compare the hashList
				curHashList := filesHashListMap[fileName]
				prevHashList := localIndex[fileName].BlockHashList
				if !areEqualHashLists(curHashList, prevHashList) {
					editedFiles = append(editedFiles, fileName)
				}
			}
		}
	}

	filesToUpload := make([]string, 0)
	filesToUpload = append(filesToUpload, newFilesAdded...)
	filesToUpload = append(filesToUpload, editedFiles...)
	// Upload newly added files
	for _, fileName := range filesToUpload {
		returnedVersion, err := uploadFile(fileName, client, localIndex, blockStoreAddr)
		if err != nil || returnedVersion == -1 {
			// download only if it exists in remote index
			_, remoteExists := remoteIndex[fileName]
			if remoteExists {
				// outdated version
				downloadFile(fileName, client, remoteIndex, localIndex, blockStoreAddr)
			}
		} else {
			// Only if update is successful, update the localIndex db
			WriteMetaFile(localIndex, client.BaseDir)
		}
	}
}

func uploadFile(fileName string, client RPCClient, localIndex map[string]*FileMetaData, blockStoreAddr string) (int32, error) {
	localPath := filepath.Join(client.BaseDir, fileName)
	localFile, err := os.Open(localPath)
	if err != nil {
		log.Println("Error while opening file", err)
	}
	defer localFile.Close()

	fileStats, err := os.Stat(localPath)
	if err != nil {
		log.Println("Erro while fetching stats", err)
	}
	fileSize := fileStats.Size()
	numBlocks := getNumberOfBlocks(fileSize, client.BlockSize)
	for i := 0; i < numBlocks; i++ {
		blockData := make([]byte, client.BlockSize)
		bytesRead, err := localFile.Read(blockData)
		if err != nil {
			log.Println("Error while reading the file", err)
		}
		blockData = blockData[:bytesRead]
		blockSize := int32(bytesRead)
		blockObject := Block{BlockData: blockData, BlockSize: blockSize}
		var success bool
		err = client.PutBlock(&blockObject, blockStoreAddr, &success)
		if err != nil {
			log.Println("Error while putting block", err)
		}
		if !success {
			log.Println("PutBlock method not successful")
		}
	}
	var returnedVersion int32
	localFileMetadata := localIndex[fileName]
	err = client.UpdateFile(localFileMetadata, &returnedVersion)
	localIndex[fileName] = localFileMetadata
	if err != nil {
		returnedVersion = -1
	}
	return returnedVersion, err
}

func isFileDeleted(fileMetaData *FileMetaData) bool {
	if len(fileMetaData.BlockHashList) == 1 && fileMetaData.BlockHashList[0] == TOMBSTONE_HASHVALUE {
		return true
	}
	return false
}

func downloadFile(fileName string, client RPCClient, remoteIndex map[string]*FileMetaData, localIndex map[string]*FileMetaData, blockStoreAddr string) error {
	// Check if the file is deleted in the remote index (TOMBSTONE RECORD)
	if isFileDeleted(remoteIndex[fileName]) {
		// Copy metadata of file
		localIndex[fileName] = remoteIndex[fileName]
		return nil
	}
	localPath := filepath.Join(client.BaseDir, fileName)
	localFile, err := os.Create(localPath)
	if err != nil {
		log.Println("Error while creating file", err)
	}
	defer localFile.Close()
	// Write metadata of file and update localIndex object (not db)
	err = WriteMetaFile(remoteIndex, client.BaseDir)
	if err != nil {
		log.Println("Error in updating local index", err)
	}
	localIndex[fileName] = remoteIndex[fileName]
	fileContent := ""
	for _, blockHash := range remoteIndex[fileName].BlockHashList {
		var block Block
		err := client.GetBlock(blockHash, blockStoreAddr, &block)
		if err != nil {
			log.Println("Error while executing GetBlock call", err)
		}
		fileContent += string(block.BlockData)
	}
	localFile.WriteString(fileContent)
	return nil
}
