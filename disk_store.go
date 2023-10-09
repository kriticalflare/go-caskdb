package caskdb

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"time"
)

// DiskStore is a Log-Structured Hash Table as described in the BitCask paper. We
// keep appending the data to a file, like a log. DiskStorage maintains an in-memory
// hash table called KeyDir, which keeps the row's location on the disk.
//
// The idea is simple yet brilliant:
//   - Write the record to the disk
//   - Update the internal hash table to point to that byte offset
//   - Whenever we get a read request, check the internal hash table for the address,
//     fetch that and return
//
// KeyDir does not store values, only their locations.
//
// The above approach solves a lot of problems:
//   - Writes are insanely fast since you are just appending to the file
//   - Reads are insanely fast since you do only one disk seek. In B-Tree backed
//     storage, there could be 2-3 disk seeks
//
// However, there are drawbacks too:
//   - We need to maintain an in-memory hash table KeyDir. A database with a large
//     number of keys would require more RAM
//   - Since we need to build the KeyDir at initialisation, it will affect the startup
//     time too
//   - Deleted keys need to be purged from the file to reduce the file size
//
// Read the paper for more details: https://riak.com/assets/bitcask-intro.pdf
//
// DiskStore provides two simple operations to get and set key value pairs. Both key
// and value need to be of string type, and all the data is persisted to disk.
// During startup, DiskStorage loads all the existing KV pair metadata, and it will
// throw an error if the file is invalid or corrupt.
//
// Note that if the database file is large, the initialisation will take time
// accordingly. The initialisation is also a blocking operation; till it is completed,
// we cannot use the database.
//
// Typical usage example:
//
//		store, _ := NewDiskStore("books.db")
//	   	store.Set("othello", "shakespeare")
//	   	author := store.Get("othello")
type DiskStore struct {
	keyDir          map[string]KeyEntry
	readFileHandle  *os.File
	writeFileHandle *os.File
	currentOffset   uint32
}

func isFileExists(fileName string) bool {
	// https://stackoverflow.com/a/12518877
	if _, err := os.Stat(fileName); err == nil || errors.Is(err, fs.ErrExist) {
		return true
	}
	return false
}

func getKeyDir(fileName string) (map[string]KeyEntry, error) {
	var f *os.File
	defer f.Close()
	keyDir := make(map[string]KeyEntry)
	var err error
	if isFileExists(fileName) {
		f, err = os.Open(fileName)
		if err != nil {
			return nil, err
		}
	} else {
		f, err = os.Create(fileName)
		if err != nil {
			return nil, err
		}
	}
	offset := 0
	for {
		headerBuffer := make([]byte, headerSize)
		n, err := f.Read(headerBuffer)
		if err == io.EOF || n == 0 {
			break
		}
		if err != nil {
			return nil, err
		}

		timestamp, keySize, valueSize := decodeHeader(headerBuffer)
		kvBuffer := make([]byte, keySize+valueSize)
		n, err = f.Read(kvBuffer)
		if err != nil {
			return nil, err
		}
		if n == 0 {
			return nil, errors.New("EOF reading key")
		}
		data := append(headerBuffer, kvBuffer...)
		_, key, _ := decodeKV(data)
		totalSize := headerSize + keySize + valueSize
		keyDir[key] = NewKeyEntry(timestamp, uint32(offset), totalSize)
		offset += int(totalSize)
	}
	return keyDir, err
}

func NewDiskStore(fileName string) (*DiskStore, error) {
	var err error
	var writeFileHandle *os.File
	var readFileHandle *os.File
	keyDir, err := getKeyDir(fileName)
	if err != nil {
		return nil, err
	}

	writeFileHandle, err = os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	readFileHandle, err = os.Open(fileName)

	return &DiskStore{
		keyDir:          keyDir,
		writeFileHandle: writeFileHandle,
		readFileHandle:  readFileHandle,
		currentOffset:   0,
	}, err
}

func (d *DiskStore) Get(key string) string {
	var value string
	if keyEntry, ok := d.keyDir[key]; ok {
		d.readFileHandle.Seek(int64(keyEntry.Offset), 0)
		kvBuffer := make([]byte, keyEntry.Size)
		d.readFileHandle.Read(kvBuffer)
		_, _, value = decodeKV(kvBuffer)
	}

	return value
}

func (d *DiskStore) Set(key string, value string) {
	timestamp := uint32(time.Now().Unix())
	totalSize, encodedKV := encodeKV(timestamp, key, value)
	d.keyDir[key] = NewKeyEntry(timestamp, d.currentOffset, uint32(totalSize))
	d.writeFileHandle.Write(encodedKV)
	d.currentOffset += uint32(totalSize)
	err := d.writeFileHandle.Sync()
	if err != nil {
		panic(fmt.Sprintf("Failed to sync to disk %s", err.Error()))
	}
}

func (d *DiskStore) Close() bool {
	d.readFileHandle.Close()
	d.writeFileHandle.Close()
	return true
}
