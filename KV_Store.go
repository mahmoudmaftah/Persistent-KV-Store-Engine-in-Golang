package main

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
)

// We will explain the following constants.
// 1. magicNumber : This is the magic number that we will write in the beginning of each SST file.
// 2. directory : This is the directory where we will store the SST files.
// 3. ext : This is the temporary extension of the SST files (To recover from failures).
// 4. defLoad : This is the default number of SST files that we will load into memory (for performance concerns).
// 5. treshold : This is the maximum number of records that we will store in the main memory before flushing to SST files.
// 6. sysVers : This is the system version (for future use). (We will use it to check if the SST files are compatible with the current system
// version).
// 7. mergeThreshold : This is the tolerable number of SST files that we can have when the system starts.

// In this project We tried to implement the singleton design pattern, you can still change the system settings by changing the consts
// defined below.

// We used an Auto Compaction at the Start and Stop of the kv store, the compaction algorithm keeps merging the SST files until the number
// of SST files is less than 10. You can change this number by changing the constant "mergeTreshold".

// We used a threshold to flush the main memory to SST files, you can change this number by changing the constant "treshold".
// you can still change the threshold and the default number of SST files loaded into memory.

// We used Go routines to start the treemap and sstManager structures Concurrently, We also used go routines to load the SST files into
// memory Concurrently.

const magicNumber uint64 = 0x1234567890ABCDEF
const directory string = "SSTFiles"
const ext string = ".tmp"
const defLoad uint64 = 1000
const treshold uint64 = 1000
const sysVers uint64 = 110011
const mergeThreshold uint64 = 10

// The kv store interface defines the methods for any kv Store instance.(Get, Set, Del, Start, Stop ...)
type KVStore interface {
	Get(string) (string, error)
	Set(string, string) error
	Del(string) (string, error)
	Start() error
	Stop() error
}

type MyKvStore struct {
	// Maximum number of Loaded SST files.
	sstM       *mySSTManager
	loadCount  int
	memDB      *PersMem
	sysVersion uint64
}

// NewKeyValueStore creates a new instance of the KeyValueStore.

func NewKeyValueStore() (*MyKvStore, error) {
	// Create the SST manager.
	sstM, err := NewSSTManager(defLoad, treshold)
	if err != nil {
		panic(err.Error())
	}

	// Create the main memory.
	memDB, err := NewPersMem()
	if err != nil {
		panic(err.Error())
	}

	return &MyKvStore{
		sstM:       sstM,
		loadCount:  0,
		memDB:      memDB,
		sysVersion: sysVers,
	}, nil
}

func (kv *MyKvStore) Start() error {

	err := kv.SSTCompaction()
	if err != nil {
		return err
	}

	kv.sstM.sstCount, err = CheckAndClean()
	if err != nil {
		return err
	}

	// Create a wait group.
	wg := &sync.WaitGroup{}

	go func(wg *sync.WaitGroup) error {
		defer wg.Done()

		wg.Add(1)
		// Load the WAL file into memory.
		err := kv.memDB.Load()
		if err != nil {
			return err
		}
		fmt.Println("WAL file loaded into memory")
		return nil
	}(wg)

	go func(wg *sync.WaitGroup) error {
		defer wg.Done()
		wg.Add(1)
		// Load the SST files into memory.
		if err := kv.sstM.LoadALL(); err != nil {
			return err
		}
		fmt.Println("SST files loaded into memory")
		return nil
	}(wg)

	wg.Wait()

	fmt.Println("Load Count :", kv.sstM.loadCount)
	fmt.Println("Load Index:", kv.sstM.loadIdx)
	fmt.Println("SST Count :", kv.sstM.sstCount)
	//fmt.Println(kv.sstM.loadCount)

	return nil
}

func (kv *MyKvStore) FlushToSST() error {

	fileName := fmt.Sprintf("%s/SST%d%s", directory, kv.sstM.sstCount, ext)
	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}

	// Close the file at the end of the function.
	// if the file is already closed, continue.
	defer file.Close()

	//file, err := kv.sstM.NextSSTFile()
	if err != nil {
		return err
	}

	// Write magic number:
	if err := binary.Write(file, binary.LittleEndian, magicNumber); err != nil {
		return err
	}

	// Write system version:
	if err := binary.Write(file, binary.LittleEndian, kv.sysVersion); err != nil {
		return err
	}

	// Write the records count:
	le := uint64(kv.memDB.store.Len())
	if err := binary.Write(file, binary.LittleEndian, le); err != nil {
		return err
	}

	// Write records
	for it := kv.memDB.store.Iterator(); it.Valid(); it.Next() {

		record := FileRecord{
			Operation: Operation(it.Value().operation),
			Key:       it.Key(),
			Value:     it.Value().value,
		}
		// Convert record to JSON
		data, err := json.Marshal(record)
		if err != nil {
			return err
		}

		// Write the length of the record
		if err := binary.Write(file, binary.BigEndian, int64(len(data))); err != nil {
			return err
		}

		// Write the record data
		_, err = file.Write(data)
		if err != nil {
			return err
		}

	}
	// Now that we could perform all operations we need to change file extension to .sst
	// Remark : The file is not yet officially an SST file.

	file.Close()
	newfileName := fmt.Sprintf("%s/SST%d.sst", directory, kv.sstM.sstCount)
	if err := os.Rename(file.Name(), newfileName); err != nil {
		return err
	}
	kv.sstM.sstCount++
	// Now we need to clear the main memory.
	if err := kv.memDB.Clear(); err != nil {
		return err
	}

	// Now we need to update the SST map.
	if err := kv.Update(); err != nil {
		return err
	}

	return nil
}

func (kv *MyKvStore) Update() error {
	fileName := fmt.Sprintf("%s/SST%d.sst", directory, kv.sstM.sstCount-1)
	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create sstmap
	ss := newSSTMap()
	if err := ss.LoadToMem(file); err != nil {
		return err
	}
	// Close the file at the end of the function.
	// if the file is already closed, continue.

	// append ss to memSST
	if kv.sstM.sstCount <= uint64(len(kv.sstM.memSST)) {
		kv.sstM.memSST[kv.sstM.sstCount-1] = *ss
	} else {
		kv.sstM.memSST = append(kv.sstM.memSST, *ss)
	}

	return nil
	// Show the size of each SST file in memory.
}

func (kv *MyKvStore) CheckIfFlush() error {
	// Check if the number of records in the main memory is greater than the threshold.

	if uint64(kv.memDB.store.Len()) > kv.sstM.loadThreshold {
		// Flush the main memory to SST files.
		fmt.Println("Need to flush!")
		err := kv.FlushToSST()
		if err != nil {
			fmt.Print(err.Error())
			return err
		}
		fmt.Println("Flushed!")
	}
	return nil
}

func (kv *MyKvStore) Stop() error {
	// Flush the main memory to SST files.
	fmt.Println("Stopping the rwina...")
	err := kv.memDB.Close()
	if err != nil {
		return err
	}

	err = kv.SSTCompaction()
	if err != nil {
		return err
	}

	return nil
}

func (kv *MyKvStore) Get(key string) (string, error) {

	// First look in the main memory.
	// If not found, look in the SST files.
	// If not found, return error.

	// GetM Returns an error only if the key isn't in the memDB at all.
	T, err := kv.memDB.GetM(key)

	if err == nil {
		// This means that we have the key with the corresponding value in our memDB.
		// If the operation is delete, return error.
		if T.operation == "del" {
			return "", errors.New("Key Deleted")
		}
		return T.value, nil
	} else {

		// First look in the SST files.
		//fmt.Println("Key not found in main memory, looking in SST files")
		val, err := kv.sstM.Search(key)
		//fmt.Println("Process finished")
		if err != nil {
			return "", err
		}
		return val, nil
	}
}

func (kv *MyKvStore) Set(key string, val string) error {
	defer kv.CheckIfFlush()
	if err := kv.memDB.SetM(key, val); err != nil {
		return err
	}
	return nil
}

func (kv *MyKvStore) Del(key string) (string, error) {
	defer kv.CheckIfFlush()

	s, err := kv.Get(key)
	if err != nil {
		return "", err
	}

	//fmt.Println("Key found in main memory, deleting...")
	// This means that we have the key with the corresponding value in our dataBase.
	err1 := kv.memDB.DelM1(key)

	fmt.Println("Ennnnnd")

	if err1 != nil {
		return "", err1
	}

	return s, nil

}

func (kv *MyKvStore) SSTCompaction() error {
	n, err := CheckAndClean()

	if err != nil {
		return err
	}
	for n > mergeThreshold {

		for i := uint64(0); i+1 < n; i += 2 {
			fmt.Printf("Merging SST%d and SST%d\n", i, i+1)
			err := kv.sstM.MergeSST(i, i+1)
			if err != nil {
				fmt.Println(err.Error())
				return err
			}
		}

		// If the number of SST files is odd, we need to rename the last SST file.
		if n%2 == 1 {
			fmt.Printf("Renaming SST%d to SST%d\n", n-1, n/2)
			err := os.Rename(fmt.Sprintf("%s/SST%d.sst", directory, n-1), fmt.Sprintf("%s/SST%d.sst", directory, n/2))
			if err != nil {
				return err
			}
			//kv.sstM.sstCount = kv.sstM.sstCount / 2
		}
		n, err = CheckAndClean()
		if err != nil {
			return err
		}

	}

	return nil
}
