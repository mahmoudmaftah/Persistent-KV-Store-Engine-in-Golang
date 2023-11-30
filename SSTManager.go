package main

// We will write the structure of the SST file as follows:

// 1. Magic number (8 bytes)
// 2. Number of records (8 bytes)
// 3. Key length (8 bytes)
// 4. Key (variable length)
// 5. Value length (8 bytes)
// 6. Value (variable length)

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
)

type SSTManager interface {
	WriteToSST(filename string, records map[string]string) error
}

// The mySSTManager struct implements the SSTManager interface.
// It will manage multiple SST files. It will keep track of the number of SST files and their names in a separate file.
// It will also keep track of the SST files in memory.

type SSTMap struct {
	mp map[string]Tuple
}

func newSSTMap() *SSTMap {
	v := make(map[string]Tuple)
	return &SSTMap{mp: v}
}

func (stm *SSTMap) LoadToMem(fl *os.File) error {

	// Read magic number
	var magic uint64
	if err := binary.Read(fl, binary.LittleEndian, &magic); err != nil {
		return err
	}

	if magic != magicNumber {
		panic("Invalid SST file")
	}

	// Read system version
	var sysVersion uint64
	if err := binary.Read(fl, binary.LittleEndian, &sysVersion); err != nil {
		return err
	}

	if sysVersion != sysVers {
		panic("Non Compatible SST file (Check the system version)")
	}

	// Read number of records
	var numRecords uint64
	if err := binary.Read(fl, binary.LittleEndian, &numRecords); err != nil {
		return err
	}

	// Read records
	for i := uint64(0); i < numRecords; i++ {

		var length int64
		if err := binary.Read(fl, binary.BigEndian, &length); err != nil {
			return err
		}

		// Read the record data
		data := make([]byte, length)
		_, err := io.ReadFull(fl, data)
		if err != nil {
			return err
		}

		// Unmarshal the JSON data into a WALRecord
		var record FileRecord
		err = json.Unmarshal(data, &record)
		if err != nil {
			return err
		}

		// Put the record in the treemap
		stm.mp[record.Key] = Tuple{operation: string(record.Operation), value: record.Value}
	}
	return nil
}

type mySSTManager struct {
	// Number of SST files.
	sstCount uint64
	// #of SST files to be loaded to memory.
	loadCount uint64
	// Index of the first loaded SST file.
	loadIdx       uint64
	memSST        []SSTMap
	loadThreshold uint64
}

// From the directory, we will load all the SST files into memory.
// We will also load the info file into memory.
// The info file will contain the number of SST files and their names.
// The info file will be used to keep track of the number of SST files and their names.

// This function will create the SST manager.
// It will create the info file if it doesn't exist.
// It will also create the directory where the SST files will be stored.

func CheckAndClean() (uint64, error) {

	var numSSTFiles uint64
	// Create the directory if it doesn't exist
	if _, err := os.Stat(directory); os.IsNotExist(err) {
		numSSTFiles = 0
		if err := os.Mkdir(directory, 0755); err != nil {
			return 0, err
		}
	} else {
		files, err := os.ReadDir(directory)
		if err != nil {
			return 0, err
		}

		numSSTFiles = uint64(len(files))

		for _, file := range files {
			//fmt.Println(filepath.Ext(file.Name()))
			if filepath.Ext(file.Name()) == ext {
				//fmt.Println("probleeeme")
				if err := os.Remove(directory + "/" + file.Name()); err != nil {
					return 0, err
				}
				print("removed")
				numSSTFiles--
			}
		}
	}
	//print(uint64(numSSTFiles))
	return uint64(numSSTFiles), nil
}

func NewSSTManager(load uint64, treshold uint64) (*mySSTManager, error) {

	numSSTFiles, err := CheckAndClean()
	if err != nil {
		return nil, err
	}

	// 100 SSTMap.
	idx := math.Max(0, float64(numSSTFiles)-float64(defLoad))
	//fmt.Printf("idx: %d\n", int(idx))
	ss := make([]SSTMap, defLoad)

	// DEfault for loadCount = 100
	return &mySSTManager{
		sstCount:      numSSTFiles,
		loadCount:     uint64(load),
		loadIdx:       uint64(idx),
		memSST:        ss,
		loadThreshold: treshold}, nil
}

// This function will load the SST files into memory. From idxLoad to sstCount.
func (m *mySSTManager) LoadALL() error {

	// New wait group
	var wg sync.WaitGroup
	for i := m.loadIdx; i < m.sstCount; i++ {
		//fmt.Println(i - m.loadIdx)

		// Increment the wait group counter
		wg.Add(1)
		// To be removed.
		go func(i uint64, wg *sync.WaitGroup) error {
			defer wg.Done()
			fileName := fmt.Sprintf("%s/SST%d.sst", directory, i)
			file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0644)
			if err != nil {
				return err
			}
			defer file.Close()

			// Load the SST file into memory.
			m.memSST[i-m.loadIdx].mp = make(map[string]Tuple)
			if err := m.memSST[i-m.loadIdx].LoadToMem(file); err != nil {
				return err
			}

			// Decrement the wait group counter
			return nil
		}(i, &wg)
	}

	// Wait for all the goroutines to finish
	wg.Wait()

	fmt.Println("All SST files loaded into memory")
	fmt.Printf("There are %d SST files in memory\n", len(m.memSST))
	// Print the size of each SST file in memory.
	/* for i := 0; i < len(m.memSST); i++ {
		fmt.Printf("SST%d size: %d\n", i, len(m.memSST[i].mp))
	} */
	return nil
}

func (m *mySSTManager) SearchInSST(key string, idx uint64) (string, error) {

	filename := fmt.Sprintf("%s/SST%d.sst", directory, idx)
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	defer file.Close()

	if err != nil {
		return "", err
	}

	// Read magic number
	var magic uint64
	if err := binary.Read(file, binary.LittleEndian, &magic); err != nil {
		return "", err
	}

	if magic != magicNumber {
		panic("Invalid SST file")
	}

	// Read system version
	var sysVersion uint64
	if err := binary.Read(file, binary.LittleEndian, &sysVersion); err != nil {
		return "", err
	}

	if sysVersion != sysVers {
		panic("Non Compatible SST file (Check the system version)")
	}

	// Read number of records
	var numRecords uint64
	if err := binary.Read(file, binary.LittleEndian, &numRecords); err != nil {
		return "", err
	}

	// Read records
	for i := uint64(0); i < numRecords; i++ {

		var length int64
		if err := binary.Read(file, binary.BigEndian, &length); err != nil {
			return "", err
		}

		// Read the record data
		data := make([]byte, length)
		_, err := io.ReadFull(file, data)
		if err != nil {
			return "", err
		}

		// Unmarshal the JSON data into a WALRecord
		var record FileRecord
		err = json.Unmarshal(data, &record)
		if err != nil {
			return "", err
		}

		// check if the keys match
		if record.Key == key {
			if record.Operation == "del" {
				return "", errors.New("Key Deleted")
			}
			return record.Value, nil
		}
		// We can stop searching if the key is greater than the current key.
		if record.Key > key {
			return "", errors.New("Key Not Found")
		}

	}
	return "", errors.New("Key Not Found")
}

func (m *mySSTManager) SearchInDisk(key string) (string, error) {

	if m.loadIdx == 0 {
		return "", errors.New("Key Not Found")
	}
	for i := m.loadIdx - 1; ; i-- {

		if i > uint64(10000000) {
			panic("Strange Error Occured")
		}

		fmt.Println(i, m.loadIdx)
		val, err := m.SearchInSST(key, i)
		if err == nil {
			return val, nil
		}
		// if the key is deleted, we can stop searching.
		if err.Error() == "Key Deleted" {
			return "", errors.New("Key Deleted")
		}
		if i == 0 {
			return "", errors.New("Key Not Found")
		}
	}
	//return "", errors.New("Key Not Found")
}

func (m *mySSTManager) Search(key string) (string, error) {

	//fmt.Print("eee")
	// Search in the SST files in memory.
	for i := len(m.memSST) - 1; i >= 0; i-- {
		if val, b := m.memSST[i].mp[key]; b {
			if val.operation == "del" {
				return "", errors.New("Key Deleted")
			}
			return val.value, nil
		}
	}

	//fmt.Print("eee")

	// Search in the SST files on disk.
	res, err := m.SearchInDisk(key)
	//fmt.Print("eee")
	if err != nil {
		return "", err
	}
	return res, nil
}

// This function will merge two SST files into one, based on their indices.
func (m *mySSTManager) MergeSST(i, j uint64) error {

	// Open the two SST files.
	file1, err := os.OpenFile(fmt.Sprintf("%s/SST%d.sst", directory, i), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file1.Close()

	file2, err := os.OpenFile(fmt.Sprintf("%s/SST%d.sst", directory, j), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file2.Close()

	// Create a new SST file with a temporary extension.
	file3, err := os.OpenFile(fmt.Sprintf("%s/SST%d%s", directory, i/2, ext), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file3.Close()

	// Read magic number
	var magic1 uint64
	if err := binary.Read(file1, binary.LittleEndian, &magic1); err != nil {
		return err
	}

	var magic2 uint64
	if err := binary.Read(file2, binary.LittleEndian, &magic2); err != nil {
		return err
	}

	if magic1 != magic2 || magic1 != magicNumber {
		panic("Invalid SST files")
	}

	// Read system version
	var sysVersion1 uint64
	if err := binary.Read(file1, binary.LittleEndian, &sysVersion1); err != nil {
		return err
	}
	var sysVersion2 uint64
	if err := binary.Read(file2, binary.LittleEndian, &sysVersion2); err != nil {
		return err
	}

	if sysVersion1 != sysVersion2 || sysVersion1 != sysVers {
		panic("Non Compatible SST files (Check the system version)")
	}

	// Read number of records
	var numRecords1 uint64
	if err := binary.Read(file1, binary.LittleEndian, &numRecords1); err != nil {
		return err
	}
	var numRecords2 uint64
	if err := binary.Read(file2, binary.LittleEndian, &numRecords2); err != nil {
		return err
	}

	var totalRecords uint64 = numRecords1 + numRecords2

	// Write magic number:
	if err := binary.Write(file3, binary.LittleEndian, magicNumber); err != nil {
		return err
	}

	// Write system version:
	if err := binary.Write(file3, binary.LittleEndian, sysVersion1); err != nil {
		return err
	}

	// Write the number of records:

	if err := binary.Write(file3, binary.LittleEndian, totalRecords); err != nil {
		return err
	}

	// Set two pointers to the beginning of the two files.
	// We will read the records from the two files and write them to the third file in the same order.

	f1 := uint64(0)
	f2 := uint64(0)
	// Read records

	// Read the first record from the first file.
	var length1 int64
	if err := binary.Read(file1, binary.BigEndian, &length1); err != nil {
		return err
	}

	// Read the record data
	data1 := make([]byte, length1)
	_, err = io.ReadFull(file1, data1)
	if err != nil {
		return err
	}

	// Unmarshal the JSON data into a WALRecord
	var record1 FileRecord
	err = json.Unmarshal(data1, &record1)
	if err != nil {
		return err
	}

	// Read the first record from the second file.
	var length2 int64
	if err := binary.Read(file2, binary.BigEndian, &length2); err != nil {
		return err
	}

	// Read the record data
	data2 := make([]byte, length2)
	_, err = io.ReadFull(file2, data2)
	if err != nil {
		return err
	}

	// Unmarshal the JSON data into a WALRecord
	var record2 FileRecord
	err = json.Unmarshal(data2, &record2)
	if err != nil {
		return err
	}

	for f1 < numRecords1 && f2 < numRecords2 {

		if record1.Key < record2.Key {
			// Write the length of the record
			if err := binary.Write(file3, binary.BigEndian, int64(length1)); err != nil {
				return err
			}

			// Write the record data
			_, err = file3.Write(data1)
			if err != nil {
				return err
			}

			// Read the next record from the first file.
			if err := binary.Read(file1, binary.BigEndian, &length1); err != nil {
				if err.Error() == "EOF" {
					f1++
					continue
				}
			}

			// Read the record data
			data1 = make([]byte, length1)
			_, err = io.ReadFull(file1, data1)
			if err != nil {
				return err
			}

			// Unmarshal the JSON data into a WALRecord
			err = json.Unmarshal(data1, &record1)
			if err != nil {
				return err
			}

			f1++
		} else {

			// Write the length of the record
			if err := binary.Write(file3, binary.BigEndian, int64(length2)); err != nil {
				return err
			}

			// Write the record data
			_, err = file3.Write(data2)
			if err != nil {
				return err
			}

			// Read the next record from the second file.
			if err := binary.Read(file2, binary.BigEndian, &length2); err != nil {
				if err.Error() == "EOF" {
					f2++
					continue
				}
			}

			// Read the record data
			data2 = make([]byte, length2)
			_, err = io.ReadFull(file2, data2)
			if err != nil {
				return err
			}

			// Unmarshal the JSON data into a WALRecord
			err = json.Unmarshal(data2, &record2)
			if err != nil {
				return err
			}

			f2++
		}
	}

	//fmt.Println("mmmmmmmmmmmmmmmmmmmmmmmmmmm")

	// Write the remaining records from the first file.
	for f1 < numRecords1 {
		// Write the length of the record
		if err := binary.Write(file3, binary.BigEndian, int64(length1)); err != nil {
			return err
		}

		// Write the record data
		_, err = file3.Write(data1)
		if err != nil {
			return err
		}

		// Read the next record from the first file.
		if err := binary.Read(file1, binary.BigEndian, &length1); err != nil {
			if err.Error() == "EOF" {
				break
			}
		}

		// Read the record data
		data1 = make([]byte, length1)
		_, err = io.ReadFull(file1, data1)
		if err != nil {
			return err
		}

	}

	// Write the remaining records from the second file.
	for f2 < numRecords2 {
		// Write the length of the record
		if err := binary.Write(file3, binary.BigEndian, int64(length2)); err != nil {
			return err
		}

		// Write the record data
		_, err = file3.Write(data2)
		if err != nil {
			return err
		}

		// Read the next record from the second file.
		if err := binary.Read(file2, binary.BigEndian, &length2); err != nil {
			if err.Error() == "EOF" {
				break
			}
		}

		// Read the record data
		data2 = make([]byte, length2)
		_, err = io.ReadFull(file2, data2)
		if err != nil {
			return err
		}

	}

	//fmt.Println("mmmmmmmmmmmmmmmmmmmmmmmmmmm")
	// Close the tree files.
	file1.Close()
	file2.Close()
	file3.Close()

	// Delete the two files.
	if err := os.Remove(fmt.Sprintf("%s/SST%d.sst", directory, j)); err != nil {
		return err
	}
	
	if err := os.Remove(fmt.Sprintf("%s/SST%d.sst", directory, i)); err != nil {
		return err
	}

	// Rename the third file.
	i = i / 2
	oldName := fmt.Sprintf("%s/SST%d%s", directory, i, ext)
	newfileName := fmt.Sprintf("%s/SST%d.sst", directory, i)
	if err := os.Rename(oldName, newfileName); err != nil {
		return err
	}

	return nil
}

// WriteToSST writes the records to the SST file.
// All the records are stored in a treemap.
// To retrieve them in sorted order we will use the Iterator() method of the treemap.
