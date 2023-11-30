package main

import (
	"errors"
	"io"
	"sync"

	"github.com/igrmk/treemap/v2"
)

// Tuple : <string, string>
type Tuple struct {
	operation string
	value     string
}

type PersistentCacheMemory interface {
	// To load KV pairs from WAL to memory(if any).
	Load() error
	GetM(string) (string, error)
	DelM(string) (string, error)
	SetM(string, string) error
}

// Use as default name : mydb.wal
type PersMem struct {
	mu    sync.RWMutex
	store *treemap.TreeMap[string, Tuple]
	wal   *WALFile
}

func NewPersMem() (*PersMem, error) {
	nw, err := NewWALFile(WalName)
	nw.SeekEnd()
	nw1 := treemap.New[string, Tuple]()
	if err != nil {
		return nil, err
	}

	inst := PersMem{wal: nw, store: nw1}
	return &inst, nil
}

// Checks if WAL is empty, if not loads all records to main memory.
// Records are loaded sequentially, therefore there is no risk.
func (s *PersMem) Load() error {
	s.wal.SeekStart()

	tp := Tuple{}
	for {
		r, err := s.wal.ReadRecord()

		// We have reached the end of the Wal File and all values have been loaded.
		if err == io.EOF {
			return nil
		}
		// Some error occured.
		if err != nil {
			return err
		}

		// use a switch instead

		switch r.Operation {
		case "set":
			tp.operation = "set"
			tp.value = r.Value

			// Put a copy of the record in the main memory.
			s.store.Set(r.Key, tp)

		case "del":
			tp.operation = "del"
			tp.value = ""

			// Put a copy of the record in the main memory.
			s.store.Set(r.Key, tp)
		}

		/*
			if r.Operation == "set" {
				tp.operation = "set"
				tp.value = r.Value

				s.store.Set(r.Key, tp)
				//fmt.Printf("restored")
			}
			if r.Operation == "del" {
				tp.operation = "del"
				tp.value = ""
				s.store.Set(r.Key, tp)
				//fmt.Printf("restored")
			}
		*/
	}
}

func (s *PersMem) Close() error {

	// Add the functionality of reducing the WAL size.
	err := s.wal.Close()
	if err != nil {
		return err
	}
	return nil
}

func (s *PersMem) GetM(key string) (Tuple, error) {
	// In this Phase we only need to retrieve the key if it could be found in the main memory.
	v, b := s.store.Get(key)
	if b == false {
		return Tuple{"", ""}, errors.New("Key Not found In MemDB")
	}
	return v, nil
}

func (s *PersMem) SetM(key string, val string) error {
	//Create The record to be added to the WAL first
	s.wal.SeekEnd()
	r := FileRecord{
		Operation: "set",
		Key:       key,
		Value:     val,
	}
	err := s.wal.WriteRecord(r)
	if err != nil {
		return err
	}

	//Add the KV-pair to the main memory.
	tp := Tuple{"set", val}
	s.store.Set(key, tp)
	return nil
}

func (s *PersMem) DelM(key string) (string, error) {

	//Create The record to be added to the WAL first
	r := FileRecord{
		Operation: "del",
		Key:       key,
		Value:     "",
	}
	err := s.wal.WriteRecord(r)
	if err != nil {
		return "", err
	}

	// In this Phase we only need to retrieve the key if it could be found in the main memory.
	val, b := s.store.Get(key)
	if !b {
		return "", errors.New("Key Not Found")
	}
	if val.operation == "del" {
		return "", errors.New("Key Deleted")
	}

	//Add the KV-pair to the main memory.
	tp := Tuple{"del", ""}
	s.store.Set(key, tp)
	return val.value, nil
}

func (s *PersMem) DelM1(key string) error {
	//Create The record to be added to the WAL first
	r := FileRecord{
		Operation: "del",
		Key:       key,
		Value:     "",
	}
	err := s.wal.WriteRecord(r)
	if err != nil {
		return err
	}

	tp := Tuple{"del", ""}
	s.store.Set(key, tp)

	return nil
}

func (s *PersMem) Clear() error {
	if err := s.wal.ResetWal(); err != nil {
		return err
	}
	s.store.Clear()
	return nil
}

/* func main() {

	walf, err := NewWALFile("mydb.wal")
	if err != nil {
		fmt.Printf("%v", err)
	}

	walf.SeekEnd()


	/* fmt.Print("mahmoud")
	tr, err := NewTreeMapWal()
	fmt.Print("mahmoud")

	if err != nil {
		fmt.Printf("%v", err)
	}
	tr.Load()
	v := Tuple{}
	for i := 0; i < 100000; i++ {

		if i%2 == 0 {
			tr.DelM(fmt.Sprintf("%d", i))
		} else {
			tr.SetM(fmt.Sprintf("%d", i), fmt.Sprintf("%d", i))
		}

		// get values
		s := fmt.Sprintf("%d", i)
		v, err = tr.GetM(s)
		fmt.Print(v)

	}

	fmt.Print("Done")

	//tr.Set(1, "World")
	//tr.Set(0, "Hello")

	/* for it := tr.Iterator(); it.Valid(); it.Next() {
		fmt.Println(it.Key(), it.Value())
	}
	fmt.Println("Done")
} */

// Todo : In the load function, when we have del handle the deletion from the memory map.
