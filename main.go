package main

import (
	"fmt"
	"net/http"
)

type Error int

func (e Error) Error() string {
	switch e {
	case KeyNotFound:
		return "Key Not Found"
	case KeyDeleted:
		return "Key Deleted"
	}
	return "Unknown Error"
}

const (
	KeyNotFound Error = iota
	KeyDeleted
)

type HTTP_API_DB struct {
	db   *MyKvStore
	port string
}

// handleGet handles GET requests
func (api *HTTP_API_DB) HandleGet(w http.ResponseWriter, r *http.Request) {
	//fmt.Println("Handling get request")
	key := r.URL.Query().Get("key")
	val, err := api.db.Get(key)

	if err == KeyNotFound || err == KeyDeleted {
		fmt.Fprint(w, "Key not found")
		return
	} else if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Fprint(w, string(val))
}

// handleSet handles POST requests
func (api *HTTP_API_DB) HandleSet(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Handling set request")

	key := r.URL.Query().Get("key")
	value := r.URL.Query().Get("value")

	//fmt.Print(key)
	//fmt.Print(value)

	if key == "" {
		http.Error(w, "Missing 'key' parameter", http.StatusBadRequest)
		return
	}

	if value == "" {
		http.Error(w, "Missing 'value' parameter", http.StatusBadRequest)
		return
	}
	if err := api.db.Set(key, value); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
	fmt.Printf("Key '%s' set to value '%s'", key, value)
}

// handleDel handles DELETE requests
func (api *HTTP_API_DB) HandleDel(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Handling del request")
	key := r.URL.Query().Get("key")

	if key == "" {
		http.Error(w, "Missing 'key' parameter", http.StatusBadRequest)
		return
	}

	val, err := api.db.Del(key)

	if err != nil {
		fmt.Fprint(w, err.Error())
		return
	}
	fmt.Fprint(w, string(val))
}

func (api *HTTP_API_DB) HandleStop(w http.ResponseWriter, r *http.Request) {

	err := api.db.Stop()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Fprint(w, "Server stopped")

}

func (api *HTTP_API_DB) Start() {
	http.Handle("/", http.FileServer(http.Dir(".")))
	http.HandleFunc("/get", api.HandleGet)
	http.HandleFunc("/set", api.HandleSet)
	http.HandleFunc("/del", api.HandleDel)
	http.HandleFunc("/stop", api.HandleStop)
	fmt.Print("Starting server on :" + api.port + "...\n")

	if err := http.ListenAndServe(":"+api.port, nil); err != nil {
		fmt.Println("Error:", err)
	}
}

func main() {
	db, err := NewKeyValueStore()
	db.Start()
	if err != nil {
		panic(err.Error())
	}
	defer db.Stop()
	api := &HTTP_API_DB{
		db,
		"8080",
	}

	api.Start()
}
