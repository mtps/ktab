package http

import (
	"encoding/base64"
	"encoding/json"
	"github.com/mtps/ktab/pkg/db"
	"github.com/mtps/ktab/pkg/types"
	"log"
	"net/http"
	"strconv"
)

func QueryTopicKey(rockses map[string]db.DB) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, req *http.Request) {
		qq := req.URL.Query()
		topic := req.PathValue("topic")
		key := qq.Get("key")
		keyBytes, err := base64.URLEncoding.DecodeString(key)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("invalid key"))
			return
		}
		// Fetch the db for the requested topic
		db, ok := rockses[topic]
		if !ok {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("topic not found"))
			return
		}
		// Pull the key from the store.
		bz, err := db.Get(keyBytes)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write(bz)
		w.Write([]byte{'\n'})
		return
	}
}

func QueryTopicOffsetPartition(rockses map[string]db.DB) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, req *http.Request) {
		// Pull url and query params.
		qq := req.URL.Query()
		topic := req.PathValue("topic")
		offsetP := qq.Get("offset")
		partitionP := qq.Get("partition")
		// Parse the partition and offset.
		off, err := strconv.ParseInt(offsetP, 10, 64)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)

		}
		part, err := strconv.ParseInt(partitionP, 10, 32)
		log.Printf("Scanning [%s] for part:%d off:%d\n", topic, part, off)
		// Fetch the db for the requested topic
		db, ok := rockses[topic]
		if !ok {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("topic not found"))
			return
		}
		// Scan for a matching item.
		var bz []byte
		db.NewIterator(func(k, v []byte) error {
			m := types.Message{}
			err := json.Unmarshal(v, &m)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte(err.Error()))
				return nil
			}
			// Found it!
			if m.Partition == int32(part) && m.Offset == off {
				bz = append(v[:0:0], v...)
				return nil
			}
			return nil
		})
		// Not found
		if bz == nil {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte("partition offset not found"))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write(bz)
		w.Write([]byte{'\n'})
	}
}
