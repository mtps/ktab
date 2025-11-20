package http

import (
	"github.com/mtps/ktab/pkg/db"
	"net/http"
)

func QueryTopic(rockses map[string]db.DB) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, req *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		topic := req.PathValue("topic")
		// Fetch the db for the requested topic
		db, ok := rockses[topic]
		if !ok {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("topic not found"))
			return
		}
		// Chunking params.
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Transfer-Encoding", "chunked")
		w.Header().Set("X-Content-Type-Options", "nosniff")

		// Start the json array.
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("["))
		flusher.Flush()

		rCh := make(chan []byte)
		go func() {
			db.NewIterator(func(k, v []byte) error {
				rCh <- v
				return nil
			})
			close(rCh)
		}()

		first := true

	L:
		for {
			select {
			case <-req.Context().Done():
				break L

			case item, aok := <-rCh:
				if !aok || item == nil {
					break L
				}
				if !first {
					w.Write([]byte(",\n"))
				}
				if first {
					first = false
				}
				w.Write(item)
				flusher.Flush()
			}
		}
		w.Write([]byte("]"))
		w.Write([]byte{'\n'})
		w.Header().Set("Content-Length", "0")
		flusher.Flush()
	}
}
