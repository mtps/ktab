package http

import (
	"encoding/json"
	"github.com/mtps/ktab/pkg/db"
	"net/http"
	"sort"
)

func ListTopic(rockses map[string]db.DB) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, req *http.Request) {
		var topics []string
		for topic, _ := range rockses {
			topics = append(topics, topic)
		}
		sort.Strings(topics)
		bz, err := json.Marshal(topics)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(err.Error()))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write(bz)
		w.Write([]byte{'\n'})
	}
}
