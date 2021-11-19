package db

import (
	"fmt"
	"strings"
)

type DBCreate = func(path string) (DB, error)

var registry = map[string]DBCreate{}

func Register(engine string, create DBCreate) {
	registry[engine] = create
}

func List() []string {
	var list []string
	for k, _ := range registry {
		list = append(list, k)
	}
	return list
}

func NewDB(engine string, path string) (DB, error) {
	creator, ok := registry[engine]
	if !ok {
		return nil, fmt.Errorf("%s engine not found: %s", engine, strings.Join(List(), ","))
	}
	return creator(path)
}