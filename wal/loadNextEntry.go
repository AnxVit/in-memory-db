package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/goccy/go-json"
)

func loadNextJSONEntry(data []byte) (n int, err error) {
	idx := bytes.IndexByte(data, '\n')
	if idx == -1 {
		return 0, ErrCorrupt
	}
	line := data[:idx]
	_, err = findValue(line, "data")
	if err != nil {
		return 0, ErrCorrupt
	}
	return idx + 1, nil
}

func loadNextBinaryEntry(data []byte) (n int, err error) {
	size, n := binary.Uvarint(data)
	if n <= 0 {
		return 0, ErrCorrupt
	}
	if uint64(len(data)-n) < size {
		return 0, ErrCorrupt
	}
	return n + int(size), nil
}

func findValue(data []byte, value string) (string, error) {
	v := make(map[string]interface{})
	if err := json.Unmarshal(data, v); err != nil {
		return "", err
	}
	return fmt.Sprintf("%v", v[value]), nil
}
