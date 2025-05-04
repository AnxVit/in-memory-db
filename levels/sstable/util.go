package sstable

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
)

func ParseFileID(name string) (uint64, bool) {
	name = filepath.Base(name)
	if !strings.HasSuffix(name, fileSuffix) {
		return 0, false
	}

	name = strings.TrimSuffix(name, fileSuffix)
	id, err := strconv.Atoi(name)
	if err != nil {
		return 0, false
	}
	return uint64(id), true
}

func IDToFilename(id uint64) string {
	return fmt.Sprintf("%06d", id) + fileSuffix
}

// func decodeKV(data []byte) (key, value []byte, ttl *time.Time, err error) {
// 	buf := bytes.NewReader(data)

// 	// Decode the length of the Key and the Key itself
// 	var keyLen int32
// 	if err := binary.Read(buf, binary.LittleEndian, &keyLen); err != nil {
// 		return nil, nil, nil, err
// 	}
// 	if keyLen < 0 || keyLen > int32(len(data)) {
// 		return nil, nil, nil, fmt.Errorf("invalid key length: %d", keyLen)
// 	}

// 	key = make([]byte, keyLen)
// 	if err := binary.Read(buf, binary.LittleEndian, &key); err != nil {
// 		return nil, nil, nil, err
// 	}

// 	// Decode the length of the Value and the Value itself
// 	var valueLen int32
// 	if err := binary.Read(buf, binary.LittleEndian, &valueLen); err != nil {
// 		return nil, nil, nil, err
// 	}
// 	if valueLen < 0 || valueLen > int32(len(data)) {
// 		return nil, nil, nil, fmt.Errorf("invalid value length: %d", valueLen)
// 	}
// 	value = make([]byte, valueLen)
// 	if err := binary.Read(buf, binary.LittleEndian, &value); err != nil {
// 		return nil, nil, nil, err
// 	}

// 	return key, value, ttl, nil
// }
