package sstable

import (
	"encoding/binary"
	"io"
	"os"
)

type BlockIndex struct {
	key    []byte
	offset int64
	size   int64
}

func (sst *SSTable) RebuildBlockIndexFromFile(blockSize int) ([]BlockIndex, error) {
	defer func() {
		sst.File.Seek(0, io.SeekStart)
	}()

	var index []BlockIndex
	var currentOffset int64 = 0

	for {
		key, nextOffset, err := readEntries(sst.File, currentOffset, blockSize)
		if err != nil {
			return nil, err
		}
		if key == nil {
			break
		}

		index = append(index, BlockIndex{
			key:    key,
			offset: currentOffset,
			size:   nextOffset - currentOffset,
		})

		currentOffset = nextOffset
	}

	return index, nil
}

func readEntries(file *os.File, offset int64, blockSize int) ([]byte, int64, error) {
	info, _ := file.Stat()
	size := info.Size()

	currentOffset, _ := file.Seek(offset, io.SeekStart)

	var err error

	keys := make([][]byte, 0)

	for (currentOffset - offset) < int64(blockSize) {
		var keyLen int32
		if err = binary.Read(file, binary.LittleEndian, &keyLen); err != nil {
			break
		}
		if keyLen < 0 || keyLen > int32(size) {
			break
		}

		key := make([]byte, keyLen)
		if err = binary.Read(file, binary.LittleEndian, &key); err != nil {
			break
		}
		keys = append(keys, key)

		var valueLen int32
		if err = binary.Read(file, binary.LittleEndian, &valueLen); err != nil {
			break
		}
		if valueLen < 0 || valueLen > int32(size) {
			break
		}
		value := make([]byte, valueLen)
		if err = binary.Read(file, binary.LittleEndian, &value); err != nil {
			break
		}

		currentOffset, _ = file.Seek(0, io.SeekCurrent)
	}

	currentOffset, _ = file.Seek(0, io.SeekCurrent)

	if err != io.EOF {
		return nil, 0, err
	}

	if len(keys) == 0 {
		return nil, 0, nil
	}
	return keys[0], currentOffset, nil
}
