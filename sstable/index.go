package sstable

import (
	"encoding/binary"
	"io"
	"os"
)

type BlockIndex struct {
	smallest []byte
	biggest  []byte
	offset   int64
	size     int64
}

func (sst *SSTable) rebuildBlockIndexFromFile() <-chan BlockIndex {
	out := make(chan BlockIndex)
	go func() {
		defer close(out)

		var currentOffset int64 = 0
		for {
			smallest, biggest, nextOffset, err := readBlock(sst.File, currentOffset, int(sst.opt.BlockSize))
			if err != nil {
				// TODO: error
				return
			}

			out <- BlockIndex{
				smallest: smallest,
				biggest:  biggest,
				offset:   currentOffset,
				size:     nextOffset - currentOffset,
			}

			currentOffset = nextOffset
		}
	}()

	return out
}

func readBlock(file *os.File, offset int64, blockSize int) ([]byte, []byte, int64, error) {
	info, _ := file.Stat()
	size := info.Size()

	currentOffset, _ := file.Seek(offset, io.SeekStart)

	var (
		err      error
		firstKey []byte
		lastKey  []byte
	)
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
		lastKey = key
		if len(firstKey) == 0 {
			firstKey = key
		}

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
		return nil, nil, 0, err
	}

	return firstKey, lastKey, currentOffset, nil
}
