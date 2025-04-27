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
	data   []Entry
}

func (sst *SSTable) rebuildBlockIndexFromFile() <-chan BlockIndex {
	out := make(chan BlockIndex)
	go func() {
		defer close(out)

		var currentOffset int64 = 0
		for {
			entries, nextOffset, err := readEntries(sst.File, currentOffset, int(sst.opt.BlockSize))
			if err != nil {
				// TODO: error
				return
			}
			if len(entries) == 0 {
				break
			}

			out <- BlockIndex{
				key:    entries[0].Key,
				offset: currentOffset,
				size:   nextOffset - currentOffset,
				data:   entries,
			}

			currentOffset = nextOffset
		}
	}()

	return out
}

func readEntries(file *os.File, offset int64, blockSize int) ([]Entry, int64, error) {
	info, _ := file.Stat()
	size := info.Size()

	currentOffset, _ := file.Seek(offset, io.SeekStart)

	var err error

	entries := make([]Entry, 0)

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

		entries = append(entries, Entry{
			Key:   key,
			Value: value,
		})

		currentOffset, _ = file.Seek(0, io.SeekCurrent)
	}

	currentOffset, _ = file.Seek(0, io.SeekCurrent)

	if err != io.EOF {
		return nil, 0, err
	}

	if len(entries) == 0 {
		return nil, 0, nil
	}
	return entries, currentOffset, nil
}
