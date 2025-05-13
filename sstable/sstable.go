package sstable

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/pkg/errors"

	adaptivefilter "GoKeyValueWarehouse/cuckooFilter/adaptiveFilter"
)

const fileSuffix = ".sst"

type Entry struct {
	Key   []byte
	Value []byte
}

type MIterator interface {
	Next() bool                                // Move to the next node
	Prev() bool                                // Move to the previous node
	HasNext() bool                             // Check if there is a next node
	HasPrev() bool                             // Check if there is a previous node
	Current() ([]byte, []byte, *time.Duration) // Get the current key and value
}

type SSTable struct {
	sync.Mutex

	opt Options

	File *os.File
	data []byte

	ref atomic.Int32

	// index
	blockIndex        []BlockIndex
	smallest, biggest []byte

	muCache    *sync.RWMutex
	blockCache *lru.Cache[int64, []Entry]

	// sstable inforamtion
	id        uint64
	CreatedAt time.Time
	tableSize int64

	// filter
	filter *adaptivefilter.ScalableCuckooFilter

	wg *sync.WaitGroup
}

func (sst *SSTable) GetID() uint64 {
	return sst.id
}

func (sst *SSTable) GetSmallest() []byte {
	return sst.smallest
}

func (sst *SSTable) GetBiggest() []byte {
	return sst.biggest
}

func (sst *SSTable) GetSize() int64 {
	return sst.tableSize
}

func TableName(id uint64, dir string) string {
	return filepath.Join(dir, IDToFilename(id))
}

func CreateTable(id uint64, iter MIterator, opt Options) (*SSTable, error) {
	if !iter.HasNext() {
		return nil, errors.New("empty entries")
	}

	createdTime := time.Now()
	file, err := os.Create("./" + IDToFilename(id)) // TODO: dir
	if err != nil {
		return nil, err
	}

	cache, err := lru.New[int64, []Entry](opt.CacheSize)
	if err != nil {
		return nil, errors.WithMessage(err, "could not create a cache")
	}

	sst := &SSTable{
		opt:        opt,
		File:       file,
		id:         id,
		ref:        atomic.Int32{},
		CreatedAt:  createdTime,
		blockCache: cache,
		filter:     adaptivefilter.NewScalableCuckooFilter(),
		blockIndex: make([]BlockIndex, 0),
	}

	sst.writeData(iter)

	fileInfo, _ := sst.File.Stat()
	sst.tableSize = fileInfo.Size()

	return sst, nil
}

var bufPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 0, 4096))
	},
}

func (sst *SSTable) writeData(iter MIterator) {
	data := make([]byte, 0)

	buf := bufPool.Get().(*bytes.Buffer)
	defer func() {
		buf.Reset()
		bufPool.Put(buf)
	}()

	var (
		sizeBlock  uint64
		firstKey   []byte
		offset     int64
		isSmallest bool
		smallest   []byte
		key        []byte
		value      []byte
	)

	entrySet := make([]Entry, 0)

	for ; iter.HasNext(); iter.Next() {
		buf.Reset()
		key, value, _ = iter.Current()
		if firstKey == nil {
			firstKey = key
		}
		if !isSmallest {
			smallest = key
			isSmallest = true
		}

		// write key
		binary.Write(buf, binary.LittleEndian, uint32(len(key)))
		buf.Write(key)

		// write value
		binary.Write(buf, binary.LittleEndian, uint32(len(value)))
		buf.Write(value)

		sizeBlock += 8 + uint64(len(value)) + uint64(len(key))
		sst.File.Write(buf.Bytes())
		data = append(data, buf.Bytes()...)
		entrySet = append(entrySet, Entry{
			Key:   key,
			Value: value,
		})

		if sizeBlock > uint64(sst.opt.BlockSize) {
			currentEntrySet := make([]Entry, len(entrySet))
			copy(currentEntrySet, entrySet)

			offset, _ = sst.File.Seek(0, io.SeekCurrent)
			sst.blockIndex = append(sst.blockIndex, newBlock(firstKey, offset, int64(sizeBlock)))

			sizeBlock = 0
			firstKey = nil
			entrySet = entrySet[:0]
		}

		go func(key []byte) {
			sst.filter.Insert(key)
		}(key)
	}

	sst.smallest = smallest
	sst.biggest = key

	if sizeBlock > 0 {
		sst.blockIndex = append(sst.blockIndex, newBlock(firstKey, offset, int64(sizeBlock)))
	}

	sst.data = data
}

func newBlock(key []byte, offset, size int64) BlockIndex {
	return BlockIndex{
		smallest: key,
		offset:   offset,
		size:     size,
	}
}

func OpenTable(file *os.File, opts Options) (*SSTable, error) {
	fileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	filename := fileInfo.Name()
	id, ok := ParseFileID(filename)
	if !ok {
		file.Close()
		return nil, errors.New("Invalid filename: " + filename)
	}

	t := &SSTable{
		File:      file,
		id:        id,
		opt:       opts,
		tableSize: fileInfo.Size(),
		CreatedAt: fileInfo.ModTime(),
	}

	t.wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		err := t.pullDataFromFile()
		if err != nil {
			// TODO: return errors.New("couldn't get dat afrom file")
			return
		}
	}(t.wg)

	t.wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer func() {
			t.File.Seek(0, io.SeekStart)
			wg.Done()
		}()

		out := t.rebuildBlockIndexFromFile()
		t.buildIndex(out)
	}(t.wg)

	//TODO:
	// t.ref.Store(1)

	return t, nil
}

func (sst *SSTable) pullDataFromFile() error {
	result := make([]byte, 0)
	_, err := sst.File.Read(result)
	if err != nil {
		return errors.New("couldn't read from file")
	}

	sst.data = result
	return nil
}

func (sst *SSTable) buildIndex(out <-chan BlockIndex) {
	var (
		isFirst bool
		biggest []byte
	)
	for block := range out {
		if !isFirst {
			isFirst = true
			sst.smallest = block.smallest
		}
		sst.blockIndex = append(sst.blockIndex, block)
		biggest = block.biggest
	}
	sst.biggest = biggest
}

func (sst *SSTable) Close() error {
	sst.wg.Wait()

	sst.File.Sync()
	if err := sst.File.Close(); err != nil {
		return err
	}

	return nil
}

func (sst *SSTable) getEntriesFromBlock(block BlockIndex) ([]Entry, error) {
	info, _ := sst.File.Stat()
	size := info.Size()

	currentOffset, _ := sst.File.Seek(block.offset, io.SeekStart)

	entries := make([]Entry, 0)

	var (
		err error
	)
	for (currentOffset - block.offset) < block.size {
		var keyLen int32
		if err = binary.Read(sst.File, binary.LittleEndian, &keyLen); err != nil {
			break
		}
		if keyLen < 0 || keyLen > int32(size) {
			break
		}
		key := make([]byte, keyLen)
		if err = binary.Read(sst.File, binary.LittleEndian, &key); err != nil {
			break
		}

		var valueLen int32
		if err = binary.Read(sst.File, binary.LittleEndian, &valueLen); err != nil {
			break
		}
		if valueLen < 0 || valueLen > int32(size) {
			break
		}
		value := make([]byte, valueLen)
		if err = binary.Read(sst.File, binary.LittleEndian, &value); err != nil {
			break
		}

		entries = append(entries, Entry{
			Key:   key,
			Value: value,
		})

		currentOffset, _ = sst.File.Seek(0, io.SeekCurrent)
	}

	if err != io.EOF {
		return nil, err
	}

	return entries, nil
}

func (sst *SSTable) searchInBlock(targetKey []byte, block BlockIndex) (*Entry, error) {
	info, _ := sst.File.Stat()
	size := info.Size()

	currentOffset, _ := sst.File.Seek(block.offset, io.SeekStart)

	var err error

	for (currentOffset - block.offset) < block.size {
		var keyLen int32
		if err = binary.Read(sst.File, binary.LittleEndian, &keyLen); err != nil {
			break
		}
		if keyLen < 0 || keyLen > int32(size) {
			break
		}
		key := make([]byte, keyLen)
		if err = binary.Read(sst.File, binary.LittleEndian, &key); err != nil {
			break
		}

		var valueLen int32
		if err = binary.Read(sst.File, binary.LittleEndian, &valueLen); err != nil {
			break
		}
		if valueLen < 0 || valueLen > int32(size) {
			break
		}
		value := make([]byte, valueLen)
		if err = binary.Read(sst.File, binary.LittleEndian, &value); err != nil {
			break
		}

		if bytes.Equal(targetKey, key) {
			return &Entry{
				Key:   key,
				Value: value,
			}, nil
		}

		currentOffset, _ = sst.File.Seek(0, io.SeekCurrent)
	}

	return nil, errors.New("not found key in block")
}
