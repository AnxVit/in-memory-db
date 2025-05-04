package sstable

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
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

type SSTable struct {
	sync.Mutex

	opt Options

	File *os.File
	data []byte

	// ref atomic.Int32 // For file garbage collection

	// index
	blockIndex        []BlockIndex
	blockCache        *lru.Cache[int64, []Entry]
	smallest, biggest []byte

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

func CreateTable(id uint64, entries []Entry, opt Options) (*SSTable, error) {
	if len(entries) == 0 {
		return nil, errors.New("empty entries")
	}

	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].Key, entries[j].Key) < 0
	})

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
		CreatedAt:  createdTime,
		smallest:   entries[0].Key,
		biggest:    entries[len(entries)-1].Key,
		filter:     adaptivefilter.NewScalableCuckooFilter(),
		blockIndex: make([]BlockIndex, 0),
		blockCache: cache,
	}

	sst.writeData(entries)

	fileInfo, _ := sst.File.Stat()
	sst.tableSize = fileInfo.Size()

	return sst, nil
}

var bufPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 0, 4096))
	},
}

func (sst *SSTable) writeData(entries []Entry) {
	data := make([]byte, 0)

	buf := bufPool.Get().(*bytes.Buffer)
	defer func() {
		buf.Reset()
		bufPool.Put(buf)
	}()

	var (
		sizeBlock uint64
		firstKey  []byte
		offset    int64
	)

	entrySet := make([]Entry, 0)

	for _, entry := range entries {
		buf.Reset()
		if firstKey == nil {
			firstKey = entry.Key
		}

		// write key
		binary.Write(buf, binary.LittleEndian, uint32(len(entry.Key)))
		buf.Write(entry.Key)

		// write value
		binary.Write(buf, binary.LittleEndian, uint32(len(entry.Value)))
		buf.Write(entry.Value)

		sizeBlock += 8 + uint64(len(entry.Value)) + uint64(len(entry.Key))
		sst.File.Write(buf.Bytes())
		data = append(data, buf.Bytes()...)
		entrySet = append(entrySet, entry)

		if sizeBlock > uint64(sst.opt.BlockSize) {
			currentEntrySet := make([]Entry, len(entrySet))
			copy(currentEntrySet, entrySet)

			go func(offset int64, entrySet []Entry) {
				sst.blockCache.Add(offset, entrySet)
			}(offset, currentEntrySet)

			offset, _ = sst.File.Seek(0, io.SeekCurrent)
			sst.blockIndex = append(sst.blockIndex, newBlock(firstKey, offset, int64(sizeBlock)))

			sizeBlock = 0
			firstKey = nil
			entrySet = entrySet[:0]
		}

		go func(key []byte) {
			sst.filter.Insert(key)
		}(entry.Key)
	}

	if sizeBlock > 0 {
		sst.blockIndex = append(sst.blockIndex, newBlock(firstKey, offset, int64(sizeBlock)))
	}

	sst.data = data
}

func newBlock(key []byte, offset, size int64) BlockIndex {
	return BlockIndex{
		key:    key,
		offset: offset,
		size:   size,
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
	for block := range out {
		sst.blockIndex = append(sst.blockIndex, block)
		sst.blockCache.Add(block.offset, block.data)
	}
	if len(sst.blockIndex) != 0 {
		firstOffset := sst.blockIndex[0].offset
		entries, _ := sst.blockCache.Get(firstOffset)
		sst.smallest = entries[0].Key

		lastOffset := sst.blockIndex[len(sst.blockIndex)-1].offset
		entries, _ = sst.blockCache.Get(lastOffset)
		sst.biggest = entries[len(entries)-1].Key
	}
}

func (sst *SSTable) Close() error {
	sst.wg.Wait()
	fileName := sst.File.Name()

	if err := sst.File.Close(); err != nil {
		return err
	}

	if err := os.Remove(fileName); err != nil {
		return err
	}
	return nil
}
