package sstable

import (
	adaptivefilter "GoKeyValueWarehouse/cuckooFilter/adaptiveFilter"
	"bytes"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
)

type SSTable struct {
	sync.Mutex

	File *os.File
	data []byte

	tableSize int

	// _index *fb.TableIndex // Nil if encryption is enabled. Use fetchIndex to access.
	// _cheap *cheapIndex
	ref atomic.Int32 // For file garbage collection

	smallest, biggest []byte
	id                uint64

	CreatedAt  time.Time
	indexStart int
	indexLen   int

	filter *adaptivefilter.ScalableCuckooFilter
}

func CreateTable(fname string, builder *Builder) (*SSTable, error) {
	_ = builder.Done()
	mf, err := os.Create(fname)
	if err != nil {
		return nil, errors.WithMessagef(err, "while creating table: %s", fname)
	}
	return OpenTable(mf, *builder.opts)
}

func OpenTable(mf *os.File, opts Options) (*SSTable, error) {
	if opts.BlockSize == 0 && opts.Compression != options.None {
		return nil, errors.New("Block size cannot be zero")
	}
	fileInfo, err := mf.Stat()
	if err != nil {
		mf.Close()
		return nil, err
	}

	filename := fileInfo.Name()
	id, ok := ParseFileID(filename)
	if !ok {
		mf.Close()
		return nil, fmt.Errorf("Invalid filename: %s", filename)
	}
	t := &SSTable{
		File:       mf,
		id:         id,
		opt:        &opts,
		IsInmemory: false,
		tableSize:  int(fileInfo.Size()),
		CreatedAt:  fileInfo.ModTime(),
	}

	//TODO:
	// t.ref.Store(1)

	if err := t.initBiggestAndSmallest(); err != nil {
		return nil, errors.WithMessage(err, "failed to initialize table")
	}

	return t, nil
}

func (t *SSTable) initBiggestAndSmallest() error {
	var err error
	var ko *fb.BlockOffset
	if ko, err = t.initIndex(); err != nil {
		return y.Wrapf(err, "failed to read index.")
	}

	t.smallest = y.Copy(ko.KeyBytes())

	it2 := t.NewIterator(REVERSED | NOCACHE)
	defer it2.Close()
	it2.Rewind()
	if !it2.Valid() {
		return errors.WithMessagef(it2.err, "failed to initialize biggest for table %s", t.Filename())
	}
	t.biggest = y.Copy(it2.Key())
	return nil
}

func (sstable *SSTable) pullData() ([]byte, error) {
	result := make([]byte, 0)

	// get the page
	dataPHeader := make([]byte, PAGE_SIZE+HEADER_SIZE)

	if pageID == 0 {

		_, err := sstable.File.ReadAt(dataPHeader, 0)
		if err != nil {
			return nil, err
		}
	} else {

		_, err := sstable.File.ReadAt(dataPHeader, pageID*(PAGE_SIZE+HEADER_SIZE))
		if err != nil {
			return nil, err
		}
	}

	// get header
	header := dataPHeader[:HEADER_SIZE]
	data := dataPHeader[HEADER_SIZE:]

	// remove the null bytes
	header = bytes.Trim(header, "\x00")

	// append the data to the result
	result = append(result, data...)

	// get the next page
	nextPage, err := strconv.ParseInt(string(header), 10, 64)
	if err != nil {
		return nil, err
	}

	if nextPage == -1 {
		return result, nil

	}

	for {

		dataPHeader = make([]byte, PAGE_SIZE+HEADER_SIZE)

		_, err := p.file.ReadAt(dataPHeader, nextPage*(PAGE_SIZE+HEADER_SIZE))
		if err != nil {
			break
		}

		// get header
		header = dataPHeader[:HEADER_SIZE]
		data = dataPHeader[HEADER_SIZE:]

		// remove the null bytes
		header = bytes.Trim(header, "\x00")
		//data = bytes.Trim(data, "\x00")

		// append the data to the result
		result = append(result, data...)

		// get the next page
		nextPage, err = strconv.ParseInt(string(header), 10, 64)
		if err != nil || nextPage == -1 {
			break
		}

	}

	return result, nil
}

func (sstable *SSTable) get(key []byte) ([]byte, error) {
	lastPage := sstable.pager.Count() - 1 // Get last page for cuckoo filter

	// Try to decode the cuckoo filter from the final pages
	for lastPage >= 0 {
		cfData, err := sstable.pager.GetPage(lastPage)
		if err != nil {
			return nil, err
		}

		cf, err = cuckoofilter.Deserialize(cfData)
		if err == nil {
			break
		}

		lastPage--
	}

	if err != nil {
		return nil, err
	}

	if cf == nil {
		return nil, nil
	}

	// Check if the key exists in the cuckoo filter, if it does we get the page index
	pg, exists := cf.Lookup(key)
	if !exists {
		return nil, nil
	}

	// Get the key value pair
	data, err := sstable.pager.GetPage(pg)
	if err != nil {
		return nil, err
	}

	// Decode the key value pair
	k, v, ttl, err := decodeKV(data)
	if err != nil {
		return nil, err
	}

	// Decompress the key and value if the sstable is compressed
	if sstable.compressed {
		_, v, err = decompressKeyValue(k, v)
		if err != nil {
			return nil, err
		}

	}

	if ttl != nil && time.Now().After(*ttl) {
		return nil, nil
	}

	// Check for tombstone value
	if bytes.Equal(v, []byte(TOMBSTONE_VALUE)) {
		return nil, nil
	}

	return v, nil
}
