package adaptivefilter

import (
	basefilter "GoKeyValueWarehouse/cuckooFilter/baseFilter"
	"GoKeyValueWarehouse/cuckooFilter/baseFilter/util"
	"bytes"
	"encoding/gob"
)

const (
	DefaultLoadFactor = 0.9
	DefaultCapacity   = 10000
)

type ScalableCuckooFilter struct {
	filters    []*basefilter.Filter
	loadFactor float32

	scaleFactor func(capacity uint) uint
}

type Store struct {
	Bytes      [][]byte
	LoadFactor float32
}

func NewScalableCuckooFilter(opts ...option) *ScalableCuckooFilter {
	sfilter := new(ScalableCuckooFilter)
	for _, opt := range opts {
		opt(sfilter)
	}

	if sfilter.loadFactor == 0 {
		sfilter.loadFactor = DefaultLoadFactor
	}
	if sfilter.scaleFactor == nil {
		sfilter.scaleFactor = func(currentSize uint) uint {
			return currentSize * util.BucketSize * 2
		}
	}

	sfilter.filters = []*basefilter.Filter{basefilter.NewFilter(DefaultCapacity)}

	return sfilter
}

func (sf *ScalableCuckooFilter) Lookup(data []byte) bool {
	for _, filter := range sf.filters {
		if filter.Lookup(data) {
			return true
		}
	}
	return false
}

func (sf *ScalableCuckooFilter) Reset() {
	for _, filter := range sf.filters {
		filter.Reset()
	}
}

func (sf *ScalableCuckooFilter) Insert(data []byte) bool {
	needScale := false
	lastFilter := sf.filters[len(sf.filters)-1]
	if (float32(lastFilter.Count()) / float32(len(lastFilter.GetBuckets()))) > sf.loadFactor {
		needScale = true
	} else {
		b := lastFilter.Insert(data)
		needScale = !b
	}
	if !needScale {
		return true
	}
	newFilter := basefilter.NewFilter(sf.scaleFactor(uint(len(lastFilter.GetBuckets()))))
	sf.filters = append(sf.filters, newFilter)
	return newFilter.Insert(data)
}

func (sf *ScalableCuckooFilter) InsertUnique(data []byte) bool {
	if sf.Lookup(data) {
		return false
	}
	return sf.Insert(data)
}

func (sf *ScalableCuckooFilter) Delete(data []byte) bool {
	for _, filter := range sf.filters {
		if filter.Delete(data) {
			return true
		}
	}
	return false
}

func (sf *ScalableCuckooFilter) Encode() []byte {
	slice := make([][]byte, len(sf.filters))
	for i, filter := range sf.filters {
		encode := filter.Encode()
		slice[i] = encode
	}
	store := &Store{
		Bytes:      slice,
		LoadFactor: sf.loadFactor,
	}
	buf := bytes.NewBuffer(nil)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(store)
	if err != nil {
		return nil
	}
	return buf.Bytes()
}

func (sf *ScalableCuckooFilter) DecodeWithParam(fBytes []byte, opts ...option) (*ScalableCuckooFilter, error) {
	instance, err := DecodeScalableFilter(fBytes)
	if err != nil {
		return nil, err
	}
	for _, opt := range opts {
		opt(instance)
	}
	return instance, nil
}

func DecodeScalableFilter(fBytes []byte) (*ScalableCuckooFilter, error) {
	buf := bytes.NewBuffer(fBytes)
	dec := gob.NewDecoder(buf)
	store := &Store{}
	err := dec.Decode(store)
	if err != nil {
		return nil, err
	}
	filterSize := len(store.Bytes)
	instance := NewScalableCuckooFilter(func(filter *ScalableCuckooFilter) {
		filter.filters = make([]*basefilter.Filter, filterSize)
	}, SetLoadFactor(store.LoadFactor))
	for i, oneBytes := range store.Bytes {
		filter, err := basefilter.Decode(oneBytes)
		if err != nil {
			return nil, err
		}
		instance.filters[i] = filter
	}
	return instance, nil

}
