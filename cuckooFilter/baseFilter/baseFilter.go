package basefilter

import (
	"GoKeyValueWarehouse/cuckooFilter/baseFilter/util"
	"fmt"
	"math/bits"
	"math/rand"
)

const maxCuckooCount = 500

type Filter struct {
	buckets   []util.Bucket
	count     uint
	bucketPow uint
}

func NewFilter(capacity uint) *Filter {
	capacity = util.GetNextPow2(uint64(capacity)) / util.BucketSize
	if capacity == 0 {
		capacity = 1
	}
	buckets := make([]util.Bucket, capacity)
	return &Filter{
		buckets:   buckets,
		count:     0,
		bucketPow: uint(bits.TrailingZeros(capacity)),
	}
}

func (cf *Filter) Lookup(data []byte) bool {
	i1, fp := util.GetIndexAndFingerprint(data, cf.bucketPow)
	if cf.buckets[i1].GetFingerprintIndex(fp) > -1 {
		return true
	}
	i2 := util.GetAltIndex(fp, i1, cf.bucketPow)
	return cf.buckets[i2].GetFingerprintIndex(fp) > -1
}

func (cf *Filter) Reset() {
	for i := range cf.buckets {
		cf.buckets[i].Reset()
	}
	cf.count = 0
}

func randi(i1, i2 uint) uint {
	if rand.Intn(2) == 0 {
		return i1
	}
	return i2
}

// Insert inserts data into the counter and returns true upon success
func (cf *Filter) Insert(data []byte) bool {
	i1, fp := util.GetIndexAndFingerprint(data, cf.bucketPow)
	if cf.insert(fp, i1) {
		return true
	}
	i2 := util.GetAltIndex(fp, i1, cf.bucketPow)
	if cf.insert(fp, i2) {
		return true
	}
	return cf.reinsert(fp, randi(i1, i2))
}

func (cf *Filter) InsertUnique(data []byte) bool {
	if cf.Lookup(data) {
		return false
	}
	return cf.Insert(data)
}

func (cf *Filter) insert(fp util.Fingerprint, i uint) bool {
	if cf.buckets[i].Insert(fp) {
		cf.count++
		return true
	}
	return false
}

func (cf *Filter) reinsert(fp util.Fingerprint, i uint) bool {
	for k := 0; k < maxCuckooCount; k++ {
		j := rand.Intn(util.BucketSize)
		oldfp := fp
		fp = cf.buckets[i][j]
		cf.buckets[i][j] = oldfp

		i = util.GetAltIndex(fp, i, cf.bucketPow)
		if cf.insert(fp, i) {
			return true
		}
	}
	return false
}

func (cf *Filter) Delete(data []byte) bool {
	i1, fp := util.GetIndexAndFingerprint(data, cf.bucketPow)
	if cf.delete(fp, i1) {
		return true
	}
	i2 := util.GetAltIndex(fp, i1, cf.bucketPow)
	return cf.delete(fp, i2)
}

func (cf *Filter) delete(fp util.Fingerprint, i uint) bool {
	if cf.buckets[i].Delete(fp) {
		if cf.count > 0 {
			cf.count--
		}
		return true
	}
	return false
}

func (cf *Filter) Count() uint {
	return cf.count
}

func (cf *Filter) GetBuckets() []util.Bucket {
	return cf.buckets
}

func (cf *Filter) Encode() []byte {
	bytes := make([]byte, len(cf.buckets)*util.BucketSize)
	for i, b := range cf.buckets {
		for j, f := range b {
			index := (i * len(b)) + j
			bytes[index] = byte(f)
		}
	}
	return bytes
}

func Decode(bytes []byte) (*Filter, error) {
	var count uint
	if len(bytes)%util.BucketSize != 0 {
		return nil, fmt.Errorf("expected bytes to be multiple of %d, got %d", util.BucketSize, len(bytes))
	}
	if len(bytes) == 0 {
		return nil, fmt.Errorf("bytes can not be empty")
	}
	buckets := make([]util.Bucket, len(bytes)/4)
	for i, b := range buckets {
		for j := range b {
			index := (i * len(b)) + j
			if bytes[index] != 0 {
				buckets[i][j] = util.Fingerprint(bytes[index])
				count++
			}
		}
	}
	return &Filter{
		buckets:   buckets,
		count:     count,
		bucketPow: uint(bits.TrailingZeros(uint(len(buckets)))),
	}, nil
}
