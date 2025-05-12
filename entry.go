package db

var TOMBSTONE = []byte("TOMBSTONE")

type Entry struct {
	Key   []byte
	Value []byte
}

func (e *Entry) calculateSize() int64 {
	k := int64(len(e.Key))
	v := int64(len(e.Value))
	return k + v + 8
}

type request struct {
	Entries []*Entry
}

func (req *request) reset() {
	req.Entries = req.Entries[:0]
}
