package main

type MetaSST struct {
	Tables map[uint64]Table
}

type Table struct {
	id    uint64
	level int
}

func NewMetaSST() *MetaSST {
	return &MetaSST{
		Tables: make(map[uint64]Table, 0),
	}
}

//TODO:
