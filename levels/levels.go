package levels

import "GoKeyValueWarehouse/levels/sstable"

type LevelsController struct {
	sst []*sstable.SSTable
}

func NewLevelContoller() *LevelsController {
	return &LevelsController{
		sst: make([]*sstable.SSTable, 0),
	}
}

func (lc *LevelsController) findSSTable(key []byte) []*sstable.SSTable {
	result := make([]*sstable.SSTable, 0)
	for _, sst := range lc.sst {
		if sst.FilterLookup(key) {
			result = append(result, sst)
		}
	}
	return result
}
