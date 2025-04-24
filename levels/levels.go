package levels

import "GoKeyValueWarehouse/levels/sstable"

type LevelsController struct {
	sst []*sstable.SSTable
}
