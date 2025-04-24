package adaptivefilter

type option func(*ScalableCuckooFilter)

func SetLoadFactor(loadFactor float32) func(*ScalableCuckooFilter) {
	return func(filter *ScalableCuckooFilter) {
		filter.loadFactor = loadFactor
	}
}

func SetScaleFactor(scaleFactor func(capacity uint) uint) func(*ScalableCuckooFilter) {
	return func(filter *ScalableCuckooFilter) {
		filter.scaleFactor = scaleFactor
	}
}
