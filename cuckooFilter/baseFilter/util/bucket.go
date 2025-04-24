package util

type Bucket [BucketSize]Fingerprint

const (
	BucketSize = 4
)

func (b *Bucket) Insert(fp Fingerprint) bool {
	for i, tfp := range b {
		if tfp == 0 {
			b[i] = fp
			return true
		}
	}
	return false
}

func (b *Bucket) Delete(fp Fingerprint) bool {
	for i, tfp := range b {
		if tfp == fp {
			b[i] = 0
			return true
		}
	}
	return false
}

func (b *Bucket) GetFingerprintIndex(fp Fingerprint) int {
	for i, tfp := range b {
		if tfp == fp {
			return i
		}
	}
	return -1
}

func (b *Bucket) Reset() {
	for i := range b {
		b[i] = 0
	}
}
