package db

type OPR_CODE int

const (
	INSERT OPR_CODE = iota
	UPDATE
	DELETE
	READ
)

type Operation struct {
	Op    OPR_CODE
	Key   []byte
	Value []byte
}
