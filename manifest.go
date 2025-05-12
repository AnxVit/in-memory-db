package db

import (
	"GoKeyValueWarehouse/sstable"
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	ManifestFilename        = "MANIFEST"
	ManifestRewriteFilename = "MANIFEST_TEMP"
)

type OperationSST string

const (
	SST_CREATE OperationSST = "CREATE"
	SST_DELETE OperationSST = "DELETE"
)

type ManifestChange struct {
	Id    uint64       `json:"Id,omitempty"`
	Op    OperationSST `json:"Op,omitempty"`
	Level uint64       `json:"Level,omitempty"`
}

type ManifestChangeSet struct {
	Changes []*ManifestChange `json:"changes,omitempty"`
}

type ManifestInfo struct {
	Tables map[uint64]uint64
	Levels []LevelManifest

	Creations int64
	Deletions int64
}

func NewManifest(dir string) (*manifest, error) {
	return openOrCreateManifest(dir)
}

type LevelManifest struct {
	Tables map[uint64]struct{}
}

type manifest struct {
	sync.RWMutex
	file      *os.File
	directory string

	maxDeletions int64

	manifestInfo ManifestInfo
}

func openOrCreateManifest(dir string) (*manifest, error) {
	path := filepath.Join(dir, ManifestFilename)

	fp, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		levels := make([]LevelManifest, 0)
		m := ManifestInfo{
			Levels: levels,
			Tables: make(map[uint64]uint64),
		}
		fp, _, err := helpRewrite(dir, &m)
		if err != nil {
			return nil, err
		}
		mf := &manifest{
			file:         fp,
			directory:    dir,
			manifestInfo: m.clone(),
		}
		return mf, nil
	}

	manifestInfo, truncOffset, err := ReplayManifestFile(fp)
	if err != nil {
		_ = fp.Close()
		return nil, err
	}

	if err := fp.Truncate(truncOffset); err != nil {
		_ = fp.Close()
		return nil, err
	}
	if _, err = fp.Seek(0, io.SeekEnd); err != nil {
		_ = fp.Close()
		return nil, err
	}

	mf := &manifest{
		file:         fp,
		directory:    dir,
		manifestInfo: manifestInfo.clone(),
	}
	return mf, nil
}

func ReplayManifestFile(fp *os.File) (ManifestInfo, int64, error) {
	r := countReader{wrapped: bufio.NewReader(fp)}
	stat, err := fp.Stat()
	if err != nil {
		return ManifestInfo{}, 0, err
	}

	levels := make([]LevelManifest, 0)
	build := ManifestInfo{
		Levels: levels,
		Tables: make(map[uint64]uint64),
	}
	var offset int64
	for {
		offset = r.count
		var lenCrcBuf [4]byte
		_, err := io.ReadFull(&r, lenCrcBuf[:])
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return ManifestInfo{}, 0, err
		}
		length := binary.BigEndian.Uint32(lenCrcBuf[0:4])
		if length > uint32(stat.Size()) {
			return ManifestInfo{}, 0, fmt.Errorf(
				"buffer length: %d greater than file size: %d. manifest file might be corrupted",
				length, stat.Size())
		}
		var buf = make([]byte, length)
		if _, err := io.ReadFull(&r, buf); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return ManifestInfo{}, 0, err
		}

		var changeSet ManifestChangeSet
		if err := json.Unmarshal(buf, &changeSet); err != nil {
			return ManifestInfo{}, 0, err
		}

		if err := applyChangeSet(&build, &changeSet); err != nil {
			return ManifestInfo{}, 0, err
		}
	}

	return build, offset, nil
}

func helpRewrite(dir string, m *ManifestInfo) (*os.File, int, error) {
	rewritePath := filepath.Join(dir, ManifestRewriteFilename)
	fp, err := os.OpenFile(rewritePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return nil, 0, err
	}

	buf := make([]byte, 8)

	netCreations := len(m.Tables)
	changes := make([]*ManifestChange, 0, len(m.Tables))
	for id, tm := range m.Tables {
		changes = append(changes, newCreateChange(id, tm))
	}

	set := ManifestChangeSet{Changes: changes}

	changeBuf, err := json.Marshal(&set)
	if err != nil {
		fp.Close()
		return nil, 0, err
	}
	binary.BigEndian.PutUint32(buf, uint32(len(changeBuf)))
	buf = append(buf, changeBuf...)
	if _, err := fp.Write(buf); err != nil {
		fp.Close()
		return nil, 0, err
	}
	if err := fp.Sync(); err != nil {
		fp.Close()
		return nil, 0, err
	}

	if err = fp.Close(); err != nil {
		return nil, 0, err
	}
	manifestPath := filepath.Join(dir, ManifestFilename)
	if err := os.Rename(rewritePath, manifestPath); err != nil {
		return nil, 0, err
	}
	fp, err = os.OpenFile(manifestPath, os.O_RDWR, 0)
	if err != nil {
		return nil, 0, err
	}
	if _, err := fp.Seek(0, io.SeekEnd); err != nil {
		fp.Close()
		return nil, 0, err
	}

	return fp, netCreations, nil
}

func (m *ManifestInfo) clone() ManifestInfo {
	changes := make([]*ManifestChange, 0, len(m.Tables))
	for id, tm := range m.Tables {
		changes = append(changes, newCreateChange(id, tm))
	}
	changeSet := ManifestChangeSet{Changes: changes}
	levels := make([]LevelManifest, 0)
	ret := ManifestInfo{
		Levels: levels,
		Tables: make(map[uint64]uint64),
	}
	applyChangeSet(&ret, &changeSet)
	return ret
}

type countReader struct {
	wrapped *bufio.Reader
	count   int64
}

func (r *countReader) Read(p []byte) (n int, err error) {
	n, err = r.wrapped.Read(p)
	r.count += int64(n)
	return
}

func (r *countReader) ReadByte() (b byte, err error) {
	b, err = r.wrapped.ReadByte()
	if err == nil {
		r.count++
	}
	return
}

func applyManifestChange(build *ManifestInfo, tc *ManifestChange) error {
	switch tc.Op {
	case SST_CREATE:
		if _, ok := build.Tables[tc.Id]; ok {
			return fmt.Errorf("MANIFEST invalid, table %d exists", tc.Id)
		}
		build.Tables[tc.Id] = tc.Level
		for len(build.Levels) <= int(tc.Level) {
			build.Levels = append(build.Levels, LevelManifest{make(map[uint64]struct{})})
		}
		build.Levels[tc.Level].Tables[tc.Id] = struct{}{}
		build.Creations++
	case SST_DELETE:
		tm, ok := build.Tables[tc.Id]
		if !ok {
			return fmt.Errorf("MANIFEST removes non-existing table %d", tc.Id)
		}
		delete(build.Levels[tm].Tables, tc.Id)
		delete(build.Tables, tc.Id)
		build.Deletions++
	default:
		return fmt.Errorf("MANIFEST file has invalid manifestChange op")
	}
	return nil
}

func (mf *manifest) rewrite() error {
	// In Windows the files should be closed before doing a Rename.
	if err := mf.file.Close(); err != nil {
		return err
	}
	fp, netCreations, err := helpRewrite(mf.directory, &mf.manifestInfo)
	if err != nil {
		return err
	}
	mf.file = fp
	mf.manifestInfo.Creations = int64(netCreations)
	mf.manifestInfo.Deletions = 0

	return nil
}

func (mf *manifest) addChanges(changesParam []*ManifestChange) error {
	changes := ManifestChangeSet{Changes: changesParam}
	buf, err := json.Marshal(&changes)
	if err != nil {
		return err
	}

	mf.Lock()
	defer mf.Unlock()
	if err := applyChangeSet(&mf.manifestInfo, &changes); err != nil {
		return err
	}
	if mf.manifestInfo.Deletions > mf.maxDeletions {
		if err := mf.rewrite(); err != nil {
			return err
		}
	} else {
		lenBuf := make([]byte, 0, 4)
		binary.BigEndian.PutUint32(lenBuf, uint32(len(buf)))
		buf = append(lenBuf[:], buf...)
		if _, err := mf.file.Write(buf); err != nil {
			return err
		}
	}

	return mf.file.Sync()
}

func buildChangeSet(cd *compactDef, newTables []*sstable.SSTable) ManifestChangeSet {
	changes := []*ManifestChange{}
	for _, table := range newTables {
		changes = append(changes,
			newCreateChange(table.GetID(), uint64(cd.nextLevel.lvID)))
	}
	for _, table := range cd.top {
		changes = append(changes, newDeleteChange(table.GetID()))
	}
	for _, table := range cd.bot {
		changes = append(changes, newDeleteChange(table.GetID()))
	}
	return ManifestChangeSet{Changes: changes}
}

func applyChangeSet(build *ManifestInfo, changeSet *ManifestChangeSet) error {
	for _, change := range changeSet.Changes {
		if err := applyManifestChange(build, change); err != nil {
			return err
		}
	}
	return nil
}

func newCreateChange(id, level uint64) *ManifestChange {
	return &ManifestChange{
		Id:    id,
		Op:    SST_CREATE,
		Level: level,
	}
}

func newDeleteChange(id uint64) *ManifestChange {
	return &ManifestChange{
		Id: id,
		Op: SST_DELETE,
	}
}

func (mf *manifest) close() error {
	return mf.file.Close()
}
