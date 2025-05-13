package wal

import (
	ringcache "GoKeyValueWarehouse/wal/ringCache"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

type LogFormat byte

const (
	Binary LogFormat = 0
	JSON   LogFormat = 1
)

var DefaultOptions = &Options{
	NoSync:           false,
	SegmentSize:      20971520,
	LogFormat:        Binary,
	SegmentCacheSize: 2,
	NoCopy:           false,
	DirPerms:         0750,
	FilePerms:        0640,
}

type Log struct {
	mu         sync.RWMutex
	path       string              // absolute path to log directory
	opts       Options             // log options
	closed     bool                // log is closed
	segments   []*segment          // all known log segments
	firstIndex uint64              // index of the first entry in log
	lastIndex  uint64              // index of the last entry in log
	sfile      *os.File            // tail segment file handle
	wbatch     Batch               // reusable write batch
	scache     ringcache.RingCache // segment entries cache
}

type segment struct {
	path  string // path of segment file
	index uint64 // first index of segment
	ebuf  []byte // cached entries buffer
	epos  []bpos // cached entries positions in buffer
}

type bpos struct {
	pos int // byte position
	end int // one byte past pos
}

func (l *Log) GetFirstIndex() uint64 {
	return l.firstIndex
}

func (l *Log) GetLastIndex() uint64 {
	return l.lastIndex
}

func Open(opts *Options, dirPath string) (*Log, error) {
	if opts == nil {
		opts = DefaultOptions
	}
	if opts.SegmentCacheSize <= 0 {
		opts.SegmentCacheSize = DefaultOptions.SegmentCacheSize
	}
	if opts.SegmentSize <= 0 {
		opts.SegmentSize = DefaultOptions.SegmentSize
	}
	if opts.DirPerms == 0 {
		opts.DirPerms = DefaultOptions.DirPerms
	}
	if opts.FilePerms == 0 {
		opts.FilePerms = DefaultOptions.FilePerms
	}

	path := filepath.Join(opts.Directory, dirPath)

	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	l := &Log{path: path, opts: *opts}
	l.scache.Resize(l.opts.SegmentCacheSize)
	if err := os.MkdirAll(path, l.opts.DirPerms); err != nil {
		return nil, err
	}
	if err := l.load(); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *Log) load() error {
	fis, err := os.ReadDir(l.path)
	if err != nil {
		return err
	}
	startIdx := -1
	endIdx := -1
	for _, fi := range fis {
		name := fi.Name()
		if fi.IsDir() || len(name) < 20 {
			continue
		}
		index, err := strconv.ParseUint(name[:20], 10, 64)
		if err != nil || index == 0 {
			continue
		}
		isStart := len(name) == 26 && strings.HasSuffix(name, ".START")
		isEnd := len(name) == 24 && strings.HasSuffix(name, ".END")
		if len(name) == 20 || isStart || isEnd {
			if isStart {
				startIdx = len(l.segments)
			} else if isEnd && endIdx == -1 {
				endIdx = len(l.segments)
			}
			l.segments = append(l.segments, &segment{
				index: index,
				path:  filepath.Join(l.path, name),
			})
		}
	}
	if len(l.segments) == 0 {
		// Create a new log
		l.segments = append(l.segments, &segment{
			index: 1,
			path:  filepath.Join(l.path, fmt.Sprintf("%020d", 1)),
		})
		l.firstIndex = 1
		l.lastIndex = 0
		l.sfile, err = os.OpenFile(l.segments[0].path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, l.opts.FilePerms)
		return err
	}
	if startIdx != -1 {
		if endIdx != -1 {
			return ErrCorrupt
		}
		for i := 0; i < startIdx; i++ {
			if err := os.Remove(l.segments[i].path); err != nil {
				return err
			}
		}
		l.segments = append([]*segment{}, l.segments[startIdx:]...)
		orgPath := l.segments[0].path
		finalPath := orgPath[:len(orgPath)-len(".START")]
		err := os.Rename(orgPath, finalPath)
		if err != nil {
			return err
		}
		l.segments[0].path = finalPath
	}
	if endIdx != -1 {
		for i := len(l.segments) - 1; i > endIdx; i-- {
			if err := os.Remove(l.segments[i].path); err != nil {
				return err
			}
		}
		l.segments = append([]*segment{}, l.segments[:endIdx+1]...)
		if len(l.segments) > 1 && l.segments[len(l.segments)-2].index ==
			l.segments[len(l.segments)-1].index {

			l.segments[len(l.segments)-2] = l.segments[len(l.segments)-1]
			l.segments = l.segments[:len(l.segments)-1]
		}
		orgPath := l.segments[len(l.segments)-1].path
		finalPath := orgPath[:len(orgPath)-len(".END")]
		err := os.Rename(orgPath, finalPath)
		if err != nil {
			return err
		}
		l.segments[len(l.segments)-1].path = finalPath
	}
	l.firstIndex = l.segments[0].index

	lseg := l.segments[len(l.segments)-1]
	l.sfile, err = os.OpenFile(lseg.path, os.O_WRONLY, l.opts.FilePerms)
	if err != nil {
		return err
	}
	if _, err := l.sfile.Seek(0, 2); err != nil {
		return err
	}
	if err := l.loadSegmentEntries(lseg); err != nil {
		return err
	}
	l.lastIndex = lseg.index + uint64(len(lseg.epos)) - 1
	return nil
}

func (l *Log) loadSegmentEntries(s *segment) error {
	data, err := os.ReadFile(s.path)
	if err != nil {
		return err
	}
	ebuf := data
	var epos []bpos
	var pos int
	for len(data) > 0 {
		var n int
		if l.opts.LogFormat == JSON {
			n, err = loadNextJSONEntry(data)
		} else {
			n, err = loadNextBinaryEntry(data)
		}
		if err != nil {
			return err
		}
		data = data[n:]
		epos = append(epos, bpos{pos, pos + n})
		pos += n
	}
	s.ebuf = ebuf
	s.epos = epos
	return nil
}

func (l *Log) WriteBatch(b *Batch) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if len(b.entries) == 0 {
		return nil
	}
	return l.writeBatch(b)
}

func (l *Log) writeBatch(b *Batch) error {
	for i := 0; i < len(b.entries); i++ {
		if b.entries[i].index != l.lastIndex+uint64(i+1) {
			return ErrOutOfOrder
		}
	}
	s := l.segments[len(l.segments)-1]
	if len(s.ebuf) > l.opts.SegmentSize {
		if err := l.cycle(); err != nil {
			return err
		}
		s = l.segments[len(l.segments)-1]
	}

	mark := len(s.ebuf)
	datas := b.datas
	for i := 0; i < len(b.entries); i++ {
		data := datas[:b.entries[i].size]
		var epos bpos
		s.ebuf, epos = l.appendEntry(s.ebuf, b.entries[i].index, data)
		s.epos = append(s.epos, epos)
		if len(s.ebuf) >= l.opts.SegmentSize {
			if _, err := l.sfile.Write(s.ebuf[mark:]); err != nil {
				return err
			}
			l.lastIndex = b.entries[i].index
			if err := l.cycle(); err != nil {
				return err
			}
			s = l.segments[len(l.segments)-1]
			mark = 0
		}
		datas = datas[b.entries[i].size:]
	}
	if len(s.ebuf)-mark > 0 {
		if _, err := l.sfile.Write(s.ebuf[mark:]); err != nil {
			return err
		}
		l.lastIndex = b.entries[len(b.entries)-1].index
	}
	if !l.opts.NoSync {
		if err := l.sfile.Sync(); err != nil {
			return err
		}
	}
	b.Clear()
	return nil
}

func (l *Log) appendEntry(dst []byte, index uint64, data []byte) (out []byte,
	epos bpos) {
	switch l.opts.LogFormat {
	case JSON:
		return appendJSONEntry(dst, index, data)
	case Binary:
		return appendBinaryEntry(dst, data)
	}
	return
}

func (l *Log) cycle() error {
	if err := l.sfile.Sync(); err != nil {
		return err
	}
	if err := l.sfile.Close(); err != nil {
		return err
	}

	l.pushCache(len(l.segments) - 1)
	s := &segment{
		index: l.lastIndex + 1,
		path:  filepath.Join(l.path, fmt.Sprintf("%020d", l.lastIndex+1)),
	}
	var err error
	l.sfile, err = os.OpenFile(s.path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, l.opts.FilePerms)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s)
	return nil
}

func (l *Log) Write(index uint64, data []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	l.wbatch.Clear()
	l.wbatch.Write(index, data)
	return l.writeBatch(&l.wbatch)
}

func (l *Log) Read(index uint64) (data []byte, err error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.closed {
		return nil, ErrClosed
	}
	if index == 0 || index < l.firstIndex || index > l.lastIndex {
		return nil, ErrNotFound
	}
	s, err := l.loadSegment(index)
	if err != nil {
		return nil, err
	}
	epos := s.epos[index-s.index]
	edata := s.ebuf[epos.pos:epos.end]
	if l.opts.LogFormat == JSON {
		return readJSON(edata)
	}
	// binary read
	size, n := binary.Uvarint(edata)
	if n <= 0 {
		return nil, ErrCorrupt
	}
	if uint64(len(edata)-n) < size {
		return nil, ErrCorrupt
	}
	if l.opts.NoCopy {
		data = edata[n : uint64(n)+size]
	} else {
		data = make([]byte, size)
		copy(data, edata[n:])
	}
	return data, nil
}

func readJSON(edata []byte) ([]byte, error) {
	var data []byte
	s, err := findValue(edata, "data")
	if err != nil {
		return nil, err
	}
	if len(s) > 0 && s[0] == '$' {
		var err error
		data, err = base64.URLEncoding.DecodeString(s[1:])
		if err != nil {
			return nil, ErrCorrupt
		}
	} else if len(s) > 0 && s[0] == '+' {
		data = make([]byte, len(s[1:]))
		copy(data, s[1:])
	} else {
		return nil, ErrCorrupt
	}
	return data, nil
}

func (l *Log) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	return l.sfile.Sync()
}

func (l *Log) TruncateFront(index uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	return l.truncateFront(index)
}
func (l *Log) truncateFront(index uint64) (err error) {
	if index == 0 || l.lastIndex == 0 ||
		index < l.firstIndex || index > l.lastIndex {
		return ErrOutOfRange
	}
	if index == l.firstIndex {
		return nil
	}

	segIdx := l.findSegment(index)
	var s *segment
	s, err = l.loadSegment(index)
	if err != nil {
		return err
	}
	epos := s.epos[index-s.index:]
	ebuf := s.ebuf[epos[0].pos:]

	tempName := filepath.Join(l.path, "TEMP")
	if err = func() error {
		f, err := os.OpenFile(tempName, os.O_CREATE|os.O_RDWR|os.O_TRUNC, l.opts.FilePerms)
		if err != nil {
			return err
		}
		defer f.Close()
		if _, err := f.Write(ebuf); err != nil {
			return err
		}
		if err := f.Sync(); err != nil {
			return err
		}
		return f.Close()
	}(); err != nil {
		return fmt.Errorf("failed to create temp file for new start segment: %w", err)
	}

	startName := filepath.Join(l.path, fmt.Sprintf("%020d", index)+".START")
	if err = os.Rename(tempName, startName); err != nil {
		return err
	}

	defer func() {
		if v := recover(); v != nil {
			err = ErrCorrupt
		}
	}()
	if segIdx == len(l.segments)-1 {
		if err = l.sfile.Close(); err != nil {
			return err
		}
	}
	for i := 0; i <= segIdx; i++ {
		if err = os.Remove(l.segments[i].path); err != nil {
			return err
		}
	}
	newName := filepath.Join(l.path, fmt.Sprintf("%020d", index))
	if err = os.Rename(startName, newName); err != nil {
		return err
	}
	s.path = newName
	s.index = index
	if segIdx == len(l.segments)-1 {
		if l.sfile, err = os.OpenFile(newName, os.O_WRONLY, l.opts.FilePerms); err != nil {
			return err
		}
		var n int64
		if n, err = l.sfile.Seek(0, 2); err != nil {
			return err
		}
		if n != int64(len(ebuf)) {
			err = errors.New("invalid seek")
			return err
		}
		if err = l.loadSegmentEntries(s); err != nil {
			return err
		}
	}
	l.segments = append([]*segment{}, l.segments[segIdx:]...)
	l.firstIndex = index
	l.clearCache()
	return nil
}

func (l *Log) TruncateBack(index uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	return l.truncateBack(index)
}

func (l *Log) truncateBack(index uint64) (err error) {
	if index == 0 || l.lastIndex == 0 ||
		index < l.firstIndex || index > l.lastIndex {
		return ErrOutOfRange
	}
	if index == l.lastIndex {
		return nil
	}
	segIdx := l.findSegment(index)
	var s *segment
	s, err = l.loadSegment(index)
	if err != nil {
		return err
	}
	epos := s.epos[:index-s.index+1]
	ebuf := s.ebuf[:epos[len(epos)-1].end]
	tempName := filepath.Join(l.path, "TEMP")
	if err = func() error {
		f, err := os.OpenFile(tempName, os.O_CREATE|os.O_RDWR|os.O_TRUNC, l.opts.FilePerms)
		if err != nil {
			return err
		}
		defer f.Close()
		if _, err := f.Write(ebuf); err != nil {
			return err
		}
		if err := f.Sync(); err != nil {
			return err
		}
		return f.Close()
	}(); err != nil {
		return fmt.Errorf("failed to create temp file for new end segment: %w", err)
	}
	endName := filepath.Join(l.path, fmt.Sprintf("%020d", index)+".END")
	if err = os.Rename(tempName, endName); err != nil {
		return err
	}

	defer func() {
		if v := recover(); v != nil {
			err = ErrCorrupt
		}
	}()

	if err = l.sfile.Close(); err != nil {
		return err
	}
	for i := segIdx; i < len(l.segments); i++ {
		if err = os.Remove(l.segments[i].path); err != nil {
			return err
		}
	}
	newName := filepath.Join(l.path, fmt.Sprintf("%020d", s.index))
	if err = os.Rename(endName, newName); err != nil {
		return err
	}
	if l.sfile, err = os.OpenFile(newName, os.O_WRONLY, l.opts.FilePerms); err != nil {
		return err
	}
	var n int64
	n, err = l.sfile.Seek(0, 2)
	if err != nil {
		return err
	}
	if n != int64(len(ebuf)) {
		err = errors.New("invalid seek")
		return err
	}
	s.path = newName
	l.segments = append([]*segment{}, l.segments[:segIdx+1]...)
	l.lastIndex = index
	l.clearCache()
	if err = l.loadSegmentEntries(s); err != nil {
		return err
	}
	return nil
}

func (l *Log) loadSegment(index uint64) (*segment, error) { //TODO: maybe check of index
	lseg := l.segments[len(l.segments)-1]
	if index >= lseg.index {
		return lseg, nil
	}
	var rseg *segment
	val := l.scache.GetLeast()
	if v, ok := val.(*segment); ok {
		if index >= v.index && index < v.index+uint64(len(v.epos)) {
			rseg = v
		}
	}
	if rseg != nil {
		return rseg, nil
	}
	idx := l.findSegment(index)
	s := l.segments[idx]
	if len(s.epos) == 0 {
		if err := l.loadSegmentEntries(s); err != nil {
			return nil, err
		}
	}
	l.pushCache(idx)
	return s, nil
}

func (l *Log) findSegment(index uint64) int {
	i, j := 0, len(l.segments)
	for i < j {
		h := i + (j-i)/2
		if index >= l.segments[h].index {
			i = h + 1
		} else {
			j = h
		}
	}
	return i - 1
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	if err := l.sfile.Sync(); err != nil {
		return err
	}
	if err := l.sfile.Close(); err != nil {
		return err
	}
	l.closed = true
	return nil
}

func (l *Log) pushCache(segIdx int) {
	v, evicted := l.scache.Add(segIdx, l.segments[segIdx])
	if evicted {
		s := v.(*segment)
		s.ebuf = nil
		s.epos = nil
	}
}

func (l *Log) ClearCache() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	l.clearCache()
	return nil
}

func (l *Log) clearCache() {
	val := l.scache.GetLeast()
	if v, ok := val.(*segment); ok {
		v.ebuf = nil
		v.epos = nil
	}
	l.scache = ringcache.RingCache{}
	l.scache.Resize(l.opts.SegmentCacheSize)
}
