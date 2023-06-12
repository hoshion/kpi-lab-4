package datastore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const outFileName = "current-data"

var ErrNotFound = fmt.Errorf("record does not exist")

type hashIndex map[string]int64

type IndexOp struct {
	isWrite bool
	key     string
	index   int64
}

type EntryWithChan struct {
	e   entry
	res chan error
}

type KeyPosition struct {
	segment  *Segment
	position int64
}

type Db struct {
	out              *os.File
	outPath          string
	outOffset        int64
	dir              string
	segmentSize      int64
	lastSegmentIndex int
	indexOps         chan IndexOp
	keyPositions     chan *KeyPosition
	putOps           chan EntryWithChan

	segments []*Segment
}

func NewDb(dir string, segmentSize int64) (*Db, error) {
	db := &Db{
		segments:     make([]*Segment, 0),
		dir:          dir,
		segmentSize:  segmentSize,
		indexOps:     make(chan IndexOp),
		keyPositions: make(chan *KeyPosition),
		putOps:       make(chan EntryWithChan),
	}

	err := db.createSegment()
	if err != nil {
		return nil, err
	}

	err = db.recover()
	if err != nil && err != io.EOF {
		return nil, err
	}

	db.startIndexRoutine()
	db.startPutRoutine()

	return db, nil
}

func (db *Db) startIndexRoutine() {
	go func() {
		for {
			op := <-db.indexOps
			if op.isWrite {
				db.setKey(op.key, op.index)
			} else {
				s, p, err := db.getSegmentAndPosition(op.key)
				if err != nil {
					db.keyPositions <- nil
				} else {
					db.keyPositions <- &KeyPosition{
						s,
						p,
					}
				}
			}
		}
	}()
}

const bufSize = 8192

func (db *Db) createSegment() error {
	filePath := db.getNewFileName()
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return err
	}

	newSegment := &Segment{
		filePath: filePath,
		index:    make(hashIndex),
	}
	db.out = f
	db.outOffset = 0
	db.outPath = filePath
	db.segments = append(db.segments, newSegment)
	if len(db.segments) >= 3 {
		db.compactOldSegments()
	}
	return err
}

func (db *Db) getNewFileName() string {
	result := filepath.Join(db.dir, fmt.Sprintf("%s%d", outFileName, db.lastSegmentIndex))
	db.lastSegmentIndex++
	return result
}

func (db *Db) compactOldSegments() {
	go func() {
		filePath := db.getNewFileName()
		newSegment := &Segment{
			filePath: filePath,
			index:    make(hashIndex),
		}
		var offset int64
		f, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
		if err != nil {
			return
		}
		lastSegmentIndex := len(db.segments) - 2
		for i := 0; i <= lastSegmentIndex; i++ {
			s := db.segments[i]
			s.mu.Lock()
			for key, index := range s.index {
				if i < lastSegmentIndex {
					isInNewerSegments := checkKeyInSegments(db.segments[i+1:lastSegmentIndex+1], key)
					if isInNewerSegments {
						continue
					}
				}
				value, _ := s.getFromSegment(index)
				e := entry{
					key:   key,
					value: value,
				}
				n, err := f.Write(e.Encode())
				if err == nil {
					newSegment.index[key] = offset
					offset += int64(n)
				}
			}
			s.mu.Unlock()
		}
		db.segments = []*Segment{newSegment, db.getLastSegment()}
	}()
}

func checkKeyInSegments(segments []*Segment, key string) bool {
	for _, s := range segments {
		s.mu.Lock()
		if _, ok := s.index[key]; ok {
			s.mu.Unlock()
			return true
		}
		s.mu.Unlock()
	}
	return false
}

func (db *Db) recover() error {
	var err error
	var buf [bufSize]byte

	in := bufio.NewReaderSize(db.out, bufSize)
	for err == nil {
		var (
			header, data []byte
			n            int
		)
		header, err = in.Peek(bufSize)
		if err == io.EOF {
			if len(header) == 0 {
				return err
			}
		} else if err != nil {
			return err
		}
		size := binary.LittleEndian.Uint32(header)

		if size < bufSize {
			data = buf[:size]
		} else {
			data = make([]byte, size)
		}
		n, err = in.Read(data)

		if err == nil {
			if n != int(size) {
				return fmt.Errorf("corrupted file")
			}

			var e entry
			e.Decode(data)
			db.setKey(e.key, int64(n))
		}
	}
	return err
}

func (db *Db) Close() error {
	return db.out.Close()
}

func (db *Db) setKey(key string, n int64) {
	db.getLastSegment().mu.Lock()
	defer db.getLastSegment().mu.Unlock()
	db.getLastSegment().index[key] = db.outOffset
	db.outOffset += n
}

func (db *Db) getSegmentAndPosition(key string) (*Segment, int64, error) {
	for i := range db.segments {
		s := db.segments[len(db.segments)-i-1]
		s.mu.Lock()
		pos, ok := s.index[key]
		if ok {
			s.mu.Unlock()
			return s, pos, nil
		}
		s.mu.Unlock()
	}

	return nil, 0, ErrNotFound
}

func (db *Db) getPos(key string) *KeyPosition {
	op := IndexOp{
		isWrite: false,
		key:     key,
	}
	db.indexOps <- op
	return <-db.keyPositions
}

func (db *Db) Get(key string) (string, error) {
	keyPos := db.getPos(key)
	if keyPos == nil {
		return "", ErrNotFound
	}
	value, err := keyPos.segment.getFromSegment(keyPos.position)
	if err != nil {
		return "", err
	}
	return value, nil
}

func (db *Db) getLastSegment() *Segment {
	return db.segments[len(db.segments)-1]
}

func (db *Db) startPutRoutine() {
	go func() {
		for {
			e := <-db.putOps
			length := e.e.getLength()
			stat, err := db.out.Stat()
			if err != nil {
				e.res <- err
				continue
			}
			if stat.Size()+length > db.segmentSize {
				err := db.createSegment()
				if err != nil {
					e.res <- err
					continue
				}
			}
			n, err := db.out.Write(e.e.Encode())
			if err == nil {
				db.indexOps <- IndexOp{
					isWrite: true,
					key:     e.e.key,
					index:   int64(n),
				}
			}
			e.res <- nil
		}
	}()
}

func (db *Db) Put(key, value string) error {
	e := entry{
		key:   key,
		value: value,
	}
	res := make(chan error)
	db.putOps <- EntryWithChan{
		e:   e,
		res: res,
	}
	return <-res
}

type Segment struct {
	outOffset int64

	index    hashIndex
	filePath string
	mu       sync.Mutex
}

func (s *Segment) getFromSegment(position int64) (string, error) {
	file, err := os.Open(s.filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	_, err = file.Seek(position, 0)
	if err != nil {
		return "", err
	}

	reader := bufio.NewReader(file)
	value, err := readValue(reader)
	if err != nil {
		return "", err
	}
	return value, nil
}
