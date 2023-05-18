package datastore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const outFileName = "current-data"

var ErrNotFound = fmt.Errorf("record does not exist")

type hashIndex map[string]int64

type Db struct {
	out              *os.File
	outPath          string
	outOffset        int64
	dir              string
	segmentSize      int64
	lastSegmentIndex int

	index    hashIndex
	segments []*Segment
}

func NewDb(dir string, segmentSize int64) (*Db, error) {
	db := &Db{
		segments:    make([]*Segment, 0),
		dir:         dir,
		segmentSize: segmentSize,
	}

	err := db.createSegment()
	if err != nil {
		return nil, err
	}

	err = db.recover()
	if err != nil && err != io.EOF {
		return nil, err
	}
	return db, nil
}

const bufSize = 8192

func (db *Db) createSegment() error {
	filePath := filepath.Join(db.dir, fmt.Sprintf("%s%d", outFileName, len(db.segments)+db.lastSegmentIndex))
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
	db.lastSegmentIndex++
	db.segments = append(db.segments, newSegment)
	if len(db.segments) >= 3 {

	}
	return err
}

func (db *Db) compactOldSegments() {
	go func() {
		filePath := filepath.Join(db.dir, outFileName+string(rune(len(db.segments)+db.lastSegmentIndex)))
		newSegment := &Segment{
			filePath: filePath,
			index:    make(hashIndex),
		}
		var offset int64
		f, _ := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
		length := len(db.segments) - 1
		for i := 0; i < length; i++ {
			s := db.segments[i]
			for key, index := range s.index {
				isInNewerSegments := checkKeyInSegments(db.segments[i+1:length-1], key)
				if isInNewerSegments {
					return
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
		}
		db.lastSegmentIndex++
	}()
}

func checkKeyInSegments(segments []*Segment, key string) bool {
	for _, s := range segments {
		if _, ok := s.index[key]; ok {
			return true
		}
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
	db.getLastSegment().index[key] = db.outOffset
	db.outOffset += n
}

func (db *Db) getSegmentAndPosition(key string) (*Segment, int64, error) {
	for i := range db.segments {
		s := db.segments[len(db.segments)-i-1]
		pos, ok := s.index[key]
		if ok {
			return s, pos, nil
		}
	}

	return nil, 0, ErrNotFound
}

func (db *Db) Get(key string) (string, error) {
	segment, position, err := db.getSegmentAndPosition(key)
	if err != nil {
		return "", err
	}
	return segment.getFromSegment(position)
}

func (db *Db) getLastSegment() *Segment {
	return db.segments[len(db.segments)-1]
}

func (db *Db) Put(key, value string) error {
	e := entry{
		key:   key,
		value: value,
	}
	length := e.getLength()
	stat, err := db.out.Stat()
	if err != nil {
		return err
	}
	if stat.Size()+length > db.segmentSize {
		err := db.createSegment()
		if err != nil {
			return err
		}
	}
	n, err := db.out.Write(e.Encode())
	if err == nil {
		db.setKey(e.key, int64(n))
	}
	return err
}

type Segment struct {
	outOffset int64

	index    hashIndex
	filePath string
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
