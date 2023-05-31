package datastore

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDb_Put(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDb(dir, 250)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	pairs := [][]string{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
	}

	outFile, err := os.Open(filepath.Join(dir, outFileName+"0"))
	if err != nil {
		t.Fatal(err)
	}

	t.Run("put/get", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
			value, err := db.Get(pair[0])
			if err != nil {
				t.Errorf("Cannot get %s: %s", pairs[0], err)
			}
			if value != pair[1] {
				t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
			}
		}
	})

	outInfo, err := outFile.Stat()
	if err != nil {
		t.Fatal(err)
	}
	size1 := outInfo.Size()

	t.Run("file growth", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
		}
		outInfo, err := outFile.Stat()
		if err != nil {
			t.Fatal(err)
		}
		if size1*2 != outInfo.Size() {
			t.Errorf("Unexpected size (%d vs %d)", size1, outInfo.Size())
		}
	})

	t.Run("new db process", func(t *testing.T) {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db, err = NewDb(dir, 100)
		if err != nil {
			t.Fatal(err)
		}

		for _, pair := range pairs {
			value, err := db.Get(pair[0])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
			if value != pair[1] {
				t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
			}
		}
	})
}

func TestDb_Segmentation(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDb(dir, 85)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	t.Run("should create new file", func(t *testing.T) {
		db.Put("key1", "value1")
		db.Put("key2", "value2")
		db.Put("key3", "value3")
		db.Put("key2", "value5")

		if len(db.segments) != 2 {
			t.Errorf("Something went wrong with segmentation. Expected 2 files, got %d", len(db.segments))
		}
	})

	t.Run("should start segmentation", func(t *testing.T) {
		db.Put("key4", "value4")

		if len(db.segments) != 3 {
			t.Errorf("Something went wrong with segmentation. Expected 3 files, got %d", len(db.segments))
		}

		time.Sleep(2 * time.Second)

		if len(db.segments) != 2 {
			t.Errorf("Something went wrong with segmentation. Expected 2 files, got %d", len(db.segments))
		}
	})

	t.Run("shouldn't store duplicates", func(t *testing.T) {
		file, err := os.Open(db.segments[0].filePath)
		defer file.Close()

		if err != nil {
			t.Error(err)
		}
		inf, _ := file.Stat()
		if inf.Size() != 126 {
			t.Errorf("Something went wrong with segmentation. Expected size 66, got %d", inf.Size())
		}
	})

	t.Run("shouldn't store new values of duplicate keys", func(t *testing.T) {
		value, _ := db.Get("key2")
		if value != "value5" {
			t.Errorf("Something went wrong with segmentation. Expected value value5, got %s", value)
		}
	})
}

func TestDb_Checksum(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := NewDb(dir, 85)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.Put("key1", "value1")

	t.Run("should get value", func(t *testing.T) {
		_, err := db.Get("key1")
		if err != nil {
			t.Errorf("Error occured while getting value")
		}
	})

	file, _ := os.OpenFile(db.outPath, os.O_RDWR, 0o655)
	file.WriteAt([]byte{0x59}, int64(3))
	file.Close()

	t.Run("shouldn't get value", func(t *testing.T) {
		_, err := db.Get("key1")
		if err == nil || !strings.Contains(err.Error(), "SHA1") {
			t.Errorf("No error occured while getting value")
		}
	})

}
