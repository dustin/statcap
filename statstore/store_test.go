package statstore

import (
	"archive/zip"
	"compress/gzip"
	"encoding/json"
	"io"
	"os"
	"testing"
	"time"
)

const basetimeSecs = 1339646554

var basetime time.Time = time.Unix(basetimeSecs, 0)

func TestFileStorer(t *testing.T) {
	filename := "testfile.gz"

	defer os.Remove(filename)
	initData(t, filename)

	f, err := os.Open(filename)
	if err != nil {
		t.Fatalf("Error reopening the file: %v", err)
	}
	defer f.Close()
	z, err := gzip.NewReader(f)
	if err != nil {
		t.Fatalf("Error opening z reader: %v", err)
	}
	d := json.NewDecoder(z)

	m := map[string]string{}
	err = d.Decode(&m)
	if m["a"] != "ayyy" {
		t.Fatalf("Didn't round trip through disk: %v", m)
	}
}

func verifyStorerReader(t *testing.T, filename string) {
	defer os.Remove(filename)
	obname := initData(t, filename)

	verify(t, obname, filename)
}

func TestFileStorerReader(t *testing.T) {
	verifyStorerReader(t, "testingfile.gz")
}

func TestZipFileStorer(t *testing.T) {
	filename := "testfile.zip"
	var obname string
	defer os.Remove(filename)
	func() {
		fs, err := GetStorer(filename)
		if err != nil {
			t.Fatalf("Error opening storer: %v", err)
		}
		defer fs.Close()

		something := map[string]string{"a": "ayyy"}

		obname, _, err = fs.Insert(something, basetime)
		if err != nil {
			t.Fatalf("Error storing item: %v", err)
		}
	}()

	f, err := zip.OpenReader(filename)
	if err != nil {
		t.Fatalf("Error reopening the file: %v", err)
	}
	defer f.Close()

	var zf *zip.File
	for _, f := range f.File {
		if f.Name == obname {
			zf = f
		}
	}

	r, err := zf.Open()
	if err != nil {
		t.Fatalf("Error reading zip contents: %v", err)
	}
	defer r.Close()

	d := json.NewDecoder(r)

	m := map[string]string{}
	err = d.Decode(&m)
	if m["a"] != "ayyy" {
		t.Fatalf("Didn't round trip through disk: %v", m)
	}
}

func initData(t *testing.T, filename string) string {
	fs, err := GetStorer(filename)
	if err != nil {
		t.Fatalf("Error opening storer: %v", err)
	}
	defer fs.Close()

	something := map[string]interface{}{"a": "ayyy"}

	obname, _, err := fs.Insert(something, basetime)
	if err != nil {
		t.Fatalf("Error storing item: %v", err)
	}

	return obname
}

func verify(t *testing.T, obname, filename string) {
	zr, err := GetStoreReader(filename)
	if err != nil {
		t.Fatalf("Error reopening the file: %v", err)
	}
	defer zr.Close()

	r, ts, err := zr.Next()
	if err != nil {
		t.Fatalf("Error reading an item: %v", err)
	}
	if ts.Unix() != basetime.Unix() {
		t.Fatalf("Expected ts %v, got %v", basetime, ts)
	}

	m := r.(map[string]interface{})
	if m["a"] != "ayyy" {
		t.Fatalf("Didn't round trip through disk: %v", m)
	}

	_, _, err = zr.Next()
	if err != io.EOF {
		t.Fatalf("Expected EOF, got: %v", err)
	}

}

func TestZipFileStorerReader(t *testing.T) {
	verifyStorerReader(t, "testingfile.zip")
}
