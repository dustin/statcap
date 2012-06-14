package statstore

import (
	"archive/zip"
	"compress/gzip"
	"encoding/json"
	"os"
	"testing"
)

func TestFileStorer(t *testing.T) {
	filename := "testfile.gz"
	defer os.Remove(filename)
	func() {
		fs, err := GetStorer(filename)
		if err != nil {
			t.Fatalf("Error opening storer: %v", err)
		}
		defer fs.Close()

		something := map[string]string{"a": "ayyy"}

		fs.Insert(something)
	}()

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

		obname, _, err = fs.Insert(something)
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
