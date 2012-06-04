package statstore

import (
	"compress/gzip"
	"encoding/json"
	"os"
	"testing"
)

func TestFileStorer(t *testing.T) {
	filename := "testfile.gz"
	defer os.Remove(filename)
	func() {
		fs, err := openFileStorer(filename)
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
