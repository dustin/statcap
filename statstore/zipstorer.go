package statstore

import (
	"archive/zip"
	"encoding/json"
	"io"
	"os"
	"sort"
	"sync"
	"time"
)

const timeFormat = "20060102T150405.json"

type zipStorer struct {
	lock sync.Mutex
	file *os.File
	z    *zip.Writer
}

func (z *zipStorer) Insert(ob interface{}, ts time.Time) (string, string, error) {
	z.lock.Lock()
	defer z.lock.Unlock()

	filename := ts.Format(timeFormat)

	h := zip.FileHeader{
		Name:   filename,
		Method: zip.Deflate,
	}
	h.SetModTime(ts)

	f, err := z.z.CreateHeader(&h)
	if err != nil {
		return "", "", err
	}

	err = json.NewEncoder(f).Encode(ob)
	if err != nil {
		return "", "", err
	}

	return filename, "", nil
}

func (z *zipStorer) Close() error {
	z.lock.Lock()
	defer z.lock.Unlock()
	defer z.file.Close()
	return z.z.Close()
}

func openZipStorer(filepath string) (*zipStorer, error) {
	f, err := os.Create(filepath)
	if err != nil {
		return nil, err
	}
	z := zip.NewWriter(f)
	return &zipStorer{
		file: f,
		z:    z,
	}, nil
}

type fileList []*zip.File

func (f fileList) Len() int {
	return len(f)
}

func (f fileList) Less(i, j int) bool {
	return f[i].ModTime().Before(f[j].ModTime())
}

func (f fileList) Swap(i, j int) {
	f[j], f[i] = f[i], f[j]
}

type ZipReader struct {
	z *zip.ReadCloser

	files   fileList
	current int
}

func (z *ZipReader) Close() error {
	return z.z.Close()
}

func (z *ZipReader) Next() (interface{}, time.Time, error) {
	if z.current >= len(z.files) {
		return nil, time.Time{}, io.EOF
	}

	defer func() {
		z.current++
	}()

	r, err := z.files[z.current].Open()
	if err != nil {
		return nil, time.Time{}, err
	}
	defer r.Close()

	var rv interface{}
	err = json.NewDecoder(r).Decode(&rv)

	return rv, z.files[z.current].ModTime(), nil
}

func openZipReader(filepath string) (*ZipReader, error) {
	f, err := zip.OpenReader(filepath)
	if err != nil {
		return nil, err
	}

	files := make(fileList, len(f.File))
	copy(files, f.File)
	sort.Sort(files)

	return &ZipReader{
		z:     f,
		files: files,
	}, nil
}
