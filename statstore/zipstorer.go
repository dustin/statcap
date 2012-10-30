package statstore

import (
	"archive/zip"
	"encoding/binary"
	"encoding/json"
	"io"
	"os"
	"sort"
	"sync"
	"time"
)

const timeFormat = "20060102T150405.000.json"

const timeTag = uint16(0x23)

type zipStorer struct {
	lock sync.Mutex
	file *os.File
	z    *zip.Writer
}

func (z *zipStorer) Insert(ob StoredItem) (string, string, error) {
	z.lock.Lock()
	defer z.lock.Unlock()

	ts := ob.Timestamp()
	filename := ts.Format(timeFormat)

	tagts := []byte(ts.Format(time.RFC3339Nano))
	tag := []byte{0, 0, 0, 0}

	binary.LittleEndian.PutUint16(tag[0:2], timeTag)
	binary.LittleEndian.PutUint16(tag[2:4], uint16(len(tagts)))

	h := zip.FileHeader{
		Name:   filename,
		Method: zip.Deflate,
		Extra:  append(tag, tagts...),
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
	return f[i].Comment < f[j].Comment
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

func (z *ZipReader) Next() (StoredItem, error) {
	rv := StoredItem{}

	if z.current >= len(z.files) {
		return rv, io.EOF
	}

	defer func() {
		z.current++
	}()

	r, err := z.files[z.current].Open()
	if err != nil {
		return rv, err
	}
	defer r.Close()

	err = json.NewDecoder(r).Decode(&rv)
	if err != nil {
		return rv, err
	}

	zf := z.files[z.current]
	if len(zf.Extra) > 0 {
		b := zf.Extra
		for len(b) >= 4 {
			tag := binary.LittleEndian.Uint16(b[:2])
			b = b[2:]
			size := binary.LittleEndian.Uint16(b[:2])
			b = b[2:]
			if tag == timeTag {
				var ts time.Time
				ts, err = time.Parse(time.RFC3339,
					string(b[:size]))
				rv.ts = &ts
			}
			b = b[size:]
		}
	}

	return rv, err
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
