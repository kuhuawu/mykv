package storage

import (
	"errors"
	"fmt"
	"kv2/entry"
	"kv2/index"
	"os"
)

const MetaSize = 29

var (
	readMissDataErr  = errors.New("miss data during read")
	writeMissDataErr = errors.New("miss data during write")
	crcErr           = errors.New("crc error")
	DeleteEntryErr   = errors.New("read an entry which had deleted")
)

const (
	fileSuffix = ".dat"
	B          = 1
	KB         = 1024 * B
	MB         = 1024 * KB
	GB         = 1024 * MB
)

type Storage struct {
	Dir      string
	FileSize int64
	Af       *ActiveFile
	Fds      map[int]*os.File
}

func NewStorage(dir string, size int64) (s *Storage, err error) {
	err = os.Mkdir(dir, os.ModePerm)
	if err != nil {
		return nil, err
	}
	s = &Storage{
		Dir:      dir,
		FileSize: size,
		Fds:      map[int]*os.File{},
	}
	s.Dir = dir
	s.Af = &ActiveFile{
		Fid: 0,
		Off: 0,
	}
	path := s.getPath()
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, err
	}
	s.Af.F = fd
	s.Fds[0] = fd
	return s, nil
}

type ActiveFile struct {
	Fid int
	F   *os.File
	Off int64
}

func (s *Storage) ReadEntry(fid int, off int64) (e *entry.Entry, err error) {
	buf := make([]byte, entry.MetaSize)
	err = s.readAt(fid, off, buf)
	if err != nil {
		return nil, err
	}
	e = entry.NewEntry()
	e.DecodeMeta(buf)
	if e.Meta.Flag == entry.DeleteFlag {
		return nil, DeleteEntryErr
	}
	off += entry.MetaSize
	payloadSize := e.Meta.KeySize + e.Meta.ValueSize
	payload := make([]byte, payloadSize)
	err = s.readAt(fid, off, payload)
	if err != nil {
		return nil, err
	}
	err = e.DecodePayload(payload)
	if err != nil {
		return nil, err
	}
	crc := e.GetCrc(buf)
	if e.Meta.Crc != crc {
		return nil, crcErr
	}
	return e, nil
}

func (s *Storage) ReadFullEntry(fid int, off int64, buf []byte) (e *entry.Entry, err error) {
	err = s.readAt(fid, off, buf[:MetaSize])
	if err != nil {
		return nil, err
	}

	e = entry.NewEntry()
	e.DecodeMeta(buf[:MetaSize])

	if e.Meta.Flag == entry.DeleteFlag {
		return nil, DeleteEntryErr
	}

	off += MetaSize
	payloadSize := uint32(e.Meta.KeySize + e.Meta.ValueSize)
	if payloadSize > uint32(len(buf[MetaSize:])) {
		return nil, errors.New("buffer too small for payload")
	}

	err = s.readAt(fid, off, buf[MetaSize:])
	if err != nil {
		return nil, err
	}

	err = e.DecodePayload(buf[MetaSize : MetaSize+int(payloadSize)])
	if err != nil {
		return nil, err
	}

	crc := e.GetCrc(buf[:MetaSize])
	if e.Meta.Crc != crc {
		return nil, crcErr
	}

	return e, nil
}

func (s *Storage) readAt(fid int, off int64, bytes []byte) (err error) {
	if fd := s.Fds[fid]; fd != nil {
		n, err := fd.ReadAt(bytes, off)
		if err != nil {
			return err
		}
		if n < len(bytes) {
			return readMissDataErr
		}
		return nil
	}
	path := fmt.Sprintf("%s/%d", s.Dir, fid)
	fd, err := os.OpenFile(path, os.O_RDWR, os.ModePerm)
	s.Fds[fid] = fd
	n, err := fd.ReadAt(bytes, off)
	if err != nil {
		return err
	}
	if n < len(bytes) {
		return readMissDataErr
	}
	return nil
}

func (s *Storage) WriteAt(bytes []byte) (i *index.Index, err error) {
	err = s.Af.writeAt(bytes)
	if err != nil {
		return nil, err
	}
	i = &index.Index{
		Fid: s.Af.Fid,
		Off: s.Af.Off,
	}
	s.Af.Off += int64(len(bytes))
	if s.Af.Off >= s.FileSize {
		err := s.rotate()
		if err != nil {
			return nil, err
		}
	}
	return i, nil
}

func (af *ActiveFile) writeAt(bytes []byte) error {
	n, err := af.F.WriteAt(bytes, af.Off)
	if n < len(bytes) {
		return writeMissDataErr
	}
	return err
}

func (s *Storage) rotate() error {
	af := &ActiveFile{
		Fid: s.Af.Fid + 1,
		Off: 0,
	}
	fd, err := os.OpenFile(s.getPath(), os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return err
	}
	af.F = fd
	s.Fds[af.Fid] = fd
	s.Af = af
	return nil
}

func (s *Storage) getPath() string {
	path := fmt.Sprintf("%s/%d%s", s.Dir, s.Af.Fid, fileSuffix)
	return path
}
