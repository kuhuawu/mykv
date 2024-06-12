package db

import (
	"errors"
	"fmt"
	"io"
	kv "kv2"
	"kv2/entry"
	"kv2/index"
	"kv2/storage"
	"os"
	"sort"
	"sync"
)

const fileSuffix = ".dat"

var (
	KeyNotFoundErr   = errors.New("key not found")
	NoNeedToMergeErr = errors.New("no need to merge")
)

type DB struct {
	rw sync.RWMutex
	kd *index.KeyDir
	s  *storage.Storage
}

func NewDB(opt *kv.Options) (db *DB, err error) {
	db = &DB{
		rw: sync.RWMutex{},
		kd: &index.KeyDir{Index: map[string]*index.Index{}},
	}
	if isExist, _ := kv.IsDirExist(opt.Dir); isExist {
		if err := db.Recovery(opt); err != nil {
			return nil, err
		}
		return db, nil
	}
	var fileSize = kv.GetSegmentSize(opt.SegmentSize)
	db.s, err = storage.NewStorage(opt.Dir, fileSize)
	if err != nil {
		return nil, err
	}
	return db, err
}

func (db *DB) Set(key []byte, value []byte) error {
	db.rw.Lock()
	defer db.rw.Unlock()
	entry := entry.NewEntryWithData(key, value)
	buf := entry.Encode()
	index, err := db.s.WriteAt(buf)
	if err != nil {
		return err
	}
	index.KeySize = len(key)
	index.ValueSize = len(value)
	db.kd.Update(string(key), index)
	return nil
}

func (db *DB) Get(key []byte) (value []byte, err error) {
	db.rw.RLock()
	defer db.rw.RUnlock()
	i := db.kd.Find(string(key))
	if i == nil {
		return nil, KeyNotFoundErr
	}
	dataSize := entry.MetaSize + i.KeySize + i.ValueSize
	buf := make([]byte, dataSize)
	entry, err := db.s.ReadFullEntry(i.Fid, i.Off, buf)
	if err != nil {
		return nil, err
	}
	return entry.Value, nil
}

func (db *DB) Delete(key []byte) error {
	db.rw.Lock()
	defer db.rw.Unlock()
	index := db.kd.Find(string(key))
	if index == nil {
		return KeyNotFoundErr
	}
	e := entry.NewEntry()
	e.Meta.Flag = entry.DeleteFlag
	_, err := db.s.WriteAt(e.Encode())
	if err != nil {
		return err
	}
	delete(db.kd.Index, string(key))
	return nil
}

func (db *DB) Merge() error {
	db.rw.Lock()
	defer db.rw.Unlock()
	fids, err := kv.GetFids(db.s.Dir)
	if err != nil {
		return err
	}
	if len(fids) < 2 {
		return NoNeedToMergeErr
	}
	sort.Ints(fids)
	for _, fid := range fids[:len(fids)-1] {
		var off int64 = 0
		for {
			entry, err := db.s.ReadEntry(fid, off)
			if err == nil {
				off += int64(entry.Size())
				oldIndex := db.kd.Index[string(entry.Key)]
				if oldIndex == nil {
					continue
				}
				if oldIndex.Fid == fid && oldIndex.Off == off {
					newIndex, err := db.s.WriteAt(entry.Encode())
					if err != nil {
						return err
					}
					db.kd.Index[string(entry.Key)] = newIndex
				}
			} else {
				if err == io.EOF {
					break
				}
				return err
			}
		}
		err = os.Remove(fmt.Sprintf("%s/%d%s", db.s.Dir, fid, fileSuffix))
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) Recovery(opt *kv.Options) (err error) {
	var fileSize = kv.GetSegmentSize(opt.SegmentSize)
	db.s = &storage.Storage{
		Dir:      opt.Dir,
		FileSize: fileSize,
		Fds:      map[int]*os.File{},
	}
	fids, err := kv.GetFids(opt.Dir)
	if err != nil {
		return err
	}
	sort.Ints(fids)
	for _, fid := range fids {
		var off int64 = 0
		path := fmt.Sprintf("%s/%d%s", opt.Dir, fid, fileSuffix)
		fd, err := os.OpenFile(path, os.O_RDWR, os.ModePerm)
		if err != nil {
			return err
		}
		db.s.Fds[fid] = fd
		for {
			entry, err := db.s.ReadEntry(fid, off)
			if err == nil {
				db.kd.Index[string(entry.Key)] = &index.Index{
					Fid:       fid,
					Off:       off,
					Timestamp: entry.Meta.TimeStamp,
				}
				off += int64(entry.Size())
			} else {
				if err == storage.DeleteEntryErr {
					continue
				}
				if err == io.EOF {
					break
				}
				return err
			}
		}
		if fid == fids[len(fids)-1] {
			af := &storage.ActiveFile{
				Fid: fid,
				F:   fd,
				Off: off,
			}
			db.s.Af = af
		}
	}
	return err
}
