package main

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"os"

	_ "github.com/mattn/go-sqlite3"
	"gopkg.in/src-d/go-billy.v4/osfs"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/plumbing/storer"
	"gopkg.in/src-d/go-git.v4/storage/memory"
)

func hash2str(hash plumbing.Hash) string {
	return hex.EncodeToString(hash[:])
}

func str2hash(str string) (plumbing.Hash, error) {
	var h plumbing.Hash
	_, err := hex.Decode(h[:], []byte(str))
	return h, err
}

type Storage struct {
	memory.ConfigStorage
	memory.ObjectStorage
	//memory.ShallowStorage
	memory.IndexStorage
	//memory.ReferenceStorage
	memory.ModuleStorage

	db *sql.DB
}

func NewStorage() (*Storage, error) {
	db, err := sql.Open("sqlite3", "./cit.db")
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS shallow (hash TEXT PRIMARY KEY)`)
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS reference (name TEXT PRIMARY KEY, type TEXT, hash TEXT, target TEXT)`)
	if err != nil {
		return nil, err
	}

	return &Storage{
		//ReferenceStorage: make(memory.ReferenceStorage),
		ConfigStorage: memory.ConfigStorage{},
		//ShallowStorage:   memory.ShallowStorage{},
		ObjectStorage: memory.ObjectStorage{
			Objects: make(map[plumbing.Hash]plumbing.EncodedObject),
			Commits: make(map[plumbing.Hash]plumbing.EncodedObject),
			Trees:   make(map[plumbing.Hash]plumbing.EncodedObject),
			Blobs:   make(map[plumbing.Hash]plumbing.EncodedObject),
			Tags:    make(map[plumbing.Hash]plumbing.EncodedObject),
		},
		ModuleStorage: make(memory.ModuleStorage),
		db:            db,
	}, nil
}

func (storage *Storage) Close() error {
	return storage.db.Close()
}

func (storage *Storage) SetShallow(commits []plumbing.Hash) error {
	tx, err := storage.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.Exec(`DELETE FROM shallow`)
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(`INSERT OR REPLACE INTO shallow VALUES (?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, c := range commits {
		_, err = stmt.Exec(hash2str(c))
		if err != nil {
			return err
		}
	}

	tx.Commit()

	return nil
}

func (storage *Storage) Shallow() ([]plumbing.Hash, error) {
	rows, err := storage.db.Query(`SELECT * FROM shallow`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []plumbing.Hash
	for rows.Next() {
		var s string
		if err = rows.Scan(&s); err != nil {
			return nil, err
		}

		h, err := str2hash(s)
		if err != nil {
			return nil, err
		}

		result = append(result, h)
	}

	return result, nil
}

func (storage *Storage) SetReference(ref *plumbing.Reference) error {
	typ := "invalid"
	if ref.Type() == plumbing.HashReference {
		typ = "hash"
	} else if ref.Type() == plumbing.SymbolicReference {
		typ = "symbol"
	}

	_, err := storage.db.Exec(`INSERT OR REPLACE INTO reference VALUES (?, ?, ?, ?)`, ref.Name().String(), typ, hash2str(ref.Hash()), ref.Target().String())
	return err
}

func (storage *Storage) CheckAndSetReference(new_, old *plumbing.Reference) error {
	if old != nil {
		ref, err := storage.Reference(old.Name())
		if err != nil {
			return err
		}
		if ref.Hash() != old.Hash() {
			return fmt.Errorf("reference has changed concurrently")
		}
	}
	return storage.SetReference(new_)
}

func (storage *Storage) Reference(name plumbing.ReferenceName) (*plumbing.Reference, error) {
	rows, err := storage.db.Query(`SELECT * FROM reference WHERE name=?`, name.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	ref, err := (*ReferenceIter)(rows).Next()
	if err == io.EOF {
		return nil, plumbing.ErrReferenceNotFound
	}

	return ref, err
}

type ReferenceIter sql.Rows

func (r *ReferenceIter) Close() {
	r.Close()
}

func (r *ReferenceIter) Next() (*plumbing.Reference, error) {
	if !(*sql.Rows)(r).Next() {
		return nil, io.EOF
	}

	var name, typ, hash, target string
	if err := (*sql.Rows)(r).Scan(&name, &typ, &hash, &target); err != nil {
		return nil, err
	}

	if typ == "hash" {
		h, err := str2hash(hash)
		if err != nil {
			return nil, err
		}
		return plumbing.NewHashReference(plumbing.ReferenceName(name), h), nil
	} else if typ == "symbol" {
		return plumbing.NewSymbolicReference(plumbing.ReferenceName(name), plumbing.ReferenceName(target)), nil
	} else {
		return nil, plumbing.ErrInvalidType
	}
}

func (r *ReferenceIter) ForEach(cb func(*plumbing.Reference) error) error {
	for {
		ref, err := r.Next()
		if err == io.EOF {
			return nil
		}

		if err = cb(ref); err != nil {
			return err
		}
	}
}

func (storage *Storage) IterReferences() (storer.ReferenceIter, error) {
	rows, err := storage.db.Query(`SELECT * FROM reference`)
	return (*ReferenceIter)(rows), err
}

func (storage *Storage) RemoveReference(name plumbing.ReferenceName) error {
	_, err := storage.db.Exec(`DELETE FROM reference WHERE name=?`, name.String())
	return err
}

func (storage *Storage) CountLooseRefs() (int, error) {
	row := storage.db.QueryRow(`SELECT COUNT(name) FROM reference`)
	if row == nil {
		return 0, fmt.Errorf("something wrong")
	}
	var i int
	if err := row.Scan(&i); err != nil {
		return 0, err
	}
	return i, nil
}

func (storage *Storage) PackRefs() error {
	return nil
}

func main() {
	storage, err := NewStorage()
	if err != nil {
		panic(err.Error())
	}
	defer storage.Close()
	fs := osfs.New("./test")

	fmt.Println("clone")
	repo, err := git.Clone(storage, fs, &git.CloneOptions{
		URL: "https://github.com/macrat/updns",
		//URL:      "https://github.com/torvalds/linux",
		Depth:    1,
		Progress: os.Stdout,
	})
	if err != nil {
		panic(err.Error())
	}

	fmt.Println("get")
	commits, err := repo.CommitObjects()
	if err != nil {
		panic(err.Error())
	}

	fmt.Println("print")
	fmt.Println("=====")
	err = commits.ForEach(func(c *object.Commit) error {
		fmt.Println(c.Message)
		return nil
	})
	if err != nil {
		panic(err.Error())
	}
}
