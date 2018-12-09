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
	"gopkg.in/src-d/go-git.v4/storage"
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
	//memory.ModuleStorage

	module string
	db     *sql.DB
}

func NewStorage() (*Storage, error) {
	db, err := sql.Open("sqlite3", "./cit.db")
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS shallow (module TEXT, hash TEXT, PRIMARY KEY (module, hash))`)
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS reference (module TEXT, name TEXT, type TEXT, hash TEXT, target TEXT, PRIMARY KEY(module, name))`)
	if err != nil {
		return nil, err
	}

	return NewModuleStorage(db, "")
}

func NewModuleStorage(db *sql.DB, module string) (*Storage, error) {
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
		//ModuleStorage: make(memory.ModuleStorage),
		module:        module,
		db:            db,
	}, nil
}

func (s *Storage) Close() error {
	return s.db.Close()
}

func (s *Storage) SetShallow(commits []plumbing.Hash) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.Exec(`DELETE FROM shallow WHERE module=?`, s.module)
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(`INSERT OR REPLACE INTO shallow (module, hash) VALUES (?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, c := range commits {
		_, err = stmt.Exec(s.module, hash2str(c))
		if err != nil {
			return err
		}
	}

	tx.Commit()

	return nil
}

func (s *Storage) Shallow() ([]plumbing.Hash, error) {
	rows, err := s.db.Query(`SELECT hash FROM shallow WHERE module=?`, s.module)
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

func (s *Storage) SetReference(ref *plumbing.Reference) error {
	typ := "invalid"
	if ref.Type() == plumbing.HashReference {
		typ = "hash"
	} else if ref.Type() == plumbing.SymbolicReference {
		typ = "symbol"
	}

	_, err := s.db.Exec(`INSERT OR REPLACE INTO reference (module, name, type, hash, target) VALUES (?, ?, ?, ?, ?)`, s.module, ref.Name().String(), typ, hash2str(ref.Hash()), ref.Target().String())
	return err
}

func (s *Storage) CheckAndSetReference(new_, old *plumbing.Reference) error {
	if old != nil {
		ref, err := s.Reference(old.Name())
		if err != nil {
			return err
		}
		if ref.Hash() != old.Hash() {
			return fmt.Errorf("reference has changed concurrently")
		}
	}
	return s.SetReference(new_)
}

func (s *Storage) Reference(name plumbing.ReferenceName) (*plumbing.Reference, error) {
	rows, err := s.db.Query(`SELECT name, type, hash, target FROM reference WHERE module=? AND name=?`, s.module, name.String())
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

func (s *Storage) IterReferences() (storer.ReferenceIter, error) {
	rows, err := s.db.Query(`SELECT name, type, hash, target FROM reference WHERE module=?`, s.module)
	return (*ReferenceIter)(rows), err
}

func (s *Storage) RemoveReference(name plumbing.ReferenceName) error {
	_, err := s.db.Exec(`DELETE FROM reference WHERE module=? AND name=?`, s.module, name.String())
	return err
}

func (s *Storage) CountLooseRefs() (int, error) {
	row := s.db.QueryRow(`SELECT COUNT(name) FROM reference WHERE module=?`, s.module)
	if row == nil {
		return 0, fmt.Errorf("something wrong")
	}
	var i int
	if err := row.Scan(&i); err != nil {
		return 0, err
	}
	return i, nil
}

func (s *Storage) PackRefs() error {
	return nil
}

func (s *Storage) Module(name string) (storage.Storer, error) {
	return NewModuleStorage(s.db, name);
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
