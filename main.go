package main

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"

	_ "github.com/mattn/go-sqlite3"
	"gopkg.in/src-d/go-billy.v4/osfs"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/storage/memory"
)

type Storage struct {
	memory.ConfigStorage
	memory.ObjectStorage
	//memory.ShallowStorage
	memory.IndexStorage
	memory.ReferenceStorage
	memory.ModuleStorage

	db *sql.DB
}

func NewStorage() (*Storage, error) {
	db, err := sql.Open("sqlite3", "./cit.db")
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS shallow (hash BLOB PRIMARY KEY)`)
	if err != nil {
		return nil, err
	}

	return &Storage{
		ReferenceStorage: make(memory.ReferenceStorage),
		ConfigStorage:    memory.ConfigStorage{},
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

	stmt, err := tx.Prepare(`INSERT INTO shallow VALUES (?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, c := range commits {
		_, err = stmt.Exec(hex.EncodeToString(c[:]))
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

	var result []plumbing.Hash
	for rows.Next() {
		var s string
		rows.Scan(&s)

		var h plumbing.Hash
		_, err := hex.Decode(h[:], []byte(s))
		if err != nil {
			return nil, err
		}

		result = append(result, h)
	}

	return result, nil
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
		//URL:      "https://github.com/macrat/updns",
		URL:      "https://github.com/torvalds/linux",
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
	err = commits.ForEach(func(c *object.Commit) error {
		fmt.Println(c.Message)
		return nil
	})
	if err != nil {
		panic(err.Error())
	}
}
