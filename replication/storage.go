package replication

import (
	"encoding/json"
	"fmt"

	"github.com/dgraph-io/badger/v3"
	"github.com/pingcap/errors"
)

type Storage struct {
	Db *badger.DB
}

func (s *Storage) Close() {
	s.Db.Close()
}

func (s *Storage) Empty() error {
	return s.Db.Update(func(txn *badger.Txn) error {
		iterator := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iterator.Close()

		for iterator.Rewind(); iterator.Valid(); iterator.Next() {
			err := txn.Delete(iterator.Item().Key())
			if err != nil {
				return err
			}
		}

		return nil
	})
}

func (s *Storage) Set(key []byte, val []byte) error {
	return s.Db.Update(func(txn *badger.Txn) error {
		ttl := badger.NewEntry(key, val)

		return txn.SetEntry(ttl)
	})
}

func (s *Storage) Get(key string) (value []byte, err error) {
	return value, s.Db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			if val == nil {
				return errors.New("val is nil")
			}

			value = val
			return nil
		})
	})
}

func (s *Storage) SetTableMapEvent(key uint64, val TableMapEvent) error {
	ks := fmt.Sprintf("%d", key)
	mr, err := json.Marshal(val)
	if err != nil {
		return err
	}

	return s.Set([]byte(ks), mr)
}

func (s *Storage) GetTableMapEvent(key uint64) (*TableMapEvent, error) {
	ks := fmt.Sprintf("%d", key)
	get, err := s.Get(ks)
	if err != nil {
		return nil, err
	}

	var result TableMapEvent

	err = json.Unmarshal(get, &result)
	if err != nil {
		return nil, err
	}

	return &result, nil
}

func (s *Storage) Test() {
	err := s.Db.View(func(txn *badger.Txn) error {
		iterator := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iterator.Close()

		for iterator.Rewind(); iterator.Valid(); iterator.Next() {
			fmt.Println("iterator.Item().Key():  ", string(iterator.Item().Key()))
		}

		return nil
	})
	if err != nil {
		fmt.Println(err, "           TEST .........")
	}

	fmt.Println("Test Over")
}
