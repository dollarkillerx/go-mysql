package replication

import (
	"encoding/json"
	"fmt"

	"github.com/dgraph-io/badger/v3"
	"github.com/pingcap/errors"
)

type Storage struct {
	Db       *badger.DB
	ServerID string
}

//func (s *Storage) Close() {
//	s.Db.Close()
//}

//func (s *Storage) Empty() error {
//	return s.Db.Update(func(txn *badger.Txn) error {
//		iterator := txn.NewIterator(badger.DefaultIteratorOptions)
//		defer iterator.Close()
//
//		for iterator.Rewind(); iterator.Valid(); iterator.Next() {
//			err := txn.Delete(iterator.Item().Key())
//			if err != nil {
//				return err
//			}
//		}
//
//		return nil
//	})
//}

func (s *Storage) Set(key []byte, val []byte) error {
	return s.Db.Update(func(txn *badger.Txn) error {
		ttl := badger.NewEntry(key, val)

		return txn.SetEntry(ttl)
	})
}

func (s *Storage) Del(key []byte) (err error) {
	return s.Db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
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

func (s *Storage) GetXID(schema []byte, table []byte) string {
	return fmt.Sprintf("schema.mapping.%s.%s.%s", s.ServerID, schema, table)
}

func (s *Storage) GetTableID(tableID uint64) string {
	return fmt.Sprintf("schema.%s.%d", s.ServerID, tableID)
}

func (s *Storage) SetTableMapEvent(xid string, val TableMapEvent) error {
	mr, err := json.Marshal(val)
	if err != nil {
		return err
	}

	// 检测是否存在
	old, err := s.Get(xid)
	if err == nil {
		err := s.Del(old)
		if err != nil {
			fmt.Println(err)
		}
	}

	newID := s.GetTableID(val.TableID)
	// 二层映射
	// 第一层: schema.mapping.server_id.schema.table
	// 第二层: schema.server_id.table_id

	err = s.Set([]byte(xid), []byte(newID))
	if err != nil {
		fmt.Println(err)
		return errors.WithStack(err)
	}

	return s.Set([]byte(newID), mr)
}

func (s *Storage) GetUpdateID(schema []byte, table []byte) string {
	return fmt.Sprintf("update_schema_%s.%s", schema, table)
}

func (s *Storage) GetUpdateSignal(updateID string) bool {
	get, err := s.Get(updateID)
	if err != nil {
		return false
	}

	switch string(get) {
	case "True":
		return true
	//case "False":
	//	return false
	default:
		return false
	}
}

func (s *Storage) SetUpdateSignal(updateID string, signal string) error {
	return s.Set([]byte(updateID), []byte(signal))
}

func (s *Storage) GetTableMapEvent(xid string) (*TableMapEvent, error) {
	get, err := s.Get(xid)
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
