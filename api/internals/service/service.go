package service

import (
	"github.com/adk2004/goDB/db/engine"
	"github.com/adk2004/goDB/db/types"
)

type DBService struct {
	eng engine.Engine
}

func NewService(db_engine engine.Engine) DBService {
	return DBService{
		eng : db_engine,
	}
}

func (svc *DBService) Get(key types.Key) (types.Value, error) {
	return svc.Get(key)
}

func (svc *DBService) Put(key types.Key, val types.Value) (error) {
	return svc.eng.Put(key, val)
}

func (svc *DBService) Delete(key types.Key) (error) {
	return svc.eng.Delete(key)
}