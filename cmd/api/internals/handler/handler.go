package handler

import (
	"net/http"

	"github.com/adk2004/goDB/cmd/api/internals/service"
)

type DBHandler struct {
	svc service.DBService
}

func NewHandler(svc service.DBService) *DBHandler {
	return &DBHandler{
		svc : svc,
	}
}

func (dbh *DBHandler) Get(w http.ResponseWriter, r *http.Request) {
	
}

func (dbh *DBHandler) Post(w http.ResponseWriter, r *http.Request) {

}

func (dbh *DBHandler) Delete(w http.ResponseWriter, r *http.Request) {

}