package main

import (
	"log"
	"net/http"
	"time"

	"github.com/adk2004/goDB/cmd/api/internals/handler"
	"github.com/adk2004/goDB/cmd/api/internals/service"
	"github.com/adk2004/goDB/cmd/api/router"
	"github.com/adk2004/goDB/db/engine"
)

func main() {
	db_engine, err :=  engine.NewEngine("./db_data", 100)
	if err!= nil {
		log.Fatalf("Failed to initialize db engine %v", err)
	}
	svc := service.NewService(db_engine)
	handler := handler.NewHandler(svc)
	r := router.SetUpRouter(*handler)
	srv := http.Server{
		Addr: ":3000",
		Handler: r,
		ReadTimeout: 5 * time.Second,
		WriteTimeout: 5 * time.Second,
		IdleTimeout: 10 * time.Second,
	}
	log.Println("goDB server started on port 3000")
	if err:= srv.ListenAndServe(); err!= nil {
		log.Fatalf("Server Failed %v", err)
	}
}
