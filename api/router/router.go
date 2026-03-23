package router

import (
	"net/http"

	"github.com/adk2004/goDB/api/internals/handler"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

func SetUpRouter(handler handler.DBHandler) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.RealIP)

	// db route
	r.Route("/db", func (u chi.Router) {
		u.Get("", handler.Get)
		u.Post("", handler.Put)
		u.Delete("", handler.Delete)
	})
	// health route
	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status": "ok"}`))
	})
	return r
}
