package api

import (
	"net/http"
)

type HandleFunc func(http.ResponseWriter, *http.Request)

func (s *Server) RegisterHandleFunc(path string, hf HandleFunc) {
	s.mux.HandleFunc(path, hf)
}

func (s *Server) RegisterHandler(path string, handler http.Handler) {
	s.mux.Handle(path, handler)
}
