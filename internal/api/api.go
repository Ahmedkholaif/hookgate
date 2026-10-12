// Package api is hookgate's HTTP surface: webhook ingest plus the admin and
// status-tracking API. hookgate-nest serves the identical contract.
package api

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Ahmedkholaif/hookgate/internal/signature"
	"github.com/Ahmedkholaif/hookgate/internal/store"
)

const (
	maxBody            = 1 << 20 // 1 MiB
	signatureTolerance = 5 * time.Minute
)

type Config struct {
	EventsTopic string
	APIKey      string // optional; protects /v1/* except ingest
	Actor       string // status_log actor, e.g. "go/api"
}

type Server struct {
	st  *store.Store
	cfg Config
	log *slog.Logger
	now func() time.Time
}

func New(st *store.Store, cfg Config, log *slog.Logger) *Server {
	return &Server{st: st, cfg: cfg, log: log, now: time.Now}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("POST /v1/ingest/{source}", s.ingest)

	admin := http.NewServeMux()
	admin.HandleFunc("GET /v1/stats", s.stats)
	admin.HandleFunc("POST /v1/sources", s.createSource)
	admin.HandleFunc("GET /v1/sources", s.listSources)
	admin.HandleFunc("GET /v1/sources/{name}", s.getSource)
	admin.HandleFunc("POST /v1/routes", s.createRoute)
	admin.HandleFunc("GET /v1/routes", s.listRoutes)
	admin.HandleFunc("DELETE /v1/routes/{id}", s.deleteRoute)
	admin.HandleFunc("GET /v1/events", s.listEvents)
	admin.HandleFunc("GET /v1/events/{id}", s.getEvent)
	admin.HandleFunc("POST /v1/events/{id}/replay", s.replay)
	admin.HandleFunc("GET /v1/messages/{id}", s.getMessage)
	mux.Handle("/v1/", s.requireKey(admin))
	return mux
}

// HealthOnly serves just /healthz, for processes that run only workers.
func HealthOnly(st *store.Store) http.Handler {
	s := &Server{st: st}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	return mux
}

func (s *Server) requireKey(next http.Handler) http.Handler {
	if s.cfg.APIKey == "" {
		return next
	}
	want := []byte("Bearer " + s.cfg.APIKey)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), want) != 1 {
			writeError(w, http.StatusUnauthorized, "missing or invalid API key")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	if err := s.st.Ping(r.Context()); err != nil {
		writeError(w, http.StatusServiceUnavailable, "database unavailable")
		return
	}
	w.Write([]byte("ok"))
}

// ingest accepts a webhook: verify the signature, derive the dedup key and
// event type, then store the event and its outbox row atomically. It never
// talks to the broker, so a broker outage doesn't fail the provider's call.
func (s *Server) ingest(w http.ResponseWriter, r *http.Request) {
	src, err := s.st.GetSource(r.Context(), r.PathValue("source"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err != nil {
		writeError(w, http.StatusRequestEntityTooLarge, "body exceeds 1 MiB")
		return
	}
	if err := signature.Verify(src.Secret, r.Header.Get(src.SignatureHeader), body, signatureTolerance, s.now()); err != nil {
		writeError(w, http.StatusUnauthorized, "invalid signature: "+strings.TrimPrefix(err.Error(), "signature: "))
		return
	}
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(body, &payload); err != nil {
		writeError(w, http.StatusBadRequest, "body must be a JSON object")
		return
	}
	res, err := s.st.Ingest(r.Context(), src.Name, DedupKey(r.Header.Get(src.IDHeader), body),
		EventType(payload, src.TypeField), body, s.cfg.EventsTopic, s.cfg.Actor)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	code := http.StatusAccepted
	if res.Duplicate {
		code = http.StatusOK
	}
	writeJSON(w, code, res)
}

// DedupKey is the provider's event id when it sends one, else a hash of the
// body (so byte-identical retries still dedupe).
func DedupKey(headerID string, body []byte) string {
	if id := strings.TrimSpace(headerID); id != "" && len(id) <= 200 {
		return "id:" + id
	}
	sum := sha256.Sum256(body)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// EventType reads a dotted path such as "type" or "data.object.type" from
// the payload, falling back to "unknown".
func EventType(payload map[string]json.RawMessage, path string) string {
	parts := strings.Split(path, ".")
	cur := payload
	for i, p := range parts {
		raw, ok := cur[p]
		if !ok {
			return "unknown"
		}
		if i == len(parts)-1 {
			var t string
			if json.Unmarshal(raw, &t) != nil || t == "" || len(t) > 200 {
				return "unknown"
			}
			return t
		}
		cur = nil
		if json.Unmarshal(raw, &cur) != nil {
			return "unknown"
		}
	}
	return "unknown"
}

func (s *Server) stats(w http.ResponseWriter, r *http.Request) {
	st, err := s.st.Stats(r.Context())
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

func (s *Server) createSource(w http.ResponseWriter, r *http.Request) {
	var in store.Source
	if !decode(w, r, &in) {
		return
	}
	src, err := s.st.CreateSource(r.Context(), in)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, src) // the only response that includes the secret
}

func (s *Server) listSources(w http.ResponseWriter, r *http.Request) {
	srcs, err := s.st.ListSources(r.Context())
	if err != nil {
		s.fail(w, r, err)
		return
	}
	for i := range srcs {
		srcs[i].Secret = ""
	}
	writeJSON(w, http.StatusOK, map[string]any{"sources": srcs})
}

func (s *Server) getSource(w http.ResponseWriter, r *http.Request) {
	src, err := s.st.GetSource(r.Context(), r.PathValue("name"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	src.Secret = ""
	writeJSON(w, http.StatusOK, src)
}

func (s *Server) createRoute(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Source    string `json:"source"`
		EventType string `json:"event_type"`
		Topic     string `json:"topic"`
	}
	if !decode(w, r, &in) {
		return
	}
	rt, err := s.st.CreateRoute(r.Context(), in.Source, in.EventType, in.Topic)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, rt)
}

func (s *Server) listRoutes(w http.ResponseWriter, r *http.Request) {
	rts, err := s.st.ListRoutes(r.Context(), r.URL.Query().Get("source"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"routes": rts})
}

func (s *Server) deleteRoute(w http.ResponseWriter, r *http.Request) {
	if err := s.st.DeleteRoute(r.Context(), r.PathValue("id")); err != nil {
		s.fail(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) listEvents(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	evs, err := s.st.ListEvents(r.Context(), store.EventFilter{Status: q.Get("status"), Source: q.Get("source"), Type: q.Get("type"), Limit: limit})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": evs})
}

func (s *Server) getEvent(w http.ResponseWriter, r *http.Request) {
	ev, err := s.st.GetEvent(r.Context(), r.PathValue("id"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, ev)
}

func (s *Server) replay(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.st.Replay(r.Context(), id, s.cfg.EventsTopic, s.cfg.Actor); err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"id": id, "status": "retrying"})
}

func (s *Server) getMessage(w http.ResponseWriter, r *http.Request) {
	m, err := s.st.GetMessage(r.Context(), r.PathValue("id"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, m)
}

// ---------------------------------------------------------------- helpers

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return false
	}
	return true
}

func (s *Server) fail(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, store.ErrInvalid):
		writeError(w, http.StatusBadRequest, cause(err))
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, cause(err))
	case errors.Is(err, store.ErrConflict):
		writeError(w, http.StatusConflict, cause(err))
	default:
		s.log.Error("request failed", "method", r.Method, "path", r.URL.Path, "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

// cause returns the detail joined onto a sentinel error, or the sentinel itself.
func cause(err error) string {
	if j, ok := err.(interface{ Unwrap() []error }); ok {
		if errs := j.Unwrap(); len(errs) > 1 {
			return errs[len(errs)-1].Error()
		}
	}
	return err.Error()
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
