// Licensed under the Apache License, Version 2.0.

package protocol

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// Change only the server's read-only confirmation, retaining the normal fake's
// authentication, session eviction, and statement routing.
func readOnlyReplies(f *fake, change func(string, map[string]any)) *atomic.Int32 {
	deleted := &atomic.Int32{}
	f.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/chargewire/v1/sessions/") {
			deleted.Add(1)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if (r.Method == http.MethodPost && r.URL.Path == "/chargewire/v1/sessions") || r.Method == http.MethodPatch {
			rec := httptest.NewRecorder()
			f.serve(rec, r)
			var doc map[string]any
			if rec.Code < 300 && json.Unmarshal(rec.Body.Bytes(), &doc) == nil {
				change(r.Method, doc)
				writeJSON(w, rec.Code, doc)
				return
			}
			for k, v := range rec.Header() {
				w.Header()[k] = v
			}
			w.WriteHeader(rec.Code)
			_, _ = w.Write(rec.Body.Bytes())
			return
		}
		f.serve(w, r)
	})
	return deleted
}

func TestReadOnlyCreationRequiresBooleanConfirmationBeforeHook(t *testing.T) {
	for _, confirmation := range []any{nil, false, "true", true} {
		t.Run(fmtValue(confirmation), func(t *testing.T) {
			f := newFake(t).withSessions()
			deleted := readOnlyReplies(f, func(_ string, doc map[string]any) {
				doc["options"] = map[string]any{"read_only": confirmation}
			})
			hooks := 0
			session, err := f.client(password, 1).OpenSessionWithHook(context.Background(), map[string]any{"read_only": true}, true,
				func(ctx context.Context, s *NewSession) error { hooks++; return nil })
			if confirmation == true {
				if err != nil || session == nil || hooks != 1 || deleted.Load() != 0 {
					t.Fatalf("confirmed: session=%v err=%v hooks=%d deletes=%d", session, err, hooks, deleted.Load())
				}
			} else if err == nil || session != nil || hooks != 0 || deleted.Load() != 1 {
				t.Fatalf("unconfirmed: session=%v err=%v hooks=%d deletes=%d", session, err, hooks, deleted.Load())
			}
		})
	}
}

func fmtValue(v any) string {
	if v == nil {
		return "missing"
	}
	if v == true {
		return "boolean_true"
	}
	if v == false {
		return "boolean_false"
	}
	return "string_true"
}

func TestReadOnlyLazyModeIsStickyAndConfirmedOnRead(t *testing.T) {
	f := newFake(t).withSessions()
	session := f.client(password, 1).NewLazySession(nil, true, nil, nil)
	ctx := context.Background()
	if err := session.SetOptions(ctx, map[string]any{"read_only": true}); err != nil {
		t.Fatal(err)
	}
	if err := session.SetOptions(ctx, map[string]any{"read_only": false}); !HasCode(err, CodeReadOnly) {
		t.Fatalf("downgrade: %v", err)
	}
	if len(f.sent()) != 0 {
		t.Fatalf("lazy setup sent requests: %v", f.sent())
	}
	if got, err := session.ReadOnly(ctx); !got || err != nil {
		t.Fatalf("confirmed read-only=%v err=%v", got, err)
	}
	if f.created[0]["read_only"] != true {
		t.Fatalf("creation options=%v", f.created)
	}
}

func TestReadOnlyIsReplayedOnReplacement(t *testing.T) {
	for _, imposed := range []bool{false, true} {
		t.Run(fmtValue(imposed), func(t *testing.T) {
			f := newFake(t).withSessions()
			readOnlyReplies(f, func(_ string, doc map[string]any) { doc["options"].(map[string]any)["read_only"] = true })
			options := map[string]any{}
			if !imposed {
				options["read_only"] = true
			}
			session, err := f.client(password, 1).OpenSession(context.Background(), options, true)
			if err != nil {
				t.Fatal(err)
			}
			f.evict()
			if err := submitSQL(t, session, "SELECT 1"); err != nil {
				t.Fatal(err)
			}
			if len(f.created) != 2 || f.created[1]["read_only"] != true {
				t.Fatalf("replacement options=%v", f.created)
			}
			if err := session.SetOptions(context.Background(), map[string]any{"read_only": false}); !HasCode(err, CodeReadOnly) {
				t.Fatalf("downgrade: %v", err)
			}
		})
	}
}

func TestReadOnlyReplacementMustConfirmAgain(t *testing.T) {
	f := newFake(t).withSessions()
	var creates atomic.Int32
	deleted := readOnlyReplies(f, func(method string, doc map[string]any) {
		if method == http.MethodPost && creates.Add(1) > 1 {
			doc["options"] = map[string]any{}
		}
	})
	session, err := f.client(password, 1).OpenSession(context.Background(), map[string]any{"read_only": true}, true)
	if err != nil {
		t.Fatal(err)
	}
	f.evict()
	if err := submitSQL(t, session, "SELECT 1"); err == nil {
		t.Fatal("used an unconfirmed replacement")
	}
	if deleted.Load() != 1 || len(f.ran) != 0 {
		t.Fatalf("deletes=%d statements=%v", deleted.Load(), f.ran)
	}
}

func TestReadOnlyPatchConfirmationAndRefusal(t *testing.T) {
	for _, mode := range []string{"confirmed", "ignored", "refused"} {
		t.Run(mode, func(t *testing.T) {
			f := newFake(t).withSessions()
			deleted := readOnlyReplies(f, func(method string, doc map[string]any) {
				if method != http.MethodPatch || mode == "confirmed" {
					return
				}
				doc["options"] = map[string]any{}
				if mode == "refused" {
					doc["errors"] = map[string]any{"read_only": map[string]any{"code": CodeUnknownOption}}
				}
			})
			session := openTestSession(t, f, true)
			err := session.SetOptions(context.Background(), map[string]any{"read_only": true})
			if mode == "confirmed" {
				if err != nil {
					t.Fatal(err)
				}
				if got, err := session.ReadOnly(context.Background()); !got || err != nil {
					t.Fatalf("read-only=%v err=%v", got, err)
				}
				if err := session.SetOptions(context.Background(), map[string]any{"read_only": false}); !HasCode(err, CodeReadOnly) {
					t.Fatalf("downgrade: %v", err)
				}
			} else {
				if err == nil {
					t.Fatal("unconfirmed patch succeeded")
				}
				if mode == "ignored" {
					if deleted.Load() != 1 {
						t.Fatalf("deletes=%d", deleted.Load())
					}
					if err := submitSQL(t, session, "SELECT 1"); err == nil {
						t.Fatal("unconfirmed connection remains usable")
					}
				} else if !HasCode(err, CodeUnknownOption) || deleted.Load() != 0 {
					t.Fatalf("refusal: %v deletes=%d", err, deleted.Load())
				}
			}
		})
	}
}

func TestReadOnlyConfirmationCannotBelongToAnOldSession(t *testing.T) {
	f := newFake(t).withSessions()
	arrived, release := make(chan struct{}), make(chan struct{})
	f.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPatch {
			close(arrived)
			<-release
			writeJSON(w, http.StatusOK, map[string]any{"options": map[string]any{"read_only": true}})
			return
		}
		if r.Method == http.MethodDelete {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		f.serve(w, r)
	})
	session := openTestSession(t, f, true)
	done := make(chan error, 1)
	go func() { done <- session.SetOptions(context.Background(), map[string]any{"read_only": true}) }()
	<-arrived
	f.evict()
	replacementErr := submitSQL(t, session, "SELECT 1")
	close(release)
	patchErr := <-done
	if replacementErr != nil {
		t.Fatal(replacementErr)
	}
	if patchErr == nil {
		t.Fatal("old confirmation accepted for replacement")
	}
	if err := submitSQL(t, session, "SELECT 1"); err == nil {
		t.Fatal("unconfirmed replacement still usable")
	}
}
