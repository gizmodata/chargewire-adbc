// Licensed under the Apache License, Version 2.0.

package chargewire

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestReadOnlyConnectionInitOptions(t *testing.T) {
	options := map[string]string{}
	for _, value := range []string{"true", "true"} {
		if err := SetInitOption(options, adbc.OptionKeyReadOnly, value); err != nil {
			t.Fatal(err)
		}
	}
	if err := SetInitOption(options, adbc.OptionKeyReadOnly, "false"); err == nil {
		t.Fatal("accepted downgrade before Init")
	}
	if err := SetInitOption(options, adbc.OptionKeyReadOnly, "invalid"); err == nil {
		t.Fatal("accepted invalid boolean")
	}
	if options[adbc.OptionKeyReadOnly] != "true" {
		t.Fatalf("failed SetOption changed mode: %v", options)
	}
	if _, err := ConnectionInitContext(context.Background(), map[string]string{adbc.OptionKeyReadOnly: "invalid"}); err == nil {
		t.Fatal("Init accepted invalid boolean")
	}
}

func TestReadOnlyOpenAndLazyConfirmation(t *testing.T) {
	for _, lazy := range []bool{false, true} {
		for _, confirm := range []bool{false, true} {
			name := "eager"
			if lazy {
				name = "lazy"
			}
			if confirm {
				name += "/confirmed"
			} else {
				name += "/ignored"
			}
			t.Run(name, func(t *testing.T) {
				f := newFakeChargewire(t)
				if lazy {
					f.setState("stopped")
				}
				var mu sync.Mutex
				var creates []map[string]any
				f.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method == http.MethodPost && r.URL.Path == "/chargewire/v1/sessions" {
						var body struct {
							Options map[string]any `json:"options"`
						}
						if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
							t.Error(err)
						}
						mu.Lock()
						creates = append(creates, body.Options)
						mu.Unlock()
						options := map[string]any{}
						if confirm {
							options["read_only"] = true
						}
						fakeJSON(w, http.StatusCreated, map[string]any{"session_id": "s1", "options": options})
						return
					}
					f.mux.ServeHTTP(w, r)
				})
				db, err := NewDriver(memory.DefaultAllocator).NewDatabaseWithContext(context.Background(), map[string]string{
					adbc.OptionKeyURI: f.server.URL, adbc.OptionKeyUsername: "u", adbc.OptionKeyPassword: "p"})
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close(context.Background())
				ctx, err := ConnectionInitContext(context.Background(), map[string]string{adbc.OptionKeyReadOnly: "true"})
				if err != nil {
					t.Fatal(err)
				}
				cnxn, err := db.Open(ctx)
				if lazy || confirm {
					if err != nil {
						t.Fatal(err)
					}
					defer cnxn.Close(context.Background())
					if lazy {
						mu.Lock()
						count := len(creates)
						mu.Unlock()
						if count != 0 {
							t.Fatal("lazy open created a session")
						}
					}
					options := cnxn.(adbc.GetSetOptionsWithContext)
					value, getErr := options.GetOption(context.Background(), adbc.OptionKeyReadOnly)
					if confirm {
						if getErr != nil || value != "true" {
							t.Fatalf("GetOption=%q err=%v", value, getErr)
						}
						if err := options.SetOption(context.Background(), adbc.OptionKeyReadOnly, "false"); err == nil {
							t.Fatal("accepted writable downgrade")
						}
					} else if getErr == nil {
						t.Fatal("GetOption reported unconfirmed mode")
					}
				} else if err == nil || cnxn != nil {
					t.Fatal("Open returned an unconfirmed session")
				}
				mu.Lock()
				defer mu.Unlock()
				if len(creates) != 1 || creates[0]["read_only"] != true {
					t.Fatalf("creation requests=%v", creates)
				}
			})
		}
	}
}
