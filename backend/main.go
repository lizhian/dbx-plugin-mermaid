package main

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	dbx "github.com/t8y2/dbx/plugins/sdk/go/dbx-plugin-sdk"
	_ "modernc.org/sqlite"
)

type plugin struct {
	mu      sync.Mutex
	dbs     map[string]*sql.DB
	configs map[string]map[string]any
}

func str(m map[string]any, k string) string { v, _ := m[k].(string); return v }
func conn(v map[string]any) map[string]any  { c, _ := v["connection"].(map[string]any); return c }
func (p *plugin) Handle(_ dbx.RequestContext, method string, raw json.RawMessage, _ *dbx.Emitter) (any, *dbx.PluginError) {
	var v map[string]any
	if json.Unmarshal(raw, &v) != nil {
		return nil, dbx.NewError(-32602, "invalid params")
	}
	switch method {
	case "connection/test":
		return map[string]any{"success": true, "message": "连接配置有效"}, nil
	case "connection/connect":
		c := conn(v)
		id := str(c, "id")
		if id == "" {
			id = str(v, "connectionId")
		}
		cfg, _ := c["external_config"].(map[string]any)
		if cfg == nil {
			cfg = c
		}
		p.mu.Lock()
		p.configs[id] = cfg
		p.mu.Unlock()
		if path := str(cfg, "path"); path != "" {
			if err := p.open(id, path); err != nil {
				return nil, dbx.NewError(-32000, err.Error())
			}
		}
		return map[string]any{"success": true, "connectionId": id}, nil
	case "connection/disconnect":
		id := str(v, "connectionId")
		p.mu.Lock()
		if d := p.dbs[id]; d != nil {
			d.Close()
			delete(p.dbs, id)
		}
		p.mu.Unlock()
		return map[string]any{"success": true}, nil
	}
	id := str(v, "connectionId")
	if id == "" {
		return nil, dbx.NewError(-32602, "missing connectionId")
	}
	switch method {
	case "storage/list":
		return p.list(id, str(v, "prefix"))
	case "storage/read":
		return p.read(id, str(v, "key"))
	case "storage/write":
		return p.write(id, str(v, "key"), str(v, "body"))
	case "storage/delete":
		return p.delete(id, str(v, "key"))
	case "storage/mkdir":
		return map[string]any{"success": true}, nil
	case "storage/rename":
		return p.rename(id, str(v, "source"), str(v, "target"))
	case "storage/history":
		return p.history(id, str(v, "key"))
	case "storage/restore":
		return map[string]any{"success": true}, nil
	default:
		return nil, dbx.MethodNotFound(method)
	}
}
func (p *plugin) open(id, path string) error {
	os.MkdirAll(filepath.Dir(path), 0700)
	d, e := sql.Open("sqlite", path)
	if e != nil {
		return e
	}
	_, e = d.Exec(`CREATE TABLE IF NOT EXISTS documents (key TEXT PRIMARY KEY, body TEXT NOT NULL, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP); CREATE TABLE IF NOT EXISTS document_versions (id INTEGER PRIMARY KEY, key TEXT, body TEXT, created_at DATETIME DEFAULT CURRENT_TIMESTAMP);`)
	if e != nil {
		return e
	}
	p.dbs[id] = d
	return nil
}
func (p *plugin) db(id string) (*sql.DB, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	d := p.dbs[id]
	if d == nil {
		return nil, fmt.Errorf("connection not initialized")
	}
	return d, nil
}
func (p *plugin) list(id, prefix string) (any, *dbx.PluginError) {
	d, e := p.db(id)
	if e != nil {
		return nil, dbx.NewError(-32000, e.Error())
	}
	rows, e := d.Query(`SELECT key,length(body),updated_at FROM documents WHERE key LIKE ? ORDER BY key`, prefix+"%")
	if e != nil {
		return nil, dbx.NewError(-32000, e.Error())
	}
	defer rows.Close()
	out := []any{}
	for rows.Next() {
		var k string
		var n int
		var t any
		rows.Scan(&k, &n, &t)
		out = append(out, map[string]any{"name": k, "key": k, "kind": "file", "size": n, "modifiedAt": t})
	}
	return map[string]any{"items": out}, nil
}
func (p *plugin) read(id, key string) (any, *dbx.PluginError) {
	d, e := p.db(id)
	if e != nil {
		return nil, dbx.NewError(-32000, e.Error())
	}
	var b string
	if e = d.QueryRow(`SELECT body FROM documents WHERE key=?`, key).Scan(&b); e != nil {
		return nil, dbx.NewError(-32004, e.Error())
	}
	return map[string]any{"body": b, "dataBase64": base64.StdEncoding.EncodeToString([]byte(b))}, nil
}
func (p *plugin) write(id, key, body string) (any, *dbx.PluginError) {
	d, e := p.db(id)
	if e != nil {
		return nil, dbx.NewError(-32000, e.Error())
	}
	_, e = d.Exec(`INSERT INTO documents(key,body) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET body=excluded.body,updated_at=CURRENT_TIMESTAMP`, key, body)
	if e != nil {
		return nil, dbx.NewError(-32000, e.Error())
	}
	d.Exec(`INSERT INTO document_versions(key,body) VALUES(?,?)`, key, body)
	return map[string]any{"success": true}, nil
}
func (p *plugin) delete(id, key string) (any, *dbx.PluginError) {
	d, e := p.db(id)
	if e != nil {
		return nil, dbx.NewError(-32000, e.Error())
	}
	_, e = d.Exec(`DELETE FROM documents WHERE key=?`, key)
	if e != nil {
		return nil, dbx.NewError(-32000, e.Error())
	}
	return map[string]any{"success": true}, nil
}
func (p *plugin) rename(id, a, b string) (any, *dbx.PluginError) {
	d, e := p.db(id)
	if e != nil {
		return nil, dbx.NewError(-32000, e.Error())
	}
	_, e = d.Exec(`UPDATE documents SET key=? WHERE key=?`, b, a)
	if e != nil {
		return nil, dbx.NewError(-32000, e.Error())
	}
	return map[string]any{"success": true}, nil
}
func (p *plugin) history(id, key string) (any, *dbx.PluginError) {
	d, e := p.db(id)
	if e != nil {
		return nil, dbx.NewError(-32000, e.Error())
	}
	rows, e := d.Query(`SELECT id,body,created_at FROM document_versions WHERE key=? ORDER BY id DESC`, key)
	if e != nil {
		return nil, dbx.NewError(-32000, e.Error())
	}
	defer rows.Close()
	out := []any{}
	for rows.Next() {
		var i int
		var b string
		var t any
		rows.Scan(&i, &b, &t)
		out = append(out, map[string]any{"id": i, "body": b, "createdAt": t})
	}
	return map[string]any{"versions": out}, nil
}
func main() {
	p := &plugin{dbs: map[string]*sql.DB{}, configs: map[string]map[string]any{}}
	s := dbx.NewServer(dbx.Metadata{ID: "io.github.lizhian.mermaid", Version: "0.1.0", Capabilities: []string{"connections", "storage"}}, p)
	if e := s.Serve(); e != nil {
		log.Fatal(e)
	}
}
