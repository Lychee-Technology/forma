// The module graph of the previous DuckDB driver, for building
// cmd/duckdbcompat with -modfile (scripts/duckdb_storage_compat.sh). It is
// never the build's go.mod: go.mod beside it still marks the module root.
// It names the driver from go.mod before the DuckDB 2.0 upgrade (#752):
// duckdb-go v2.10506.0, which bundles DuckDB 1.5.6. Move it forward when the
// next upgrade lands, so the probe always pairs the current release with the
// one a rollback would return to.
module github.com/lychee-technology/forma

go 1.26

require github.com/duckdb/duckdb-go/v2 v2.10506.0

require (
	github.com/duckdb/duckdb-go-bindings v0.10506.0 // indirect
	github.com/duckdb/duckdb-go-bindings/lib/darwin-amd64 v0.10506.0 // indirect
	github.com/duckdb/duckdb-go-bindings/lib/darwin-arm64 v0.10506.0 // indirect
	github.com/duckdb/duckdb-go-bindings/lib/linux-amd64 v0.10506.0 // indirect
	github.com/duckdb/duckdb-go-bindings/lib/linux-arm64 v0.10506.0 // indirect
	github.com/duckdb/duckdb-go-bindings/lib/windows-amd64 v0.10506.0 // indirect
	github.com/go-viper/mapstructure/v2 v2.5.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
)
