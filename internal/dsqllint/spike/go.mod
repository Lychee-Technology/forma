// EXPERIMENTAL: DSQL typed call-site spike (#740). A separate module so the
// spike never enters forma's go.mod, `go test ./...`, make test or CI.
module forma-dsql-spike

go 1.26

require golang.org/x/tools v0.42.0

require (
	golang.org/x/mod v0.33.0 // indirect
	golang.org/x/sync v0.19.0 // indirect
)
