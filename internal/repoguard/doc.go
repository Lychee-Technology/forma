// Package repoguard holds the repository's policy tests: guards over the
// Makefile, the CI and nightly workflows, go.mod, the docs site, the
// MiniStack lane script, and source-wide patterns that span every package.
// None of them exercises the API of package forma, so they live here rather
// than in the module root, which stays the public package and its own tests.
//
// The package has no production code; this file only documents it. Its tests
// resolve the repository root once, in TestMain, by walking up from the
// working directory (the package directory under go test, anywhere inside the
// repository for a compiled guard binary) to the go.mod declaring
// github.com/lychee-technology/forma, read as the go command reads it. They
// read every repository file through that root with root-relative paths, this
// package's own directory and fixtures included: nothing depends on the
// working directory. Fixtures the guards own (the MiniStack lane's fake tools)
// sit in this package's testdata/.
package repoguard
