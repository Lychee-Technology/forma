// Package repoguard holds the repository's policy tests: guards over the
// Makefile, the CI and nightly workflows, go.mod, the docs site, the
// MiniStack lane script, and source-wide patterns that span every package.
// None of them exercises the API of package forma, so they live here rather
// than in the module root, which stays the public package and its own tests.
//
// The package has no production code; this file only documents it. Its tests
// resolve the repository root once, in TestMain, by walking up from the
// package directory to the go.mod declaring github.com/lychee-technology/forma,
// and read every repository file through that root with root-relative paths.
// Fixtures the guards own (the MiniStack lane's fake tools) sit in this
// package's testdata/.
package repoguard
