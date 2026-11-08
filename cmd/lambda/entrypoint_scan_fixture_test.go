package main

import (
	"os"
	"path/filepath"
	"testing"
)

// The fixture tree's import path. The scan resolves the forma imports by their
// real paths, so a fixture spells those, and its own helper packages sit under
// this path.
const fixtureTreePath = "example.com/fixture/cmd/app"

const fixtureImports = `import (
	"github.com/lychee-technology/forma/factory"
	"github.com/lychee-technology/forma/internal/bootstrap"
)
`

// fixtureBuildsInMain is a main package that builds a manager from main and
// leaves the overlay to whatever the case adds.
const fixtureBuildsInMain = "package main\n\n" + fixtureImports + `
func main() {
	cfg := loadConfig()
	_, _ = factory.NewEntityManagerWithConfig(cfg, nil)
}
`

type scanFixtureCase struct {
	name  string
	files map[string]string // slash-separated path in the tree -> source
	want  entryPointScan
}

// scanFixtureCases pin what the AST scan behind
// TestEntityConfigFromEnvWiredInEveryWriteEntryPoint counts (#429): each case
// is a cmd/<name> tree in miniature, and want is the verdict on it. The trees
// only have to parse, not compile.
var scanFixtureCases = []scanFixtureCase{
	{
		name: "overlay in a sibling file, called from main",
		files: map[string]string{
			"main.go":   fixtureBuildsInMain,
			"config.go": "package main\n\n" + fixtureImports + "func loadConfig() any { c, _ := bootstrap.EntityConfigFromEnv(nil); return c }\n",
		},
		want: entryPointScan{builds: true, overlays: true},
	},
	{
		name: "overlay only named in a comment",
		files: map[string]string{
			"main.go":   fixtureBuildsInMain,
			"config.go": "package main\n\n" + fixtureImports + "// loadConfig calls bootstrap.EntityConfigFromEnv(nil).\nfunc loadConfig() any { return nil }\n",
		},
		want: entryPointScan{builds: true},
	},
	{
		name: "overlay only named in a string",
		files: map[string]string{
			"main.go":   fixtureBuildsInMain,
			"config.go": "package main\n\nfunc loadConfig() any { return \"bootstrap.EntityConfigFromEnv(nil)\" }\n",
		},
		want: entryPointScan{builds: true},
	},
	{
		name: "builder only named in a comment and a string",
		files: map[string]string{
			"main.go": "package main\n\n// main never calls factory.NewEntityManagerWithConfig.\nfunc main() { println(\"factory.NewEntityManager\") }\n",
		},
		want: entryPointScan{},
	},
	{
		name: "overlay in a function nothing calls",
		files: map[string]string{
			"main.go":   fixtureBuildsInMain,
			"config.go": "package main\n\n" + fixtureImports + "func loadConfig() any { return nil }\nfunc unused() { bootstrap.EntityConfigFromEnv(nil) }\n",
		},
		want: entryPointScan{builds: true},
	},
	{
		name: "aliased imports",
		files: map[string]string{
			"main.go": `package main

import (
	f "github.com/lychee-technology/forma/factory"
	boot "github.com/lychee-technology/forma/internal/bootstrap"
)

func main() {
	cfg, _ := boot.EntityConfigFromEnv(nil)
	_, _ = f.NewEntityManagerWithConfigContext(nil, cfg, nil)
}
`,
		},
		want: entryPointScan{builds: true, overlays: true},
	},
	{
		name: "dot imports",
		files: map[string]string{
			"main.go": `package main

import (
	. "github.com/lychee-technology/forma/factory"
	. "github.com/lychee-technology/forma/internal/bootstrap"
)

func main() {
	cfg, _ := EntityConfigFromEnv(nil)
	_, _ = NewEntityManagerWithConfig(cfg, nil)
}
`,
		},
		want: entryPointScan{builds: true, overlays: true},
	},
	{
		name: "a package that is not forma's factory does not build",
		files: map[string]string{
			"main.go": "package main\n\nimport \"example.com/other/factory\"\n\nfunc main() { factory.NewEntityManager() }\n",
		},
		want: entryPointScan{},
	},
	{
		name: "builder taken as a value",
		files: map[string]string{
			"main.go": "package main\n\n" + fixtureImports + "func main() { build := factory.NewEntityManagerWithConfig; _, _ = build(nil, nil) }\n",
		},
		want: entryPointScan{builds: true},
	},
	{
		name: "overlay in a helper package main imports",
		files: map[string]string{
			"main.go": `package main

import (
	"example.com/fixture/cmd/app/wiring"
	"github.com/lychee-technology/forma/factory"
)

func main() { _, _ = factory.NewEntityManagerWithConfig(wiring.Config(), nil) }
`,
			"wiring/wiring.go": "package wiring\n\n" + fixtureImports + "func Config() any { c, _ := bootstrap.EntityConfigFromEnv(nil); return c }\n",
		},
		want: entryPointScan{builds: true, overlays: true},
	},
	{
		name: "overlay in a helper package whose name is not its directory's",
		files: map[string]string{
			"main.go": `package main

import (
	"example.com/fixture/cmd/app/wiring"
	"github.com/lychee-technology/forma/factory"
)

func main() { _, _ = factory.NewEntityManagerWithConfig(config.Config(), nil) }
`,
			"wiring/config.go": "package config\n\n" + fixtureImports + "func Config() any { c, _ := bootstrap.EntityConfigFromEnv(nil); return c }\n",
		},
		want: entryPointScan{builds: true, overlays: true},
	},
	{
		name: "overlay in a dot-imported helper package",
		files: map[string]string{
			"main.go": `package main

import (
	. "example.com/fixture/cmd/app/wiring"
	"github.com/lychee-technology/forma/factory"
)

func main() { _, _ = factory.NewEntityManagerWithConfig(Config(), nil) }
`,
			"wiring/wiring.go": "package wiring\n\n" + fixtureImports + "func Config() any { c, _ := bootstrap.EntityConfigFromEnv(nil); return c }\n",
		},
		want: entryPointScan{builds: true, overlays: true},
	},
	{
		name: "overlay in a dot-imported helper function main never calls",
		files: map[string]string{
			"main.go": `package main

import (
	. "example.com/fixture/cmd/app/wiring"
	"github.com/lychee-technology/forma/factory"
)

func main() { _, _ = factory.NewEntityManagerWithConfig(Config(), nil) }
`,
			"wiring/wiring.go": "package wiring\n\n" + fixtureImports + "func Config() any { return nil }\nfunc Unused() { bootstrap.EntityConfigFromEnv(nil) }\n",
		},
		want: entryPointScan{builds: true},
	},
	{
		name: "overlay in files Go ignores by name",
		files: map[string]string{
			"main.go":     fixtureBuildsInMain + "func loadConfig() any { return nil }\n",
			"_scratch.go": "package main\n\n" + fixtureImports + "func init() { bootstrap.EntityConfigFromEnv(nil) }\n",
			".scratch.go": "package main\n\n" + fixtureImports + "func init() { bootstrap.EntityConfigFromEnv(nil) }\n",
		},
		want: entryPointScan{builds: true},
	},
	{
		name: "unparsable files Go ignores by name are not parsed",
		files: map[string]string{
			"main.go":      fixtureBuildsInMain + "func loadConfig() any { return nil }\n",
			"_broken.go":   "package main\n\nfunc main( {\n",
			".broken.go":   "package main\n\nfunc main( {\n",
			"_dir/main.go": "package main\n\nfunc main( {\n",
		},
		want: entryPointScan{builds: true},
	},
	{
		name: "overlay in a tree package main does not import",
		files: map[string]string{
			"main.go":          fixtureBuildsInMain + "func loadConfig() any { return nil }\n",
			"wiring/wiring.go": "package wiring\n\n" + fixtureImports + "func init() { bootstrap.EntityConfigFromEnv(nil) }\n",
		},
		want: entryPointScan{builds: true},
	},
	{
		name: "overlay in a variable initialiser",
		files: map[string]string{
			"main.go":   fixtureBuildsInMain,
			"config.go": "package main\n\n" + fixtureImports + "var _, overlayErr = bootstrap.EntityConfigFromEnv(nil)\nfunc loadConfig() any { return nil }\n",
		},
		want: entryPointScan{builds: true, overlays: true},
	},
	{
		name: "overlay in init",
		files: map[string]string{
			"main.go":   fixtureBuildsInMain,
			"config.go": "package main\n\n" + fixtureImports + "func init() { bootstrap.EntityConfigFromEnv(nil) }\nfunc loadConfig() any { return nil }\n",
		},
		want: entryPointScan{builds: true, overlays: true},
	},
	{
		name: "overlay in a function passed as a value",
		files: map[string]string{
			"main.go":   fixtureBuildsInMain,
			"config.go": "package main\n\n" + fixtureImports + "func loadConfig() any { return run(overlay) }\nfunc run(f func()) any { return nil }\nfunc overlay() { bootstrap.EntityConfigFromEnv(nil) }\n",
		},
		want: entryPointScan{builds: true, overlays: true},
	},
	{
		name: "overlay in a method of a type main uses",
		files: map[string]string{
			"main.go":   fixtureBuildsInMain,
			"config.go": "package main\n\n" + fixtureImports + "type loader[T any] struct{}\nfunc (l *loader[T]) Load() { bootstrap.EntityConfigFromEnv(nil) }\nfunc loadConfig() any { return &loader[int]{} }\n",
		},
		want: entryPointScan{builds: true, overlays: true},
	},
	{
		name: "overlay in a method of a type nothing uses",
		files: map[string]string{
			"main.go":   fixtureBuildsInMain,
			"config.go": "package main\n\n" + fixtureImports + "type loader struct{}\nfunc (l loader) Load() { bootstrap.EntityConfigFromEnv(nil) }\nfunc loadConfig() any { return nil }\n",
		},
		want: entryPointScan{builds: true},
	},
	{
		name: "a build-constrained file is still read",
		files: map[string]string{
			"main.go": "//go:build linux && arm64 && !linux\n\n" + fixtureBuildsInMain + "func loadConfig() any { return nil }\n",
		},
		want: entryPointScan{builds: true},
	},
	{
		name: "testdata and test files are not read",
		files: map[string]string{
			"main.go":                "package main\n\nfunc main() {}\n",
			"main_test.go":           fixtureBuildsInMain,
			"testdata/fixture.go":    fixtureBuildsInMain,
			"_scratch/scratch.go":    fixtureBuildsInMain,
			".hidden/hidden_main.go": fixtureBuildsInMain,
		},
		want: entryPointScan{},
	},
}

func TestScanEntryPointTreeFixtures(t *testing.T) {
	for _, tc := range scanFixtureCases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			for name, source := range tc.files {
				path := filepath.Join(dir, filepath.FromSlash(name))
				if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
					t.Fatalf("failed to create the directory of %s: %v", name, err)
				}
				if err := os.WriteFile(path, []byte(source), 0o600); err != nil {
					t.Fatalf("failed to write %s: %v", name, err)
				}
			}
			got, err := scanEntryPointTree(dir, fixtureTreePath)
			if err != nil {
				t.Fatalf("scan failed: %v", err)
			}
			if got != tc.want {
				t.Fatalf("scan = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// TestScanEntryPointTreeRejectsUnparsableFile pins that a file the parser
// refuses fails the scan instead of being skipped: a skipped file is exactly
// the "never looked" hole #321 closed.
func TestScanEntryPointTreeRejectsUnparsableFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte("package main\n\nfunc main( {\n"), 0o600); err != nil {
		t.Fatalf("failed to write main.go: %v", err)
	}
	if _, err := scanEntryPointTree(dir, fixtureTreePath); err == nil {
		t.Fatal("expected a parse error, got none")
	}
}
