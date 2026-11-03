package sites

import (
	"fmt"
	"go/token"
	"os"
	"sort"
	"strings"
	"time"

	"golang.org/x/tools/go/packages"
)

// DefaultEntries are the shipped entry points of forma: the library
// surface (root, factory, cdc) and the binaries that ship (server, tools,
// lambda). Every other main package must be listed as unshipped.
var DefaultEntries = []string{".", "./factory", "./cdc", "./cmd/server", "./cmd/tools", "./cmd/lambda"}

// Mode selects how SQL forwarded through a project function is followed.
type Mode string

const (
	// Verbatim: a function is a wrapper only when it passes one of its own
	// string parameters unchanged as the SQL argument of a sink.
	Verbatim Mode = "verbatim"
	// Taint: a function is a wrapper when the SQL argument of a sink depends,
	// through local assignments and builder writes, on any of its string
	// parameters.
	Taint Mode = "taint"
)

// Options configures one inventory run.
type Options struct {
	Dir     string
	Entries []string
	Mode    Mode
	// Patterns, when set, replaces the shipped-closure computation: the
	// packages matching them are analysed (used by fixtures).
	Patterns []string
	// BuildFlags are passed to the go command (for example -tags dsqlspike).
	BuildFlags []string
}

// Load computes the shipped closure, loads it with types and analyses it.
func Load(opt Options) (*Result, error) {
	if opt.Mode == "" {
		opt.Mode = Verbatim
	}
	if len(opt.Entries) == 0 {
		opt.Entries = DefaultEntries
	}
	timings := map[string]time.Duration{}
	start := time.Now()
	paths := opt.Patterns
	var module string
	if len(paths) == 0 {
		var err error
		paths, module, err = shippedClosure(opt.Dir, opt.Entries)
		if err != nil {
			return nil, err
		}
	}
	timings["closure"] = time.Since(start)

	start = time.Now()
	cfg := &packages.Config{
		Fset:       token.NewFileSet(),
		BuildFlags: opt.BuildFlags,
		Dir:        opt.Dir,
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes |
			packages.NeedTypesInfo | packages.NeedImports | packages.NeedModule,
	}
	pkgs, err := packages.Load(cfg, paths...)
	if err != nil {
		return nil, fmt.Errorf("load shipped packages: %w", err)
	}
	var loadErrs []string
	packages.Visit(pkgs, nil, func(p *packages.Package) {
		for _, e := range p.Errors {
			loadErrs = append(loadErrs, e.Error())
		}
	})
	if len(loadErrs) > 0 {
		return nil, fmt.Errorf("load errors (fail closed): %s", strings.Join(loadErrs, "; "))
	}
	timings["load"] = time.Since(start)
	if module == "" && len(pkgs) > 0 && pkgs[0].Module != nil {
		module = pkgs[0].Module.Path
	}

	start = time.Now()
	a := newAnalyzer(cfg.Fset, pkgs, module, opt.Dir, opt.Mode)
	res := a.run()
	timings["analyse"] = time.Since(start)
	res.Timings = map[string]string{}
	for k, v := range timings {
		res.Timings[k] = v.Round(time.Millisecond).String()
	}
	res.Mode = opt.Mode
	res.Packages = len(pkgs)
	res.PeakRSSKB = peakRSS()
	return res, nil
}

// shippedClosure answers the module packages in the import closure of
// entries, and the module path.
func shippedClosure(dir string, entries []string) ([]string, string, error) {
	cfg := &packages.Config{Dir: dir, Mode: packages.NeedName | packages.NeedImports | packages.NeedDeps | packages.NeedModule}
	roots, err := packages.Load(cfg, entries...)
	if err != nil {
		return nil, "", fmt.Errorf("list shipped closure: %w", err)
	}
	var module string
	set := map[string]bool{}
	packages.Visit(roots, nil, func(p *packages.Package) {
		if p.Module != nil && p.Module.Main {
			set[p.PkgPath] = true
			module = p.Module.Path
		}
	})
	paths := make([]string, 0, len(set))
	for p := range set {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	return paths, module, nil
}

// MainPackages answers every main package of the module at dir.
func MainPackages(dir string) ([]string, error) {
	cfg := &packages.Config{Dir: dir, Mode: packages.NeedName}
	pkgs, err := packages.Load(cfg, "./...")
	if err != nil {
		return nil, fmt.Errorf("list main packages: %w", err)
	}
	var mains []string
	for _, p := range pkgs {
		if p.Name == "main" {
			mains = append(mains, p.PkgPath)
		}
	}
	sort.Strings(mains)
	return mains, nil
}

func peakRSS() int {
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return -1
	}
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "VmHWM:") {
			var kb int
			_, _ = fmt.Sscanf(strings.TrimSpace(strings.TrimPrefix(line, "VmHWM:")), "%d", &kb)
			return kb
		}
	}
	return -1
}
