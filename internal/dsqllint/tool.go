package dsqllint

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// ToolVersion is the pinned dsql-lint release. To upgrade, follow
// docs/dsql-lint.md: change the version and every wheel's URL and sha256
// together, from PyPI's JSON API for the new release.
const ToolVersion = "0.2.22"

// wheel is one platform's published dsql-lint wheel on PyPI. The wheels
// carry the prebuilt Rust binary and nothing else to install, so the gate
// extracts it directly instead of running pip.
type wheel struct {
	URL    string
	SHA256 string
}

// wheels pins dsql-lint ToolVersion per GOOS/GOARCH.
var wheels = map[string]wheel{
	"linux/amd64": {
		URL:    "https://files.pythonhosted.org/packages/21/db/69678a20c978079b1b37139879c1854445e694b1642c9e56ffd1cb2682e5/dsql_lint-0.2.22-py3-none-manylinux_2_17_x86_64.manylinux2014_x86_64.whl",
		SHA256: "c1a322d0e5818a4617678378d559ddaa5c080271c1fc4bf2a60035b2d79aa1ef",
	},
	"linux/arm64": {
		URL:    "https://files.pythonhosted.org/packages/c6/fd/983e95b9b033290aa63fc5dddfb682193e6df77044a5907206373750d66f/dsql_lint-0.2.22-py3-none-manylinux_2_17_aarch64.manylinux2014_aarch64.whl",
		SHA256: "4d131fcca1e417f8e823f12b4b7466eedb1fc1c45ecf91efba29f67bc578fe20",
	},
	"darwin/amd64": {
		URL:    "https://files.pythonhosted.org/packages/12/5b/151e5fb8e48d809cad8fc5f99a51831084778749debe28c85bacfd83c17e/dsql_lint-0.2.22-py3-none-macosx_10_12_x86_64.whl",
		SHA256: "db52fc35d47c297f1a168a2ee11ea4be6359957896ef74006285b6959bc0fa5c",
	},
	"darwin/arm64": {
		URL:    "https://files.pythonhosted.org/packages/b8/5b/1da6178182731f08194ffff25863d4ad57d490b63f17b8660c9736f256cd/dsql_lint-0.2.22-py3-none-macosx_11_0_arm64.whl",
		SHA256: "a3b3260f8c6578f843fe8152d06a82d53cd01655d2d7db824c795c12143825ed",
	},
}

// binaryInWheel is the dsql-lint executable's path inside a wheel.
var binaryInWheel = "dsql_lint-" + ToolVersion + ".data/scripts/dsql-lint"

// EnsureTool answers a dsql-lint executable of ToolVersion. With override
// set (DSQL_LINT_BIN) it uses that binary; otherwise it uses the copy cached
// under cacheDir, downloading and verifying the pinned wheel first if there
// is none. Either way the binary must report ToolVersion.
func EnsureTool(ctx context.Context, cacheDir, override string) (string, error) {
	if override != "" {
		return override, verifyVersion(ctx, override)
	}
	platform := runtime.GOOS + "/" + runtime.GOARCH
	w, ok := wheels[platform]
	if !ok {
		return "", fmt.Errorf("no pinned dsql-lint %s wheel for %s; set DSQL_LINT_BIN to a dsql-lint %s binary", ToolVersion, platform, ToolVersion)
	}
	path := filepath.Join(cacheDir, "dsql-lint-"+ToolVersion, runtime.GOOS+"-"+runtime.GOARCH, "dsql-lint")
	if _, err := os.Stat(path); err != nil {
		if err := install(ctx, w, path); err != nil {
			return "", err
		}
	}
	return path, verifyVersion(ctx, path)
}

func install(ctx context.Context, w wheel, path string) error {
	data, err := download(ctx, w.URL)
	if err != nil {
		return err
	}
	binary, err := extractBinary(data, w.SHA256)
	if err != nil {
		return fmt.Errorf("dsql-lint wheel %s: %w", w.URL, err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create %s: %w", filepath.Dir(path), err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, binary, 0o755); err != nil {
		return fmt.Errorf("write %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("install %s: %w", path, err)
	}
	return nil
}

func download(ctx context.Context, url string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request for %s: %w", url, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("download dsql-lint %s from %s: %w", ToolVersion, url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("download dsql-lint %s from %s: HTTP %s", ToolVersion, url, resp.Status)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 256<<20))
	if err != nil {
		return nil, fmt.Errorf("read dsql-lint wheel from %s: %w", url, err)
	}
	return data, nil
}

// extractBinary checks a wheel against its pinned sha256 and answers the
// dsql-lint executable inside it.
func extractBinary(data []byte, wantSHA256 string) ([]byte, error) {
	sum := sha256.Sum256(data)
	if got := hex.EncodeToString(sum[:]); got != wantSHA256 {
		return nil, fmt.Errorf("sha256 %s, pinned %s: refusing an unverified download", got, wantSHA256)
	}
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("open as zip: %w", err)
	}
	for _, f := range zr.File {
		if f.Name != binaryInWheel {
			continue
		}
		rc, err := f.Open()
		if err != nil {
			return nil, fmt.Errorf("open %s: %w", f.Name, err)
		}
		defer func() { _ = rc.Close() }()
		binary, err := io.ReadAll(rc)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", f.Name, err)
		}
		return binary, nil
	}
	return nil, fmt.Errorf("no %s inside", binaryInWheel)
}

func verifyVersion(ctx context.Context, path string) error {
	out, err := exec.CommandContext(ctx, path, "--version").CombinedOutput()
	if err != nil {
		return fmt.Errorf("run %s --version: %w: %s", path, err, strings.TrimSpace(string(out)))
	}
	if got, want := strings.TrimSpace(string(out)), "dsql-lint "+ToolVersion; got != want {
		return fmt.Errorf("%s reports %q, the gate pins %q", path, got, want)
	}
	return nil
}
