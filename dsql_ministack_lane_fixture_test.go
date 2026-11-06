package forma

import (
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const (
	laneProject = "lane-test"
	laneLabel   = "forma.dsql-ministack.project"
)

// laneFixture runs a copy of the MiniStack lane's script against the fake
// docker, curl and go in testdata/dsql_ministack. The copy derives its repo root
// from its own path, so its logs land in the test's directory.
type laneFixture struct {
	t      *testing.T
	root   string
	state  string
	script string
	env    []string
}

func newLaneFixture(t *testing.T) *laneFixture {
	t.Helper()
	// Short and outside t.TempDir: it holds a unix socket, whose path is limited.
	root, err := os.MkdirTemp("", "ms")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	f := &laneFixture{t: t, root: root, state: filepath.Join(root, "state")}

	bin := filepath.Join(root, "bin")
	scripts := filepath.Join(root, "scripts")
	for _, dir := range []string{bin, scripts, filepath.Join(f.state, "fail"), filepath.Join(f.state, "hooks")} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("create %s: %v", dir, err)
		}
	}
	tools, err := filepath.Abs("testdata/dsql_ministack/fake_tools.sh")
	if err != nil {
		t.Fatalf("resolve fake tools: %v", err)
	}
	for _, tool := range []string{"docker", "curl", "go"} {
		if err := os.Symlink(tools, filepath.Join(bin, tool)); err != nil {
			t.Fatalf("link fake %s: %v", tool, err)
		}
	}
	body, err := os.ReadFile(ministackScript)
	if err != nil {
		t.Fatalf("read %s: %v", ministackScript, err)
	}
	f.script = filepath.Join(scripts, "test_dsql_ministack.sh")
	f.write(f.script, string(body))
	f.write(filepath.Join(scripts, "dsql_ministack_expected_skips.txt"), "TestLedgered\tthe fake go skips it\n")

	// preflight wants a real socket, which the fake docker never reads.
	socket := filepath.Join(root, "d.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatalf("listen on %s: %v", socket, err)
	}
	t.Cleanup(func() { listener.Close() })

	for _, kv := range os.Environ() {
		switch strings.SplitN(kv, "=", 2)[0] {
		case "PATH", "DOCKER_HOST", "MINISTACK_DSQL_PROJECT", "DSQL_DOCKER_NETWORK", "DSQL_BASE_PORT":
		default:
			f.env = append(f.env, kv)
		}
	}
	f.env = append(f.env,
		"PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"),
		"DOCKER_HOST=unix://"+socket,
		"FAKE_STATE="+f.state,
		"MINISTACK_DSQL_PROJECT="+laneProject,
		"LANE_SCRIPT="+f.script,
	)
	return f
}

func (f *laneFixture) write(path, content string) {
	f.t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		f.t.Fatalf("create %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		f.t.Fatalf("write %s: %v", path, err)
	}
}

// lane runs the script and answers its combined output and exit code.
func (f *laneFixture) lane(mode string, env ...string) (string, int) {
	f.t.Helper()
	cmd := exec.Command("bash", f.script, mode)
	cmd.Env = append(append([]string{}, f.env...), env...)
	out, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	switch {
	case err == nil:
		return string(out), 0
	case errors.As(err, &exit):
		return string(out), exit.ExitCode()
	}
	f.t.Fatalf("run lane %s: %v\n%s", mode, err, out)
	return "", 0
}

// docker calls the fake directly, to set up what the lane then finds.
func (f *laneFixture) docker(args ...string) string {
	f.t.Helper()
	cmd := exec.Command(filepath.Join(f.root, "bin", "docker"), args...)
	cmd.Env = f.env
	out, err := cmd.CombinedOutput()
	if err != nil {
		f.t.Fatalf("fake docker %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

func (f *laneFixture) fail(key string) {
	f.t.Helper()
	f.write(filepath.Join(f.state, "fail", key), "")
}

func (f *laneFixture) hook(suite, script string) {
	f.t.Helper()
	f.write(filepath.Join(f.state, "hooks", suite), script)
}

func (f *laneFixture) artifact(project, name string) string {
	return filepath.Join(f.root, ".artifacts", "dsql-ministack", project, name)
}

// names lists what the fake Docker holds: kind is containers or networks.
func (f *laneFixture) names(kind string) []string {
	f.t.Helper()
	files, err := filepath.Glob(filepath.Join(f.state, kind, "*", "name"))
	if err != nil {
		f.t.Fatalf("list %s: %v", kind, err)
	}
	names := []string{}
	for _, file := range files {
		name, err := os.ReadFile(file)
		if err != nil {
			f.t.Fatalf("read %s: %v", file, err)
		}
		names = append(names, strings.TrimSpace(string(name)))
	}
	return names
}

func (f *laneFixture) calls() string {
	f.t.Helper()
	calls, err := os.ReadFile(filepath.Join(f.state, "calls.log"))
	if err != nil && !os.IsNotExist(err) {
		f.t.Fatalf("read calls: %v", err)
	}
	return string(calls)
}

func (f *laneFixture) assertEmpty(output string) {
	f.t.Helper()
	if left := append(f.names("containers"), f.names("networks")...); len(left) != 0 {
		f.t.Errorf("left behind: %v\n%s", left, output)
	}
}

func (f *laneFixture) assertRan(project string) {
	f.t.Helper()
	summary, err := os.ReadFile(f.artifact(project, "summary.txt"))
	if err != nil {
		f.t.Fatalf("read %s's summary: %v", project, err)
	}
	for suite, passes := range map[string]int{"gate": 1, "entity": 10, "cdc": 9, "coordination": 96} {
		if !strings.Contains(string(summary), fmt.Sprintf("%-14s pass=%-5d", suite, passes)) {
			f.t.Errorf("%s's summary has no %d passes for %s:\n%s", project, passes, suite, summary)
		}
	}
}
