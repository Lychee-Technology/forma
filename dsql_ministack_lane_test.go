package forma

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestMiniStackLaneCleansUpWhatItCreated is the fake's own check: the tests below
// mean nothing unless a clean run through it passes and leaves nothing.
func TestMiniStackLaneCleansUpWhatItCreated(t *testing.T) {
	f := newLaneFixture(t)
	output, code := f.lane("run")
	if code != 0 || !strings.Contains(output, "torn down") {
		t.Fatalf("exit %d, want a clean run:\n%s", code, output)
	}
	f.assertEmpty(output)
	f.assertRan(laneProject)
	if !strings.Contains(f.calls(), "-X DELETE") {
		t.Errorf("the cluster was not deleted through MiniStack's API:\n%s", f.calls())
	}
}

// TestMiniStackLaneLeavesAnExistingContainerAlone is R1-F1: a run that finds its
// container name taken removes its own network and nothing else, calls no API on
// a container it did not create, and clears no logs.
func TestMiniStackLaneLeavesAnExistingContainerAlone(t *testing.T) {
	f := newLaneFixture(t)
	sentinel := f.docker("create", "--name", laneProject)
	f.docker("start", sentinel)
	f.write(f.artifact(laneProject, "earlier.log"), "from the holder of the name")

	output, code := f.lane("run")
	if code == 0 {
		t.Fatalf("the run passed with its container name taken:\n%s", output)
	}
	if got := f.names("containers"); len(got) != 1 || got[0] != laneProject {
		t.Errorf("containers = %v, want the existing one untouched:\n%s", got, output)
	}
	if got := f.names("networks"); len(got) != 0 {
		t.Errorf("the run left its own network behind: %v", got)
	}
	for _, call := range strings.Split(f.calls(), "\n") {
		for _, acted := range []string{"docker rm", "docker logs", "docker port", "curl"} {
			if strings.HasPrefix(call, acted) {
				t.Errorf("the run acted on what it did not create: %s", call)
			}
		}
	}
	if _, err := os.Stat(f.artifact(laneProject, "earlier.log")); err != nil {
		t.Errorf("the losing run cleared the logs: %v", err)
	}
}

func TestMiniStackLaneRemovesOnlyItsOwnContainerWhenItCannotStart(t *testing.T) {
	f := newLaneFixture(t)
	f.docker("create", "--name", "unrelated")
	f.fail("docker-start")
	output, code := f.lane("run")
	if code == 0 || !strings.Contains(output, "cannot start MiniStack") {
		t.Fatalf("exit %d, want a failed start:\n%s", code, output)
	}
	if got := f.names("containers"); len(got) != 1 || got[0] != "unrelated" {
		t.Errorf("containers = %v, want only the unrelated one", got)
	}
	if got := f.names("networks"); len(got) != 0 {
		t.Errorf("networks = %v, want none", got)
	}
}

// TestMiniStackLaneLeavesAnExistingNetworkAlone: losing the network claim owns
// nothing, so nothing is removed, and nothing runs that could clear logs.
func TestMiniStackLaneLeavesAnExistingNetworkAlone(t *testing.T) {
	f := newLaneFixture(t)
	f.docker("network", "create", laneProject)
	f.write(f.artifact(laneProject, "earlier.log"), "from the holder of the name")

	output, code := f.lane("run")
	if code == 0 || strings.Contains(output, "torn down") {
		t.Fatalf("exit %d, want a refusal that tears nothing down:\n%s", code, output)
	}
	if got := f.names("networks"); len(got) != 1 {
		t.Errorf("networks = %v, want the existing one", got)
	}
	if _, err := os.Stat(f.artifact(laneProject, "earlier.log")); err != nil {
		t.Errorf("the losing run cleared the logs: %v", err)
	}
}

// TestMiniStackLaneOverlappingRuns is R1-F2: a second invocation starts halfway
// through the first's entity suite. Whatever it is configured with, the first
// keeps its logs and its true counts.
func TestMiniStackLaneOverlappingRuns(t *testing.T) {
	cases := []struct {
		name, env string
		wantPass  bool
		project   string
	}{
		{name: "same configuration", env: ""},
		{name: "same project on another network", env: "DSQL_DOCKER_NETWORK=lane-other-net"},
		{
			name:     "independent project",
			env:      "MINISTACK_DSQL_PROJECT=lane-other DSQL_BASE_PORT=25999",
			wantPass: true,
			project:  "lane-other",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newLaneFixture(t)
			second := filepath.Join(f.state, "second")
			f.hook("entity", fmt.Sprintf("%s bash \"$LANE_SCRIPT\" run >%q 2>&1; echo $? >%q\n", tc.env, second+".out", second+".rc"))

			output, code := f.lane("run")
			if code != 0 {
				t.Fatalf("the first run failed, exit %d:\n%s", code, output)
			}
			f.assertRan(laneProject)
			f.assertEmpty(output)
			log, err := os.ReadFile(f.artifact(laneProject, "entity.log"))
			if err != nil {
				t.Fatalf("the first run lost its entity log: %v", err)
			}
			if got := strings.Count(string(log), "--- PASS"); got != 10 {
				t.Errorf("entity log holds %d passes, want 10:\n%s", got, log)
			}

			secondOut, err := os.ReadFile(second + ".out")
			if err != nil {
				t.Fatalf("the second run never ran: %v", err)
			}
			rc, err := os.ReadFile(second + ".rc")
			if err != nil {
				t.Fatalf("read the second run's exit code: %v", err)
			}
			passed := strings.TrimSpace(string(rc)) == "0"
			if passed != tc.wantPass {
				t.Errorf("second run exit %s, want passed=%v:\n%s", rc, tc.wantPass, secondOut)
			}
			if tc.project != "" {
				f.assertRan(tc.project)
			}
		})
	}
}

// TestMiniStackLaneFailsWhenLogsAreLost is the other half of R1-F2: a suite whose
// results cannot be read must not pass with blank counts.
func TestMiniStackLaneFailsWhenLogsAreLost(t *testing.T) {
	for name, target := range map[string]string{
		"its log":          "entity.log",
		"the whole folder": "",
	} {
		t.Run(name, func(t *testing.T) {
			f := newLaneFixture(t)
			f.hook("entity", fmt.Sprintf("rm -rf %q\n", f.artifact(laneProject, target)))
			output, code := f.lane("run")
			if code == 0 {
				t.Fatalf("the run passed having lost %s:\n%s", name, output)
			}
			if !strings.Contains(output, "cannot read its results") {
				t.Errorf("the failure does not name the lost results:\n%s", output)
			}
			f.assertEmpty(output)
		})
	}
}

// TestMiniStackLaneFailsWhenDockerGoesAway is R1-F3: suites pass, then Docker
// stops answering. Nothing can be confirmed removed, so the lane fails, and once
// Docker is back the documented recovery removes what stayed.
func TestMiniStackLaneFailsWhenDockerGoesAway(t *testing.T) {
	f := newLaneFixture(t)
	f.hook("coordination", fmt.Sprintf("touch %q\n", filepath.Join(f.state, "fail", "docker-all")))
	output, code := f.lane("run")
	if code == 0 || strings.Contains(output, "torn down") {
		t.Fatalf("exit %d, want a failure that claims no teardown:\n%s", code, output)
	}
	if !strings.Contains(output, "cleanup was incomplete or could not be confirmed") {
		t.Errorf("the failure does not name the cleanup:\n%s", output)
	}
	if len(f.names("containers")) == 0 || len(f.names("networks")) == 0 {
		t.Fatalf("the fake let resources go with Docker down: the test proves nothing")
	}

	if err := os.Remove(filepath.Join(f.state, "fail", "docker-all")); err != nil {
		t.Fatalf("bring Docker back: %v", err)
	}
	output, code = f.lane("down")
	if code != 0 {
		t.Fatalf("recovery failed, exit %d:\n%s", code, output)
	}
	f.assertEmpty(output)
}

// TestMiniStackLaneReportsEachCleanupFailureAndKeepsGoing: one failing step fails
// the run and does not stop the steps after it.
func TestMiniStackLaneReportsEachCleanupFailureAndKeepsGoing(t *testing.T) {
	cases := []struct {
		key            string
		wantContainers []string
		wantNetworks   int
	}{
		// With no listing nothing can be confirmed, but removal is still tried,
		// and works: the run fails with nothing left.
		{key: "docker-ps"},
		{key: "docker-network-ls"},
		{key: "docker-network-rm", wantNetworks: 1},
		{key: "docker-rm", wantContainers: []string{"ministack-dsql-", laneProject}, wantNetworks: 1},
		{key: "docker-logs"},
		// The API refuses, and the backend still goes, by label on the network.
		{key: "curl-delete"},
	}
	for _, tc := range cases {
		t.Run(tc.key, func(t *testing.T) {
			f := newLaneFixture(t)
			f.hook("coordination", fmt.Sprintf("touch %q\n", filepath.Join(f.state, "fail", tc.key)))
			output, code := f.lane("run")
			if code == 0 {
				t.Fatalf("the run passed with %s failing:\n%s", tc.key, output)
			}
			if tc.key != "docker-logs" && strings.Contains(output, "torn down") {
				t.Errorf("claims a teardown with %s failing:\n%s", tc.key, output)
			}
			got := f.names("containers")
			if len(got) != len(tc.wantContainers) {
				t.Errorf("containers = %v, want %v:\n%s", got, tc.wantContainers, output)
			}
			if got := f.names("networks"); len(got) != tc.wantNetworks {
				t.Errorf("networks = %v, want %d:\n%s", got, tc.wantNetworks, output)
			}
		})
	}
}

// TestMiniStackLaneDownRemovesOnlyTheLanesOwn: 'down' has no IDs from an earlier
// process, so the label decides.
func TestMiniStackLaneDownRemovesOnlyTheLanesOwn(t *testing.T) {
	owned := laneLabel + "=" + laneProject

	t.Run("left by an interrupted run", func(t *testing.T) {
		f := newLaneFixture(t)
		f.docker("network", "create", "--label", owned, laneProject)
		f.docker("create", "--name", "ministack-dsql-x", "--label", "ministack=dsql", "--network", laneProject)
		f.docker("create", "--name", laneProject, "--label", owned, "--network", laneProject)
		output, code := f.lane("down")
		if code != 0 {
			t.Fatalf("exit %d:\n%s", code, output)
		}
		f.assertEmpty(output)
	})

	for name, mark := range map[string][]string{
		"unlabeled":         {},
		"another project's": {"--label", laneLabel + "=another"},
	} {
		t.Run(name, func(t *testing.T) {
			f := newLaneFixture(t)
			f.docker(append([]string{"network", "create"}, append(mark, laneProject)...)...)
			f.docker(append([]string{"create", "--name", laneProject}, mark...)...)
			output, code := f.lane("down")
			if code == 0 {
				t.Errorf("reported success with a foreign container in the way:\n%s", output)
			}
			if len(f.names("containers")) != 1 || len(f.names("networks")) != 1 {
				t.Errorf("down removed what is not the lane's:\n%s", output)
			}
		})
	}

	t.Run("Docker cannot answer", func(t *testing.T) {
		f := newLaneFixture(t)
		f.docker("create", "--name", laneProject, "--label", owned)
		f.fail("docker-ps")
		output, code := f.lane("down")
		if code == 0 || strings.Contains(output, "torn down") {
			t.Errorf("exit %d, want a failure:\n%s", code, output)
		}
	})

	// One failure each, with no later step left to notice it instead. Only the
	// lane's network or container exists, so nothing else can fail on its account.
	for _, tc := range []struct{ name, key, kind string }{
		{"backends cannot be listed", "docker-ps-backends", "network"},
		{"container cannot be removed", "docker-rm", "container"},
		{"network cannot be removed", "docker-network-rm", "network"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newLaneFixture(t)
			if tc.kind == "network" {
				f.docker("network", "create", "--label", owned, laneProject)
			} else {
				f.docker("create", "--name", laneProject, "--label", owned)
			}
			f.fail(tc.key)
			output, code := f.lane("down")
			if code == 0 || strings.Contains(output, "torn down") {
				t.Errorf("exit %d, want a failure:\n%s", code, output)
			}
		})
	}
}

// A project names a path the lane clears, so it cannot be allowed to climb out.
func TestMiniStackLaneRefusesAProjectThatIsNotAName(t *testing.T) {
	f := newLaneFixture(t)
	precious := filepath.Join(f.root, ".artifacts", "precious")
	f.write(precious, "keep")
	output, code := f.lane("run", "MINISTACK_DSQL_PROJECT=../../.artifacts")
	if code == 0 || !strings.Contains(output, "not a valid container name") {
		t.Errorf("exit %d, want a refusal:\n%s", code, output)
	}
	if f.calls() != "" {
		t.Errorf("called tools before refusing: %s", f.calls())
	}
	if _, err := os.Stat(precious); err != nil {
		t.Errorf("a file outside the lane's directory went: %v", err)
	}
}
