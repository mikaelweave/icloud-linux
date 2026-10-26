import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


CLI_PATH = Path(__file__).with_name("icloudctl")


class ICloudCtlIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="icloud-cli-test-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.env = {
            **os.environ,
            "HOME": str(self.root),
            "ICLOUD_CONFIG_DIR": str(self.root / "config"),
            "ICLOUD_STATE_DIR": str(self.root / "state % logs"),
            "ICLOUD_CACHE_DIR": str(self.root / "cache"),
            "ICLOUD_SERVICE_DIR": str(self.root / "services"),
        }

    def run_functions(self, commands, *args):
        return subprocess.run(
            ["bash", "-c", 'source "$1" help >/dev/null; shift; ' + commands,
             "_", str(CLI_PATH), *args],
            env=self.env,
            check=True,
            capture_output=True,
            text=True,
        )

    def test_help_retains_fork_and_upstream_commands(self):
        result = subprocess.run(
            ["bash", str(CLI_PATH), "help"],
            env=self.env,
            check=True,
            capture_output=True,
            text=True,
        )
        for command in ("init-system", "auth-status", "hydrate", "sync", "refresh", "--accept-terms"):
            with self.subTest(command=command):
                self.assertIn(command, result.stdout)

    def test_service_generation_preserves_scope_and_quotes_paths(self):
        for scope, target in (("user", "default.target"), ("system", "multi-user.target")):
            with self.subTest(scope=scope):
                self.run_functions(
                    'set_scope "$1"; ensure_dirs; REPO_DIR="$2"; write_service "$3"',
                    scope,
                    str(self.root / 'repo % "quoted"'),
                    str(self.root / "mount % drive"),
                )
                service = (self.root / "services" / "icloud.service").read_text()
                self.assertIn(f"filesystem ({scope})", service)
                self.assertIn(f"WantedBy={target}", service)
                self.assertIn("RestartPreventExitStatus=2", service)
                self.assertIn(r'repo %% \"quoted\"/.venv/bin/python"', service)
                self.assertIn(f'WorkingDirectory={self.root}/repo %% "quoted"', service)
                self.assertIn(f'Environment="ICLOUD_LOG_PATH={self.root}/state %% logs/icloud.log"', service)
                self.assertIn(f'ExecStopPost=-/usr/bin/fusermount -uz "{self.root}/mount %% drive"', service)

    @unittest.skipUnless(shutil.which("systemd-analyze"), "systemd-analyze is unavailable")
    def test_generated_unit_passes_systemd_verification(self):
        repo = self.root / "repo % build"
        python = repo / ".venv" / "bin" / "python"
        python.parent.mkdir(parents=True)
        python.symlink_to(shutil.which("true"))
        for scope in ("user", "system"):
            with self.subTest(scope=scope):
                self.run_functions(
                    'set_scope "$1"; ensure_dirs; REPO_DIR="$2"; write_service "$3"',
                    scope,
                    str(repo),
                    str(self.root / "mount % drive"),
                )
                result = subprocess.run(
                    ["systemd-analyze", "verify", str(self.root / "services" / "icloud.service")],
                    env=self.env,
                    capture_output=True,
                    text=True,
                )
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_saved_environment_round_trips_mount_path(self):
        mount = str(self.root / "mount space % 'quoted'")
        result = self.run_functions(
            'ensure_dirs; write_env "$1"; unset ICLOUD_MOUNT; '
            'load_env_if_present; printf "%s" "$ICLOUD_MOUNT"',
            mount,
        )
        self.assertEqual(result.stdout, mount)

    def test_refresh_targets_user_service_without_starting_it(self):
        result = self.run_functions(
            'systemctl_run() { printf "%s\\n" "$*"; }; cmd_refresh'
        )
        self.assertIn("kill --kill-whom=main --signal=SIGUSR1 icloud.service", result.stdout)


if __name__ == "__main__":
    unittest.main()
