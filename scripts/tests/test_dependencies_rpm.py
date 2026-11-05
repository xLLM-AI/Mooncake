"""Exercise dependency selection with package managers and installs mocked."""

import os
from pathlib import Path
import subprocess
import unittest


SCRIPT = (Path(__file__).resolve().parents[2] / "dependencies.sh").read_text()
HELPERS = SCRIPT.split("if [ $(id -u) -ne 0 ]; then", 1)[0]
PACKAGES = SCRIPT.split("# Install system packages\n", 1)[1].split(
    "# Initialize and update git submodules\n", 1
)[0]
RPM_SYSTEMS = ("centos", "rhel", "rocky", "almalinux", "euleros", "openeuler", "kylin")
MOCKS = r"""
yum() { printf 'yum: %s\n' "$*"; }
dnf() { printf 'dnf: %s\n' "$*"; }
apt-get() { printf 'apt-get: %s\n' "$*"; }
apt-cache() { return 0; }
has_shca_tools() { [ "$MOCK_SHCA" = 1 ]; }
install_msgpack_cxx_headers() { printf 'msgpack-cxx fallback checked\n'; }
"""


class RpmDependencySelectionTest(unittest.TestCase):
    def run_selection(self, system, shca=False):
        env = dict(os.environ, OS=system, MOCK_SHCA="1" if shca else "0")
        env["KYLIN_EPKL_URL"] = "https://example.invalid/test-repository"
        result = subprocess.run(
            [
                "bash",
                "-c",
                HELPERS + MOCKS + PACKAGES + '\necho "PACKAGES: $SYSTEM_PACKAGES"',
            ],
            env=env,
            text=True,
            capture_output=True,
            check=True,
        )
        package_line = next(
            line for line in result.stdout.splitlines() if line.startswith("PACKAGES: ")
        )
        return package_line.split()[1:], result.stdout

    def test_required_rpm_headers_are_not_dropped(self):
        for system in RPM_SYSTEMS:
            with self.subTest(system=system):
                packages, _ = self.run_selection(system)
                self.assertIn("boost-devel", packages)
                self.assertIn("msgpack-devel", packages)

    def test_every_rpm_system_checks_cpp_header_fallback(self):
        for system in RPM_SYSTEMS:
            with self.subTest(system=system):
                _, output = self.run_selection(system)
                self.assertIn("msgpack-cxx fallback checked", output)

    def test_skip_broken_and_shca_handling_are_preserved(self):
        for system in RPM_SYSTEMS[:-1]:
            with self.subTest(system=system):
                packages, output = self.run_selection(system, shca=True)
                self.assertNotIn("rdma-core-devel", packages)
                self.assertIn("boost-devel", packages)
                self.assertIn("yum: install -y --skip-broken", output)

    def test_debian_family_keeps_native_packages_without_git_fallback(self):
        for system in ("ubuntu", "debian"):
            with self.subTest(system=system):
                packages, output = self.run_selection(system)
                self.assertIn("libboost-all-dev", packages)
                self.assertIn("libmsgpack-cxx-dev", packages)
                self.assertNotIn("msgpack-cxx fallback checked", output)


if __name__ == "__main__":
    unittest.main()
