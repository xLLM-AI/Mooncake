"""Run the sourceable production installer with mocked package managers."""

import os
from pathlib import Path
import subprocess
import unittest


SCRIPT = Path(__file__).resolve().parents[2] / "dependencies.sh"
RPM_SYSTEMS = ("centos", "rhel", "rocky", "almalinux", "euleros", "openeuler", "kylin")
MOCKS = r"""
package_manager() {
    printf 'INSTALL: %s\n' "$*"
    case " $* " in
        *" glog-devel "*) [ "$FAIL_REQUIRED" != 1 ] ;;
        *" boost1.78-devel "*) [ "$FAIL_BOOST178" != 1 ] ;;
        *" boost-devel "*) [ "$FAIL_BOOST" != 1 ] ;;
        *" msgpack-devel "*|*" cli11-devel "*) [ "$FAIL_OPTIONAL" != 1 ] ;;
        *) return 0 ;;
    esac
}
yum() { package_manager yum "$@"; }
dnf() { package_manager dnf "$@"; }
apt-get() { package_manager apt-get "$@"; }
apt-cache() { return 0; }
has_shca_tools() { [ "$MOCK_SHCA" = 1 ]; }
install_msgpack_cxx_headers() { printf 'msgpack-cxx fallback checked\n'; }
"""


class RpmDependencySelectionTest(unittest.TestCase):
    def run_selection(self, system, shca=False, **failures):
        env = dict(os.environ, OS=system, MOCK_SHCA="1" if shca else "0")
        for name in ("FAIL_REQUIRED", "FAIL_BOOST178", "FAIL_BOOST", "FAIL_OPTIONAL"):
            env[name] = "1" if failures.get(name, False) else "0"
        env["KYLIN_EPKL_URL"] = "https://example.invalid/test-repository"
        return subprocess.run(
            [
                "bash",
                "-c",
                'source "$1"\n' + MOCKS + "\ninstall_system_packages",
                "test",
                str(SCRIPT),
            ],
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )

    def installs(self, result):
        return [
            line.split()[1:]
            for line in result.stdout.splitlines()
            if line.startswith("INSTALL: ")
        ]

    def test_required_rpm_headers_are_not_dropped(self):
        for system in RPM_SYSTEMS:
            with self.subTest(system=system):
                result = self.run_selection(system)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                required = self.installs(result)[0]
                for package in (
                    "glog-devel",
                    "protobuf-devel",
                    "numactl-devel",
                    "rdma-core-devel",
                ):
                    self.assertIn(package, required)
                self.assertNotIn("msgpack-devel", required)
                self.assertNotIn("--skip-broken", required)
                self.assertIn("msgpack-cxx fallback checked", result.stdout)

    def test_existing_rhel_family_shca_handling_is_preserved(self):
        for system in ("centos", "rhel", "rocky", "almalinux", "euleros", "openeuler"):
            with self.subTest(system=system):
                result = self.run_selection(system, shca=True)
                self.assertEqual(result.returncode, 0)
                self.assertNotIn("rdma-core-devel", self.installs(result)[0])

    def test_kylin_optional_packages_use_the_custom_repository(self):
        result = self.run_selection("kylin", FAIL_OPTIONAL=True)
        self.assertEqual(result.returncode, 0)
        for command in self.installs(result):
            self.assertIn("dnf", command)
            self.assertIn(
                "--repofrompath=kylin-epkl,https://example.invalid/test-repository",
                command,
            )
        self.assertIn("msgpack-cxx fallback checked", result.stdout)

    def test_missing_optional_packages_still_reach_cpp_header_fallback(self):
        for system in RPM_SYSTEMS:
            with self.subTest(system=system):
                result = self.run_selection(system, FAIL_OPTIONAL=True)
                self.assertEqual(result.returncode, 0)
                self.assertIn("msgpack-cxx fallback checked", result.stdout)

    def test_missing_required_package_stops_before_fallback(self):
        for system in RPM_SYSTEMS:
            with self.subTest(system=system):
                result = self.run_selection(system, FAIL_REQUIRED=True)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("Failed to install system packages", result.stdout)
                self.assertNotIn("msgpack-cxx fallback checked", result.stdout)
                self.assertEqual(len(self.installs(result)), 1)

    def test_manylinux_boost_name_and_distribution_fallback(self):
        for system in RPM_SYSTEMS:
            with self.subTest(system=system):
                native = self.run_selection(system)
                commands = self.installs(native)
                self.assertTrue(
                    any("boost1.78-devel" in command for command in commands)
                )
                self.assertFalse(any("boost-devel" in command for command in commands))
                fallback = self.run_selection(system, FAIL_BOOST178=True)
                self.assertEqual(fallback.returncode, 0)
                self.assertTrue(
                    any("boost-devel" in command for command in self.installs(fallback))
                )

    def test_both_boost_names_missing_is_fatal(self):
        for system in RPM_SYSTEMS:
            with self.subTest(system=system):
                result = self.run_selection(system, FAIL_BOOST178=True, FAIL_BOOST=True)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("Failed to install Boost", result.stdout)
                self.assertNotIn("msgpack-cxx fallback checked", result.stdout)

    def test_debian_family_keeps_native_packages_without_git_fallback(self):
        for system in ("ubuntu", "debian"):
            with self.subTest(system=system):
                result = self.run_selection(system)
                self.assertEqual(result.returncode, 0)
                required = self.installs(result)[0]
                self.assertIn("libboost-all-dev", required)
                self.assertIn("libmsgpack-cxx-dev", required)
                self.assertNotIn("msgpack-cxx fallback checked", result.stdout)

    def test_source_does_not_run_main(self):
        result = subprocess.run(
            [
                "bash",
                "-c",
                'source "$1"; declare -F install_system_packages',
                "test",
                str(SCRIPT),
            ],
            text=True,
            capture_output=True,
            check=True,
        )
        self.assertEqual(result.stdout.strip(), "install_system_packages")


if __name__ == "__main__":
    unittest.main()
