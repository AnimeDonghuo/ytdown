import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ytdown.metrics import ContainerMetrics


class MetricTests(unittest.TestCase):
    def test_uses_cgroup_container_cpu_and_ram_not_process_rss(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / "cpu.stat").write_text("usage_usec 1000000\n")
            (root / "cpu.max").write_text("100000 100000\n")
            (root / "memory.current").write_text(str(128 * 1024 * 1024))
            (root / "memory.max").write_text(str(512 * 1024 * 1024))
            with patch("ytdown.metrics.time.monotonic", side_effect=[100, 110]):
                metrics = ContainerMetrics(root)
                (root / "cpu.stat").write_text("usage_usec 6000000\n")
                text = metrics.describe()
        self.assertIn("Container CPU: 50% of quota", text)
        self.assertIn("RAM: 128.0 MiB / 512.0 MiB", text)

    def test_unavailable_cgroup_is_not_called_koyeb_usage(self):
        with tempfile.TemporaryDirectory() as folder:
            self.assertEqual(ContainerMetrics(Path(folder)).describe(), "Container CPU/RAM: unavailable")
