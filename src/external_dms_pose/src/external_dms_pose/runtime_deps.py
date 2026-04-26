from pathlib import Path
import sys
from typing import Optional


def inject_site_packages(venv_root: str = "/home/vci/venvs/openvino_env") -> Optional[str]:
    py_version = f"python{sys.version_info.major}.{sys.version_info.minor}"
    site_packages = Path(venv_root) / "lib" / py_version / "site-packages"
    if not site_packages.exists():
        return None

    site_packages_str = str(site_packages)
    if site_packages_str not in sys.path:
        sys.path.insert(0, site_packages_str)
    return site_packages_str
