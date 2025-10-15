import yaml
import os

class ConfigLoader:
    """Class chịu trách nhiệm load và cung cấp cấu hình từ config.yaml"""

    def __init__(self, config_path=None):
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
        self.config_path = config_path or os.path.join(base_dir, "ingestion_config.yaml")
        self._config = None

    def _load_config(self):
        """Đọc file YAML vào dict"""
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Config file not found: {self.config_path}")

        try:
            with open(self.config_path, "r") as f:
                self._config = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML syntax in {self.config_path}") from e

    @property
    def config(self):
        """Truy cập config (lazy load nếu chưa load lần đầu)"""
        if self._config is None:
            self._load_config()
        return self._config

    def get(self, key, default=None):
        """Truy cập nhanh tới key trong config."""
        return self.config.get(key, default)
