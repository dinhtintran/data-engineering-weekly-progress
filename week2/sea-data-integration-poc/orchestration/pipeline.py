import yaml
from pathlib import Path
from typing import Dict, Any
import sys
import logging

# Ensure project root is on sys.path so sibling folders import correctly
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from ingestion.worldbank_gdp_per_capita import main as ingest_main
from transform.worldbank_transform import main as transform_main


def setup_logging(config: Dict[str, Any]) -> None:
    """
    Configure the root logger using values in the provided configuration.
    
    Parameters:
        config (Dict[str, Any]): Configuration mapping containing an optional "logging" section with:
            - "level": log level name (e.g., "INFO"); defaults to "INFO".
            - "to_file": if truthy, write logs to a file; otherwise log to stdout.
            - "file_path": destination path for file logging when "to_file" is truthy; defaults to "orchestration/pipeline.log".
    
    This function clears existing root handlers, sets the root logger level, applies a standard formatter, and attaches either a FileHandler or a StreamHandler based on the "logging" settings.
    """
    log_cfg = config.get("logging", {})
    level_name = str(log_cfg.get("level", "INFO")).upper()
    level = getattr(logging, level_name, logging.INFO)
    root = logging.getLogger()
    # Clear existing handlers to avoid duplicate logs across runs
    for h in list(root.handlers):
        root.removeHandler(h)
    root.setLevel(level)
    formatter = logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
    if log_cfg.get("to_file", False):
        file_path = log_cfg.get("file_path", "orchestration/pipeline.log")
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(file_path)
        fh.setLevel(level)
        fh.setFormatter(formatter)
        root.addHandler(fh)
    else:
        sh = logging.StreamHandler(sys.stdout)
        sh.setLevel(level)
        sh.setFormatter(formatter)
        root.addHandler(sh)


def load_config(path: Path) -> Dict[str, Any]:
    """
    Load a YAML configuration file and return its contents as a dictionary.
    
    Parameters:
        path (Path): Filesystem path to the YAML configuration file to read.
    
    Returns:
        Dict[str, Any]: Configuration mapping parsed from the YAML file.
    """
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def run() -> None:
    """
    Orchestrates the pipeline by loading configuration, initializing logging, and executing ingestion and transformation stages.
    
    Loads configuration from a config.yaml adjacent to this module, sets up logging according to that configuration, runs the ingestion stage followed by the transformation stage, and emits progress logs for each major step.
    """
    base_dir = Path(__file__).resolve().parent
    cfg_path = base_dir / "config.yaml"
    config = load_config(cfg_path)
    setup_logging(config)
    logger = logging.getLogger("orchestration.pipeline")
    logger.info("Starting pipeline")
    logger.info("Ingestion step started")
    ingest_main(config)
    logger.info("Ingestion step completed")
    logger.info("Transform step started")
    transform_main(config)
    logger.info("Transform step completed")
    logger.info("Pipeline finished successfully")


if __name__ == "__main__":
    run()