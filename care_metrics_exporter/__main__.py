"""Console entry point for the CARE metrics exporter."""

from __future__ import annotations

import logging

from care_metrics_exporter.config import ConfigurationError, Settings
from care_metrics_exporter.server import configure_logging, run

logger = logging.getLogger(__name__)


def main() -> int:
    """Validate configuration, then serve metrics until terminated."""
    try:
        settings = Settings.from_env()
    except ConfigurationError as error:
        configure_logging("INFO")
        # ``error`` is built to never contain the broker URL or its credentials.
        logger.error("invalid exporter configuration: %s", error)
        return 2

    configure_logging(settings.log_level)
    try:
        run(settings)
    except ConfigurationError as error:
        logger.error("invalid exporter configuration: %s", error)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
