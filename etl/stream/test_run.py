import logging
import signal
from .config import Config
from .test_service import TestStreamService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main() -> None:
    config = Config.from_env()
    service = TestStreamService(config)

    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}")
        service.running = False

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    service.start()


if __name__ == "__main__":
    main()
