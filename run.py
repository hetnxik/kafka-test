import signal
import logging
from config import Config
from service import EventStreamService

logger = logging.getLogger(__name__)


def main() -> None:
    config = Config.from_env()
    service = EventStreamService(config)

    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}")
        service.running = False

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    service.start()


if __name__ == "__main__":
    main()
