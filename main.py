import logging
import os
import threading
from pathlib import Path

from arbitrum import load_config as arbitrum_load_config
from arbitrum import main as arbitrum_main
from bsc import load_config as bsc_load_config
from bsc import main as bsc_main
from polygon import load_config as polygon_load_config
from polygon import main as polygon_main
from tron import load_config as tron_load_config
from tron import main as tron_main


RUNNERS = {
    "arbitrum": (arbitrum_main, arbitrum_load_config),
    "bsc": (bsc_main, bsc_load_config),
    "polygon": (polygon_main, polygon_load_config),
    "tron": (tron_main, tron_load_config),
}


def load_env_file(path: Path, override: bool = False) -> None:
    if not path.exists():
        return

    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        if not key:
            continue
        if not override and key in os.environ:
            continue
        os.environ[key] = value.strip().strip('"').strip("'")


def parse_monitor_chains(value: str) -> list[str]:
    raw_items = [item.strip().lower() for item in value.split(",") if item.strip()]
    if not raw_items:
        return ["bsc"]
    if "all" in raw_items:
        return list(RUNNERS)

    selected: list[str] = []
    for item in raw_items:
        if item not in RUNNERS:
            supported = ", ".join(list(RUNNERS) + ["all"])
            raise SystemExit(f"unsupported MONITOR_CHAIN: {item}. supported: {supported}")
        if item not in selected:
            selected.append(item)
    return selected


def run_chain(chain: str) -> None:
    runner, _ = RUNNERS[chain]
    try:
        runner()
    except KeyboardInterrupt:
        logging.info("monitor %s stopped by user", chain)
        raise
    except BaseException:  # noqa: BLE001
        logging.exception("monitor %s stopped unexpectedly", chain)
        raise


def main() -> None:
    load_env_file(Path(".env"), override=True)
    chains = parse_monitor_chains(os.getenv("MONITOR_CHAIN") or "bsc")

    for chain in chains:
        _, load_config = RUNNERS[chain]
        load_config()

    try:
        if len(chains) == 1:
            run_chain(chains[0])
            return

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
        )
        logging.info("starting monitors: %s", ", ".join(chains))

        threads = []
        for chain in chains:
            thread = threading.Thread(
                target=run_chain,
                args=(chain,),
                name=f"{chain}-monitor",
                daemon=False,
            )
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()
    except KeyboardInterrupt:
        logging.info("all monitors stopped by user")


if __name__ == "__main__":
    main()
