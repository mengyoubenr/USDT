import json
import logging
import os
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


TRANSFER_EVENT_TOPIC = (
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
)
SYMBOL_SELECTOR = "0x95d89b41"
DECIMALS_SELECTOR = "0x313ce567"
SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
DEFAULT_STATE_FILE = "bsc_state.json"
BSC_USDT_CONTRACT = "0x55d398326f99059ff775485246999027b3197955"
BSC_USDC_CONTRACT = "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d"
SUPPORTED_BEP20_TOKENS = {
    BSC_USDT_CONTRACT: "USDT-BEP20",
    BSC_USDC_CONTRACT: "USDC-BEP20",
}


def load_env_file(path: Path) -> None:
    if not path.exists():
        return

    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        if not key or key in os.environ:
            continue
        os.environ[key] = value.strip().strip('"').strip("'")


def normalize_address(address: str) -> str:
    normalized = address.strip().lower()
    if not normalized.startswith("0x") or len(normalized) != 42:
        raise ValueError(f"invalid address: {address}")
    int(normalized[2:], 16)
    return normalized


def parse_address_list(value: str) -> set[str]:
    addresses = set()
    for item in value.split(","):
        stripped = item.strip()
        if not stripped:
            continue
        addresses.add(normalize_address(stripped))
    return addresses


def parse_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def topic_for_address(address: str) -> str:
    return "0x" + ("0" * 24) + normalize_address(address)[2:]


def decode_topic_address(topic: str) -> str:
    hex_value = topic.lower().replace("0x", "")
    return normalize_address("0x" + hex_value[-40:])


def hex_to_int(value: str | None) -> int:
    if not value:
        return 0
    return int(value, 16)


def format_token_amount(raw_value: int, decimals: int) -> str:
    if decimals <= 0:
        return str(raw_value)

    digits = str(raw_value).rjust(decimals + 1, "0")
    whole = digits[:-decimals] or "0"
    fraction = digits[-decimals:].rstrip("0")
    return whole if not fraction else f"{whole}.{fraction}"


def format_shanghai_time(timestamp: int) -> str:
    dt = datetime.fromtimestamp(timestamp, tz=SHANGHAI_TZ)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def decode_abi_string(data: str) -> str | None:
    payload = data[2:] if data.startswith("0x") else data
    if not payload:
        return None

    raw = bytes.fromhex(payload)
    if len(raw) == 32:
        text = raw.rstrip(b"\x00").decode("utf-8", errors="ignore").strip()
        return text or None

    if len(raw) < 64:
        text = raw.decode("utf-8", errors="ignore").strip("\x00").strip()
        return text or None

    offset = int.from_bytes(raw[:32], "big")
    if offset + 32 > len(raw):
        return None

    length = int.from_bytes(raw[offset : offset + 32], "big")
    start = offset + 32
    end = start + length
    if end > len(raw):
        return None

    text = raw[start:end].decode("utf-8", errors="ignore").strip()
    return text or None


@dataclass
class MonitorConfig:
    rpc_url: str
    telegram_token: str
    telegram_chat_id: str
    monitor_addresses: set[str]
    rpc_timeout: float
    rpc_retries: int
    rpc_retry_delay: float
    poll_interval: float
    confirmations: int
    start_block: str
    state_file: Path
    notify_native: bool


@dataclass
class TransferNotification:
    direction: str
    token_symbol: str
    amount: str
    from_address: str
    to_address: str
    tx_hash: str
    timestamp: int


class RpcClient:
    def __init__(
        self,
        url: str,
        timeout: float,
        retries: int,
        retry_delay: float,
    ) -> None:
        self.url = url
        self.timeout = timeout
        self.retries = retries
        self.retry_delay = retry_delay
        self.request_id = 0

    def call(self, method: str, params: list[Any]) -> Any:
        self.request_id += 1
        payload = json.dumps(
            {
                "jsonrpc": "2.0",
                "id": self.request_id,
                "method": method,
                "params": params,
            }
        ).encode("utf-8")
        request = urllib.request.Request(
            self.url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        last_error: Exception | None = None
        for attempt in range(self.retries + 1):
            try:
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    result = json.loads(response.read().decode("utf-8"))
                break
            except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
                last_error = exc
                if attempt >= self.retries:
                    raise RuntimeError(f"rpc request failed for {method}: {exc}") from exc
                logging.warning(
                    "bsc rpc call %s failed, retrying (%s/%s): %s",
                    method,
                    attempt + 1,
                    self.retries,
                    exc,
                )
                time.sleep(self.retry_delay)
        else:
            raise RuntimeError(f"rpc request failed for {method}: {last_error}")

        if result.get("error"):
            raise RuntimeError(f"rpc error for {method}: {result['error']}")
        return result["result"]


class TelegramNotifier:
    def __init__(self, token: str, chat_id: str) -> None:
        self.token = token
        self.chat_id = chat_id

    def send(self, message: str) -> None:
        payload = json.dumps(
            {
                "chat_id": self.chat_id,
                "text": message,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True,
            }
        ).encode("utf-8")
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        request = urllib.request.Request(
            url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                result = json.loads(response.read().decode("utf-8"))
        except urllib.error.URLError as exc:
            raise RuntimeError(f"telegram request failed: {exc}") from exc

        if not result.get("ok"):
            raise RuntimeError(f"telegram api error: {result}")


class BscMonitor:
    def __init__(self, config: MonitorConfig) -> None:
        self.config = config
        self.rpc = RpcClient(
            config.rpc_url,
            timeout=config.rpc_timeout,
            retries=config.rpc_retries,
            retry_delay=config.rpc_retry_delay,
        )
        self.notifier = TelegramNotifier(
            config.telegram_token,
            config.telegram_chat_id,
        )
        self.token_cache: dict[str, tuple[str, int]] = {}
        self.tx_success_cache: dict[str, bool] = {}
        self.monitored_topics = [topic_for_address(addr) for addr in config.monitor_addresses]
        self.token_contracts = set(SUPPORTED_BEP20_TOKENS)

    def run(self) -> None:
        last_processed = self._resolve_last_processed_block()
        logging.info("start monitoring from block %s", last_processed + 1)

        while True:
            try:
                chain_head = self._get_latest_block_number()
                target_block = chain_head - self.config.confirmations
                if target_block <= last_processed:
                    time.sleep(self.config.poll_interval)
                    continue

                for block_number in range(last_processed + 1, target_block + 1):
                    self._process_block(block_number)
                    self._save_state(block_number)
                    last_processed = block_number
            except KeyboardInterrupt:
                logging.info("monitor stopped by user")
                raise
            except Exception as exc:  # noqa: BLE001
                logging.exception("monitor loop failed: %s", exc)
                time.sleep(self.config.poll_interval)

    def _resolve_last_processed_block(self) -> int:
        state = self._load_state()
        saved_block = state.get("last_processed_block")
        if isinstance(saved_block, int):
            return saved_block

        latest_block = self._get_latest_block_number()
        target_block = max(latest_block - self.config.confirmations, 0)
        start_block = self.config.start_block.strip().lower()
        if start_block == "latest":
            self._save_state(target_block)
            return target_block

        configured_block = int(self.config.start_block)
        initial_block = max(configured_block - 1, 0)
        self._save_state(initial_block)
        return initial_block

    def _load_state(self) -> dict[str, Any]:
        if not self.config.state_file.exists():
            return {}
        try:
            return json.loads(self.config.state_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            logging.warning("failed to read state file %s: %s", self.config.state_file, exc)
            return {}

    def _save_state(self, block_number: int) -> None:
        self.config.state_file.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps({"last_processed_block": block_number}, indent=2)
        self.config.state_file.write_text(payload, encoding="utf-8")

    def _get_latest_block_number(self) -> int:
        return hex_to_int(self.rpc.call("eth_blockNumber", []))

    def _get_block(self, block_number: int) -> dict[str, Any]:
        return self.rpc.call("eth_getBlockByNumber", [hex(block_number), True])

    def _process_block(self, block_number: int) -> None:
        block = self._get_block(block_number)
        if not block:
            raise RuntimeError(f"block {block_number} not found")

        timestamp = hex_to_int(block.get("timestamp"))
        notifications = []
        notifications.extend(self._collect_native_transfers(block, timestamp))
        notifications.extend(self._collect_token_transfers(block_number, timestamp))

        for notification in notifications:
            message = self._format_message(notification)
            self.notifier.send(message)
            logging.info(
                "sent %s %s tx=%s",
                notification.direction,
                notification.token_symbol,
                notification.tx_hash,
            )

    def _collect_native_transfers(
        self,
        block: dict[str, Any],
        timestamp: int,
    ) -> list[TransferNotification]:
        if not self.config.notify_native:
            return []

        notifications: list[TransferNotification] = []
        for tx in block.get("transactions", []):
            if hex_to_int(tx.get("value")) <= 0:
                continue

            from_address = normalize_address(tx["from"])
            to_raw = tx.get("to")
            if not to_raw:
                continue

            to_address = normalize_address(to_raw)
            from_monitored = from_address in self.config.monitor_addresses
            to_monitored = to_address in self.config.monitor_addresses
            if not from_monitored and not to_monitored:
                continue

            tx_hash = tx["hash"]
            if not self._is_successful_transaction(tx_hash):
                continue

            amount = format_token_amount(hex_to_int(tx["value"]), 18)
            if from_monitored:
                notifications.append(
                    TransferNotification(
                        direction="出账",
                        token_symbol="BNB",
                        amount=amount,
                        from_address=from_address,
                        to_address=to_address,
                        tx_hash=tx_hash,
                        timestamp=timestamp,
                    )
                )
            if to_monitored and to_address != from_address:
                notifications.append(
                    TransferNotification(
                        direction="收款",
                        token_symbol="BNB",
                        amount=amount,
                        from_address=from_address,
                        to_address=to_address,
                        tx_hash=tx_hash,
                        timestamp=timestamp,
                    )
                )

        return notifications

    def _collect_token_transfers(
        self,
        block_number: int,
        timestamp: int,
    ) -> list[TransferNotification]:
        base_filter: dict[str, Any] = {
            "fromBlock": hex(block_number),
            "toBlock": hex(block_number),
            "address": sorted(self.token_contracts),
        }

        filters = [
            {
                **base_filter,
                "topics": [TRANSFER_EVENT_TOPIC, self.monitored_topics],
            },
            {
                **base_filter,
                "topics": [TRANSFER_EVENT_TOPIC, None, self.monitored_topics],
            },
        ]

        unique_logs: dict[str, dict[str, Any]] = {}
        for filter_params in filters:
            logs = self.rpc.call("eth_getLogs", [filter_params])
            for log in logs:
                key = f"{log['transactionHash']}:{log.get('logIndex', '0x0')}"
                unique_logs[key] = log

        notifications: list[TransferNotification] = []
        for log in unique_logs.values():
            topics = log.get("topics", [])
            if len(topics) < 3 or topics[0].lower() != TRANSFER_EVENT_TOPIC:
                continue

            contract_address = normalize_address(log["address"])
            if contract_address not in self.token_contracts:
                continue

            from_address = decode_topic_address(topics[1])
            to_address = decode_topic_address(topics[2])
            raw_amount = hex_to_int(log.get("data"))
            token_symbol, decimals = self._get_token_metadata(contract_address)
            token_symbol = self._get_token_label(contract_address, token_symbol)
            amount = format_token_amount(raw_amount, decimals)
            tx_hash = log["transactionHash"]

            if from_address in self.config.monitor_addresses:
                notifications.append(
                    TransferNotification(
                        direction="出账",
                        token_symbol=token_symbol,
                        amount=amount,
                        from_address=from_address,
                        to_address=to_address,
                        tx_hash=tx_hash,
                        timestamp=timestamp,
                    )
                )
            if to_address in self.config.monitor_addresses and to_address != from_address:
                notifications.append(
                    TransferNotification(
                        direction="收款",
                        token_symbol=token_symbol,
                        amount=amount,
                        from_address=from_address,
                        to_address=to_address,
                        tx_hash=tx_hash,
                        timestamp=timestamp,
                    )
                )

        return notifications

    def _is_successful_transaction(self, tx_hash: str) -> bool:
        if tx_hash in self.tx_success_cache:
            return self.tx_success_cache[tx_hash]

        receipt = self.rpc.call("eth_getTransactionReceipt", [tx_hash])
        status = hex_to_int(receipt.get("status"))
        success = status == 1
        self.tx_success_cache[tx_hash] = success
        return success

    def _get_token_metadata(self, contract_address: str) -> tuple[str, int]:
        normalized = normalize_address(contract_address)
        if normalized in self.token_cache:
            return self.token_cache[normalized]

        symbol = self._eth_call_string(normalized, SYMBOL_SELECTOR) or "TOKEN"
        decimals = self._eth_call_uint(normalized, DECIMALS_SELECTOR, default=18)
        metadata = (symbol, decimals)
        self.token_cache[normalized] = metadata
        return metadata

    def _get_token_label(self, contract_address: str, fallback: str) -> str:
        normalized = normalize_address(contract_address)
        return SUPPORTED_BEP20_TOKENS.get(normalized, fallback)

    def _eth_call_string(self, contract_address: str, selector: str) -> str | None:
        try:
            result = self.rpc.call(
                "eth_call",
                [{"to": contract_address, "data": selector}, "latest"],
            )
        except Exception:  # noqa: BLE001
            return None
        return decode_abi_string(result)

    def _eth_call_uint(self, contract_address: str, selector: str, default: int) -> int:
        try:
            result = self.rpc.call(
                "eth_call",
                [{"to": contract_address, "data": selector}, "latest"],
            )
        except Exception:  # noqa: BLE001
            return default

        if not result or result == "0x":
            return default
        return int(result, 16)

    def _format_message(self, notification: TransferNotification) -> str:
        return "\n".join(
            [
                f"*交易类型*：`#{notification.direction}`",
                f"*交易币种*：`#{notification.token_symbol}`",
                f"*交易金额*：`{notification.amount}{notification.token_symbol}`",
                f"*出账地址*：`{notification.from_address}`",
                f"*入账地址*：`{notification.to_address}`",
                f"*交易时间*：`{format_shanghai_time(notification.timestamp)}`",
                f"*交易哈希*：`{notification.tx_hash}`",
            ]
        )


def load_config() -> MonitorConfig:
    load_env_file(Path(".env"))

    telegram_token = (
        os.getenv("telergam_bot_token")
        or os.getenv("telegram_bot_token")
        or ""
    ).strip()
    telegram_chat_id = (os.getenv("tg_chat_id") or "").strip()
    rpc_url = (os.getenv("BSC_RPC") or "").strip()
    monitor_addresses_raw = (os.getenv("BSC_MONITOR_ADDRESSES") or "").strip()

    missing_fields = []
    if not telegram_token:
        missing_fields.append("telergam_bot_token")
    if not telegram_chat_id:
        missing_fields.append("tg_chat_id")
    if not rpc_url:
        missing_fields.append("BSC_RPC")
    if not monitor_addresses_raw:
        missing_fields.append("BSC_MONITOR_ADDRESSES")

    if missing_fields:
        missing = ", ".join(missing_fields)
        raise SystemExit(f"missing required config in .env: {missing}")

    try:
        monitor_addresses = parse_address_list(monitor_addresses_raw)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

    return MonitorConfig(
        rpc_url=rpc_url,
        telegram_token=telegram_token,
        telegram_chat_id=telegram_chat_id,
        monitor_addresses=monitor_addresses,
        rpc_timeout=float(os.getenv("BSC_RPC_TIMEOUT", "15")),
        rpc_retries=int(os.getenv("BSC_RPC_RETRIES", "2")),
        rpc_retry_delay=float(os.getenv("BSC_RPC_RETRY_DELAY", "1")),
        poll_interval=float(os.getenv("BSC_POLL_INTERVAL", "3")),
        confirmations=int(os.getenv("BSC_CONFIRMATIONS", "1")),
        start_block=os.getenv("BSC_START_BLOCK", "latest"),
        state_file=Path(os.getenv("BSC_STATE_FILE", DEFAULT_STATE_FILE)),
        notify_native=parse_bool(os.getenv("BSC_NOTIFY_NATIVE"), default=True),
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    config = load_config()
    monitor = BscMonitor(config)
    monitor.run()


if __name__ == "__main__":
    main()
