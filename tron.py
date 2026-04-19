import hashlib
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


TRANSFER_EVENT_TOPIC = "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
DEFAULT_STATE_FILE = "tron_state.json"
TRON_NATIVE_SYMBOL = "TRX"
TRON_USDT_CONTRACT = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
TRON_USDC_CONTRACT = "TEkxiTehnzSmSe2XqrBj4w32RUN966rdz8"
SUPPORTED_TRC20_TOKENS = {
    TRON_USDT_CONTRACT: ("USDT-TRON", 6),
    TRON_USDC_CONTRACT: ("USDC-TRON", 6),
}
BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


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


def b58encode(payload: bytes) -> str:
    number = int.from_bytes(payload, "big")
    encoded = ""
    while number > 0:
        number, remainder = divmod(number, 58)
        encoded = BASE58_ALPHABET[remainder] + encoded

    leading_zeroes = len(payload) - len(payload.lstrip(b"\x00"))
    return ("1" * leading_zeroes) + (encoded or "1")


def b58decode(value: str) -> bytes:
    number = 0
    for char in value:
        if char not in BASE58_ALPHABET:
            raise ValueError(f"invalid base58 character: {char}")
        number = (number * 58) + BASE58_ALPHABET.index(char)

    decoded = number.to_bytes((number.bit_length() + 7) // 8, "big")
    leading_ones = len(value) - len(value.lstrip("1"))
    return (b"\x00" * leading_ones) + decoded


def tron_hex_to_base58(value: str) -> str:
    normalized = value.strip().lower().replace("0x", "")
    if len(normalized) == 40:
        normalized = "41" + normalized
    if len(normalized) != 42 or not normalized.startswith("41"):
        raise ValueError(f"invalid Tron hex address: {value}")

    payload = bytes.fromhex(normalized)
    checksum = hashlib.sha256(hashlib.sha256(payload).digest()).digest()[:4]
    return b58encode(payload + checksum)


def tron_base58_to_hex(value: str) -> str:
    decoded = b58decode(value.strip())
    if len(decoded) != 25:
        raise ValueError(f"invalid Tron base58 address: {value}")

    payload = decoded[:-4]
    checksum = decoded[-4:]
    expected = hashlib.sha256(hashlib.sha256(payload).digest()).digest()[:4]
    if checksum != expected or payload[0] != 0x41:
        raise ValueError(f"invalid Tron base58 address: {value}")
    return payload.hex()


def normalize_tron_address(address: str) -> str:
    stripped = address.strip()
    if not stripped:
        raise ValueError("empty Tron address")
    if stripped.startswith("T"):
        return tron_hex_to_base58(tron_base58_to_hex(stripped))
    return tron_hex_to_base58(stripped)


def parse_address_list(value: str) -> set[str]:
    addresses = set()
    for item in value.split(","):
        stripped = item.strip()
        if not stripped:
            continue
        addresses.add(normalize_tron_address(stripped))
    return addresses


def parse_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def normalize_topic(topic: str) -> str:
    return topic.lower().replace("0x", "")


def decode_topic_address(topic: str) -> str:
    normalized = normalize_topic(topic)
    return tron_hex_to_base58("41" + normalized[-40:])


def decode_log_address(address: str) -> str:
    normalized = address.lower().replace("0x", "")
    return tron_hex_to_base58(normalized)


def format_token_amount(raw_value: int, decimals: int) -> str:
    if decimals <= 0:
        return str(raw_value)

    digits = str(raw_value).rjust(decimals + 1, "0")
    whole = digits[:-decimals] or "0"
    fraction = digits[-decimals:].rstrip("0")
    return whole if not fraction else f"{whole}.{fraction}"


def format_shanghai_time(timestamp: int) -> str:
    if timestamp > 10_000_000_000:
        timestamp = timestamp // 1000
    dt = datetime.fromtimestamp(timestamp, tz=SHANGHAI_TZ)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


@dataclass
class MonitorConfig:
    rpc_url: str
    api_key: str
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


class TronHttpClient:
    def __init__(
        self,
        url: str,
        api_key: str,
        timeout: float,
        retries: int,
        retry_delay: float,
    ) -> None:
        self.url = url.rstrip("/")
        self.api_key = api_key.strip()
        self.timeout = timeout
        self.retries = retries
        self.retry_delay = retry_delay

    def call(self, path: str, payload: dict[str, Any] | None = None) -> Any:
        request_payload = json.dumps(payload or {}).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["TRON-PRO-API-KEY"] = self.api_key

        request = urllib.request.Request(
            f"{self.url}/{path.lstrip('/')}",
            data=request_payload,
            headers=headers,
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
                    raise RuntimeError(f"tron request failed for {path}: {exc}") from exc
                logging.warning(
                    "tron rpc call %s failed, retrying (%s/%s): %s",
                    path,
                    attempt + 1,
                    self.retries,
                    exc,
                )
                time.sleep(self.retry_delay)
        else:
            raise RuntimeError(f"tron request failed for {path}: {last_error}")

        if isinstance(result, dict) and result.get("Error"):
            raise RuntimeError(f"tron api error for {path}: {result['Error']}")
        return result


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


class TronMonitor:
    def __init__(self, config: MonitorConfig) -> None:
        self.config = config
        self.client = TronHttpClient(
            config.rpc_url,
            config.api_key,
            timeout=config.rpc_timeout,
            retries=config.rpc_retries,
            retry_delay=config.rpc_retry_delay,
        )
        self.notifier = TelegramNotifier(
            config.telegram_token,
            config.telegram_chat_id,
        )
        self.token_contracts = set(SUPPORTED_TRC20_TOKENS)

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
        block = self.client.call("wallet/getnowblock")
        return int(block["block_header"]["raw_data"]["number"])

    def _get_block(self, block_number: int) -> dict[str, Any]:
        return self.client.call("wallet/getblockbynum", {"num": block_number})

    def _get_block_tx_infos(self, block_number: int) -> list[dict[str, Any]]:
        result = self.client.call("wallet/gettransactioninfobyblocknum", {"num": block_number})
        return result if isinstance(result, list) else []

    def _process_block(self, block_number: int) -> None:
        block = self._get_block(block_number)
        if not block:
            raise RuntimeError(f"block {block_number} not found")

        block_timestamp = int(block["block_header"]["raw_data"].get("timestamp", 0))
        tx_infos = self._get_block_tx_infos(block_number)
        tx_info_by_id = {
            info.get("id") or info.get("txid"): info for info in tx_infos if info.get("id") or info.get("txid")
        }

        notifications = []
        notifications.extend(self._collect_native_transfers(block, tx_info_by_id, block_timestamp))
        notifications.extend(self._collect_token_transfers(tx_infos, block_timestamp))

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
        tx_info_by_id: dict[str, dict[str, Any]],
        block_timestamp: int,
    ) -> list[TransferNotification]:
        if not self.config.notify_native:
            return []

        notifications: list[TransferNotification] = []
        for tx in block.get("transactions", []):
            contracts = tx.get("raw_data", {}).get("contract", [])
            if not contracts:
                continue
            contract = contracts[0]
            if contract.get("type") != "TransferContract":
                continue

            value = contract.get("parameter", {}).get("value", {})
            amount_sun = int(value.get("amount", 0))
            if amount_sun <= 0:
                continue

            from_address = normalize_tron_address(value["owner_address"])
            to_address = normalize_tron_address(value["to_address"])
            from_monitored = from_address in self.config.monitor_addresses
            to_monitored = to_address in self.config.monitor_addresses
            if not from_monitored and not to_monitored:
                continue

            tx_hash = tx["txID"]
            tx_info = tx_info_by_id.get(tx_hash)
            if not self._is_successful_transaction(tx, tx_info):
                continue

            amount = format_token_amount(amount_sun, 6)
            timestamp = tx_info.get("blockTimeStamp", block_timestamp) if tx_info else block_timestamp
            if from_monitored:
                notifications.append(
                    TransferNotification(
                        direction="出账",
                        token_symbol=TRON_NATIVE_SYMBOL,
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
                        token_symbol=TRON_NATIVE_SYMBOL,
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
        tx_infos: list[dict[str, Any]],
        block_timestamp: int,
    ) -> list[TransferNotification]:
        notifications: list[TransferNotification] = []
        for tx_info in tx_infos:
            if not self._is_successful_transaction(None, tx_info):
                continue

            tx_hash = tx_info.get("id") or tx_info.get("txid")
            if not tx_hash:
                continue

            for log_item in tx_info.get("log", []):
                topics = log_item.get("topics", [])
                if len(topics) < 3 or normalize_topic(topics[0]) != TRANSFER_EVENT_TOPIC:
                    continue

                contract_address = decode_log_address(log_item["address"])
                token_metadata = SUPPORTED_TRC20_TOKENS.get(contract_address)
                if token_metadata is None:
                    continue

                from_address = decode_topic_address(topics[1])
                to_address = decode_topic_address(topics[2])
                from_monitored = from_address in self.config.monitor_addresses
                to_monitored = to_address in self.config.monitor_addresses
                if not from_monitored and not to_monitored:
                    continue

                token_symbol, decimals = token_metadata
                raw_amount = int(log_item.get("data", "0"), 16)
                amount = format_token_amount(raw_amount, decimals)
                timestamp = int(tx_info.get("blockTimeStamp", block_timestamp))

                if from_monitored:
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
                if to_monitored and to_address != from_address:
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

    def _is_successful_transaction(
        self,
        tx: dict[str, Any] | None,
        tx_info: dict[str, Any] | None,
    ) -> bool:
        if tx_info:
            receipt_result = tx_info.get("receipt", {}).get("result")
            if receipt_result:
                return receipt_result == "SUCCESS"
            result = tx_info.get("result")
            if result:
                return result == "SUCCESS"

        if tx:
            for item in tx.get("ret", []):
                contract_ret = item.get("contractRet")
                if contract_ret:
                    return contract_ret == "SUCCESS"

        return True

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
    rpc_url = (
        os.getenv("Tron_RPC")
        or os.getenv("TRON_RPC")
        or ""
    ).strip()
    api_key = (os.getenv("TRON_API_KEY") or "").strip()
    monitor_addresses_raw = (os.getenv("TRON_MONITOR_ADDRESSES") or "").strip()

    missing_fields = []
    if not telegram_token:
        missing_fields.append("telergam_bot_token")
    if not telegram_chat_id:
        missing_fields.append("tg_chat_id")
    if not rpc_url:
        missing_fields.append("Tron_RPC")
    if not monitor_addresses_raw:
        missing_fields.append("TRON_MONITOR_ADDRESSES")

    if missing_fields:
        missing = ", ".join(missing_fields)
        raise SystemExit(f"missing required config in .env: {missing}")

    try:
        monitor_addresses = parse_address_list(monitor_addresses_raw)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

    return MonitorConfig(
        rpc_url=rpc_url,
        api_key=api_key,
        telegram_token=telegram_token,
        telegram_chat_id=telegram_chat_id,
        monitor_addresses=monitor_addresses,
        rpc_timeout=float(os.getenv("TRON_RPC_TIMEOUT", "15")),
        rpc_retries=int(os.getenv("TRON_RPC_RETRIES", "2")),
        rpc_retry_delay=float(os.getenv("TRON_RPC_RETRY_DELAY", "1")),
        poll_interval=float(os.getenv("TRON_POLL_INTERVAL", "3")),
        confirmations=int(os.getenv("TRON_CONFIRMATIONS", "1")),
        start_block=os.getenv("TRON_START_BLOCK", "latest"),
        state_file=Path(os.getenv("TRON_STATE_FILE", DEFAULT_STATE_FILE)),
        notify_native=parse_bool(os.getenv("TRON_NOTIFY_NATIVE"), default=True),
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    config = load_config()
    monitor = TronMonitor(config)
    monitor.run()


if __name__ == "__main__":
    main()
