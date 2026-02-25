import hashlib
import json
import logging
import os
import shutil
import sys
import time
from typing import Dict, Iterable, List, Tuple
from urllib.parse import urlparse

try:
    import smbclient  # type: ignore
except ImportError:
    smbclient = None


def get_env_var(name: str) -> str:
    """Вернёт обязательную переменную окружения или завершит процесс с ошибкой."""
    value = os.getenv(name)
    if not value:
        print(
            f"Ошибка: обязательная переменная окружения '{name}' не задана!",
            file=sys.stderr,
        )
        sys.exit(1)
    return value


SOURCE_DIR = get_env_var("SOURCE_DIR")
TARGET_DIR = get_env_var("TARGET_DIR")

STABLE_SECONDS = int(os.getenv("STABLE_SECONDS", "3"))
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "1"))
TRIGGER_FILE = os.getenv("TRIGGER_FILE", "trigger.txt")
RETRY_COUNT = int(os.getenv("RETRY_COUNT", "3"))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", "2"))
MANIFEST_PREFIX = os.getenv("MANIFEST_PREFIX", "manifest")
RUN_MODE = os.getenv("RUN_MODE", "trigger").lower()  # trigger | cron

# SMB учетные данные (опционально)
SMB_USERNAME = os.getenv("SMB_USERNAME")
SMB_PASSWORD = os.getenv("SMB_PASSWORD")


def is_smb_path(path: str) -> bool:
    """Проверить, является ли путь SMB путём."""
    return path.lower().startswith("smb://")


def parse_smb_path(path: str) -> tuple:
    """Разобрать SMB путь вида smb://host/share/path/file.

    Вернёт кортеж (host, share, path_on_share).
    """
    parsed = urlparse(path)
    if parsed.scheme.lower() != "smb":
        raise ValueError(f"Ожидается SMB путь, получен: {path}")

    host = parsed.netloc
    parts = parsed.path.strip("/").split("/", 1)
    if len(parts) < 2:
        raise ValueError(
            f"Некорректный SMB путь: {path}. Ожидается smb://host/share/path"
        )

    share = parts[0]
    path_on_share = "/" + parts[1] if len(parts) > 1 else "/"

    return host, share, path_on_share


def smb_makedirs(smb_path: str) -> None:
    """Создать директории на SMB с опциональной аутентификацией."""
    if not smbclient:
        raise ImportError(
            "smbprotocol не установлен. Установите: pip install smbprotocol"
        )

    host, share, path_on_share = parse_smb_path(smb_path)
    parent = os.path.dirname(path_on_share).rstrip("/")

    if not parent or parent == "":
        return

    try:
        kwargs = {}
        if SMB_USERNAME:
            kwargs["username"] = SMB_USERNAME
        if SMB_PASSWORD:
            kwargs["password"] = SMB_PASSWORD

        smbclient.mkdir(rf"//{host}/{share}{parent}", **kwargs)
    except Exception as exc:
        # Директория может уже существовать, игнорируем некоторые ошибки
        if "exist" not in str(exc).lower():
            logging.warning("Ошибка при создании директории SMB %s: %s", smb_path, exc)


def smb_copy_file(src: str, dst_smb: str) -> None:
    """Скопировать локальный файл на SMB с опциональной аутентификацией."""
    if not smbclient:
        raise ImportError(
            "smbprotocol не установлен. Установите: pip install smbprotocol"
        )

    host, share, path_on_share = parse_smb_path(dst_smb)

    kwargs = {}
    if SMB_USERNAME:
        kwargs["username"] = SMB_USERNAME
    if SMB_PASSWORD:
        kwargs["password"] = SMB_PASSWORD

    # Копируем локальный файл на SMB
    with open(src, "rb") as local_file:
        with smbclient.open_file(
            rf"//{host}/{share}{path_on_share}", mode="wb", **kwargs
        ) as smb_file:
            shutil.copyfileobj(local_file, smb_file)


def setup_logging() -> None:
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    log_file = os.getenv("LOG_FILE")
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.FileHandler(log_file)] if log_file else None,
    )


def wait_for_stable_file(path: str) -> bool:
    """Дождаться стабильного размера файла в течение STABLE_SECONDS."""
    last_size = -1
    unchanged_for = 0
    while True:
        try:
            size = os.path.getsize(path)
        except OSError:
            return False
        if size == last_size:
            unchanged_for += POLL_INTERVAL
        else:
            last_size = size
            unchanged_for = 0
        if unchanged_for >= STABLE_SECONDS:
            return True
        time.sleep(POLL_INTERVAL)


def list_source_files() -> Iterable[str]:
    try:
        for name in os.listdir(SOURCE_DIR):
            if name == TRIGGER_FILE:
                continue
            path = os.path.join(SOURCE_DIR, name)
            if os.path.isfile(path):
                yield path
    except FileNotFoundError:
        logging.error("Исходная директория %s недоступна", SOURCE_DIR)


def file_hash(path: str, chunk_size: int = 1024 * 1024) -> str:
    """Вернёт SHA256 файла."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def manifest_entry(path: str, check_stable: bool = True) -> Tuple[bool, Dict]:
    """Сформировать запись манифеста для файла."""
    for attempt in range(1, RETRY_COUNT + 1):
        if check_stable and not wait_for_stable_file(path):
            logging.warning("Файл %s исчез до чтения", path)
            return False, {}
        try:
            stat = os.stat(path)
            digest = file_hash(path)
            return True, {
                "name": os.path.basename(path),
                "size": stat.st_size,
                "mtime": int(stat.st_mtime),
                "sha256": digest,
            }
        except Exception as exc:  # pylint: disable=broad-except
            logging.error("Ошибка чтения %s на попытке %s: %s", path, attempt, exc)
            if attempt < RETRY_COUNT:
                time.sleep(RETRY_DELAY)
    return False, {}


def build_manifest(check_stable: bool = True) -> Tuple[int, int, List[Dict]]:
    """Собрать манифест по всем файлам."""
    found = ok = failed = 0
    entries: List[Dict] = []
    for src in list_source_files():
        found += 1
        success, entry = manifest_entry(src, check_stable)
        if success:
            ok += 1
            entries.append(entry)
        else:
            failed += 1
    return ok, failed, entries


def copy_with_hash(src: str, dst: str, check_stable: bool = True) -> bool:
    """Копировать файл с проверкой стабильности и сверкой SHA256.

    Поддерживает как локальные пути, так и SMB пути (smb://host/share/path).
    """
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            if check_stable and not wait_for_stable_file(src):
                logging.warning("Файл %s исчез до копирования", src)
                return False

            src_hash = file_hash(src)

            if is_smb_path(dst):
                # Копирование на SMB
                smb_makedirs(dst)
                smb_copy_file(src, dst)
            else:
                # Копирование на локальную файловую систему
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                shutil.copy2(src, dst)

            os.remove(src)
            logging.info(
                "Файл скопирован %s -> %s (попытка %s, hash=%s)",
                src,
                dst,
                attempt,
                src_hash,
            )
            return True
        except Exception as exc:  # pylint: disable=broad-except
            logging.error(
                "Ошибка копирования %s -> %s на попытке %s: %s", src, dst, attempt, exc
            )
            if attempt < RETRY_COUNT:
                time.sleep(RETRY_DELAY)
    return False


def copy_all_files(check_stable: bool = True) -> Tuple[int, int, int]:
    found = ok = failed = 0
    for src in list_source_files():
        found += 1
        dst = os.path.join(TARGET_DIR, os.path.basename(src))
        if copy_with_hash(src, dst, check_stable):
            ok += 1
        else:
            failed += 1
    return found, ok, failed


def write_manifest(entries: List[Dict]) -> str:
    os.makedirs(SOURCE_DIR, exist_ok=True)
    manifest = {
        "generated_at": int(time.time()),
        "source_dir": SOURCE_DIR,
        "files": entries,
    }
    filename = f"{MANIFEST_PREFIX}-{int(time.time())}.json"
    path = os.path.join(SOURCE_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    return path


def wait_for_trigger() -> str:
    return os.path.join(SOURCE_DIR, TRIGGER_FILE)


def process_files_once(check_stable: bool = True) -> None:
    """Однократная обработка файлов: манифест + копирование.

    Если файлов нет вообще, то ничего не делаем (не создаём манифест).
    """
    ok, failed, entries = build_manifest(check_stable=check_stable)
    if ok == 0 and failed == 0:
        logging.info("В исходной директории нет файлов, действий не требуется.")
        return

    manifest_path = write_manifest(entries)
    copied_found, copied_ok, copied_failed = copy_all_files(check_stable=check_stable)
    logging.info(
        "Сводка: файлов всего=%s успешно=%s ошибки=%s; манифест=%s (успешно=%s ошибки=%s)",
        copied_found,
        copied_ok,
        copied_failed,
        manifest_path,
        ok,
        failed,
    )


def run_trigger_loop() -> None:
    """Режим ожидания триггер-файла в бесконечном цикле."""
    trigger_path = wait_for_trigger()
    logging.info("Запуск в режиме trigger, триггер=%s", trigger_path)
    while True:
        if os.path.isfile(trigger_path) and wait_for_stable_file(trigger_path):
            logging.info("Обнаружен триггер %s", trigger_path)
            process_files_once(check_stable=True)
            try:
                os.remove(trigger_path)
                logging.info("Триггер %s удалён", trigger_path)
            except OSError as exc:
                logging.error("Не удалось удалить триггер %s: %s", trigger_path, exc)
        time.sleep(POLL_INTERVAL)


def main() -> None:
    setup_logging()
    logging.info(
        "Старт. SOURCE_DIR=%s TARGET_DIR=%s TRIGGER_FILE=%s RUN_MODE=%s",
        SOURCE_DIR,
        TARGET_DIR,
        TRIGGER_FILE,
        RUN_MODE,
    )

    if RUN_MODE not in ("cron", "trigger"):
        logging.error(
            "Некорректное значение RUN_MODE=%s, ожидается 'cron' или 'trigger'. "
            "Завершаем работу с ошибкой.",
            RUN_MODE,
        )
        sys.exit(1)

    if RUN_MODE == "cron":
        # Для работы по крону: один запуск обработки и завершение.
        logging.info("Работаем в режиме cron: один проход и выход.")
        process_files_once(check_stable=True)
        logging.info("Завершение работы (cron-запуск).")
    else:
        # Режим trigger: ожидания триггер-файла.
        logging.info("Работаем в режиме trigger (режим по умолчанию).")
        run_trigger_loop()


if __name__ == "__main__":
    main()
