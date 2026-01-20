from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable
from urllib import request
from urllib.error import URLError


@dataclass(frozen=True)
class UpdateFile:
    path: str
    sha256: str
    url: str | None = None


class UpdateError(Exception):
    pass


class UpdateManager:
    def __init__(self, app_root: Path, logger: Callable[[str], None]):
        self.app_root = app_root
        self.logger = logger
        self.excluded_paths = {
            "logs",
            "farm_tool.db",
            "farm_tool.db-shm",
            "farm_tool.db-wal",
            "banned_dead_tokens.txt",
        }

    def download_and_apply(self, update_data: dict) -> None:
        files_payload = update_data.get("files")
        if files_payload:
            self._apply_files_update(files_payload)
            return

        download_url = update_data.get("download_url") or update_data.get("url")
        if not download_url:
            raise UpdateError("Brak download_url/url w odpowiedzi aktualizacji.")
        expected_hash = update_data.get("sha256") or update_data.get("checksum")
        self._apply_archive_update(download_url, expected_hash)

    def _apply_files_update(self, files_payload: Iterable[dict]) -> None:
        update_files = self._parse_files_payload(files_payload)
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            for update_file in update_files:
                if self._is_excluded(update_file.path):
                    self.logger(f"[Updater] Pomijam plik z listy wykluczeń: {update_file.path}")
                    continue
                if not update_file.url:
                    raise UpdateError(f"Brak URL dla pliku {update_file.path}.")
                dest = tmp_path / update_file.path
                dest.parent.mkdir(parents=True, exist_ok=True)
                self._download_file(update_file.url, dest)
                self._validate_sha256(dest, update_file.sha256)
                self._replace_file(dest, self.app_root / update_file.path)

    def _apply_archive_update(self, download_url: str, expected_hash: str | None) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            archive_path = tmp_path / "update.zip"
            self._download_file(download_url, archive_path)
            if expected_hash:
                self._validate_sha256(archive_path, expected_hash)
            extract_dir = tmp_path / "update_extract"
            extract_dir.mkdir(parents=True, exist_ok=True)
            with zipfile.ZipFile(archive_path) as archive:
                archive.extractall(extract_dir)
            manifest_path = self._find_manifest(extract_dir)
            if not manifest_path:
                raise UpdateError("Brak manifestu aktualizacji (manifest.json/update_manifest.json).")
            manifest = self._load_manifest(manifest_path)
            update_files = self._parse_files_payload(manifest.get("files", []))
            if not update_files:
                raise UpdateError("Manifest nie zawiera listy plików.")
            for update_file in update_files:
                if self._is_excluded(update_file.path):
                    self.logger(f"[Updater] Pomijam plik z listy wykluczeń: {update_file.path}")
                    continue
                source_path = extract_dir / update_file.path
                if not source_path.is_file():
                    raise UpdateError(f"Brak pliku w paczce: {update_file.path}.")
                self._validate_sha256(source_path, update_file.sha256)
                self._replace_file(source_path, self.app_root / update_file.path)

    def _download_file(self, url: str, destination: Path) -> None:
        try:
            with request.urlopen(url, timeout=30) as response, destination.open("wb") as output:
                shutil.copyfileobj(response, output)
        except (URLError, OSError) as exc:
            raise UpdateError(f"Nie udało się pobrać pliku z {url}: {exc}") from exc

    def _validate_sha256(self, file_path: Path, expected_hash: str) -> None:
        file_hash = hashlib.sha256()
        with file_path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                file_hash.update(chunk)
        digest = file_hash.hexdigest()
        if digest.lower() != expected_hash.lower():
            raise UpdateError(
                f"Hash SHA256 niezgodny dla {file_path.name}: {digest} (oczekiwano {expected_hash})."
            )

    def _replace_file(self, source: Path, target: Path) -> None:
        if not self._is_safe_path(target):
            raise UpdateError(f"Nieprawidłowa ścieżka docelowa: {target}.")
        target.parent.mkdir(parents=True, exist_ok=True)
        temp_target = target.with_suffix(target.suffix + ".new")
        shutil.copy2(source, temp_target)
        os.replace(temp_target, target)
        self.logger(f"[Updater] Zastąpiono plik: {target.relative_to(self.app_root)}")

    def _is_safe_path(self, path: Path) -> bool:
        try:
            path.resolve().relative_to(self.app_root.resolve())
        except ValueError:
            return False
        return True

    def _is_excluded(self, relative_path: str) -> bool:
        parts = Path(relative_path).parts
        return any(part in self.excluded_paths for part in parts)

    def _parse_files_payload(self, payload: Iterable[dict]) -> list[UpdateFile]:
        files = []
        for item in payload:
            if not isinstance(item, dict):
                continue
            path = item.get("path") or item.get("file")
            sha256 = item.get("sha256") or item.get("hash")
            if not path or not sha256:
                continue
            if not self._is_relative_safe(path):
                raise UpdateError(f"Nieprawidłowa ścieżka w manifeście: {path}.")
            files.append(UpdateFile(path=str(path), sha256=str(sha256), url=item.get("url")))
        return files

    def _is_relative_safe(self, path_value: str) -> bool:
        path = Path(path_value)
        if path.is_absolute():
            return False
        return ".." not in path.parts

    def _find_manifest(self, extract_dir: Path) -> Path | None:
        for name in ("update_manifest.json", "manifest.json"):
            candidate = extract_dir / name
            if candidate.is_file():
                return candidate
        return None

    def _load_manifest(self, path: Path) -> dict:
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise UpdateError(f"Nie udało się odczytać manifestu: {exc}") from exc
