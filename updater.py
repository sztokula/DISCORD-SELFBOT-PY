from __future__ import annotations

import hashlib
import hmac
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
    # Example: set UPDATE_SIGNING_KEY in env and sign payloads with HMAC-SHA256.
    # Canonical string: json.dumps(payload_without_signature, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    SIGNING_KEY_ENV = "UPDATE_SIGNING_KEY"

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
        self._require_valid_signature(update_data)
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
            backup_root = tmp_path / "backup"
            backup_root.mkdir(parents=True, exist_ok=True)
            applied = []
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

            try:
                for update_file in update_files:
                    if self._is_excluded(update_file.path):
                        continue
                    source = tmp_path / update_file.path
                    target = self.app_root / update_file.path
                    applied.append(self._replace_file_with_backup(source, target, backup_root))
            except UpdateError as exc:
                self._rollback_replacements(applied)
                raise UpdateError(f"Aktualizacja przerwana: {exc}") from exc

    def _require_valid_signature(self, update_data: dict) -> None:
        signature = update_data.get("signature")
        if not signature:
            raise UpdateError("Brak podpisu aktualizacji (signature).")
        signing_key = os.getenv(self.SIGNING_KEY_ENV, "").strip()
        if not signing_key:
            raise UpdateError("Brak klucza podpisu. Ustaw UPDATE_SIGNING_KEY w środowisku.")
        canonical = self._canonicalize_payload(update_data)
        expected = hmac.new(
            signing_key.encode("utf-8"),
            canonical.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(expected.lower(), str(signature).lower()):
            raise UpdateError("Nieprawidłowy podpis aktualizacji.")

    def _canonicalize_payload(self, update_data: dict) -> str:
        data = dict(update_data)
        data.pop("signature", None)
        return json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=True)

    def _apply_archive_update(self, download_url: str, expected_hash: str | None) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            backup_root = tmp_path / "backup"
            backup_root.mkdir(parents=True, exist_ok=True)
            applied = []
            archive_path = tmp_path / "update.zip"
            self._download_file(download_url, archive_path)
            if expected_hash:
                self._validate_sha256(archive_path, expected_hash)
            extract_dir = tmp_path / "update_extract"
            extract_dir.mkdir(parents=True, exist_ok=True)
            with zipfile.ZipFile(archive_path) as archive:
                self._safe_extract(archive, extract_dir)
            manifest_path = self._find_manifest(extract_dir)
            if not manifest_path:
                raise UpdateError("Brak manifestu aktualizacji (manifest.json/update_manifest.json).")
            manifest = self._load_manifest(manifest_path)
            update_files = self._parse_files_payload(manifest.get("files", []))
            if not update_files:
                raise UpdateError("Manifest nie zawiera listy plików.")

            for update_file in update_files:
                if self._is_excluded(update_file.path):
                    continue
                source_path = extract_dir / update_file.path
                if not source_path.is_file():
                    raise UpdateError(f"Brak pliku w paczce: {update_file.path}.")
                self._validate_sha256(source_path, update_file.sha256)

            try:
                for update_file in update_files:
                    if self._is_excluded(update_file.path):
                        continue
                    source_path = extract_dir / update_file.path
                    target_path = self.app_root / update_file.path
                    applied.append(self._replace_file_with_backup(source_path, target_path, backup_root))
            except UpdateError as exc:
                self._rollback_replacements(applied)
                raise UpdateError(f"Aktualizacja przerwana: {exc}") from exc

    def _safe_extract(self, archive: zipfile.ZipFile, extract_dir: Path) -> None:
        base = extract_dir.resolve()
        for member in archive.infolist():
            filename = member.filename
            if not filename:
                continue
            member_path = Path(filename)
            if member_path.is_absolute() or member_path.drive or ".." in member_path.parts:
                raise UpdateError(f"Nieprawidłowa ścieżka w paczce: {filename}.")
            target_path = (extract_dir / member_path).resolve()
            try:
                target_path.relative_to(base)
            except ValueError:
                raise UpdateError(f"Nieprawidłowa ścieżka w paczce: {filename}.")
            archive.extract(member, extract_dir)

    def _create_staging_root(self) -> Path:
        staging_root = Path(tempfile.mkdtemp(dir=self.app_root.parent, prefix=".update_staging_"))
        shutil.copytree(self.app_root, staging_root, dirs_exist_ok=True)
        return staging_root

    def _stage_file(self, source: Path, target: Path, root: Path) -> None:
        if not self._is_safe_path_for_root(target, root):
            raise UpdateError(f"Nieprawidłowa ścieżka docelowa: {target}.")
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)

    def _is_safe_path_for_root(self, path: Path, root: Path) -> bool:
        try:
            path.resolve().relative_to(root.resolve())
        except ValueError:
            return False
        return True

    def _swap_staged_root(self, staging_root: Path) -> None:
        backup_root = self.app_root.with_name(self.app_root.name + ".backup")
        if backup_root.exists():
            raise UpdateError(f"Katalog kopii zapasowej już istnieje: {backup_root}.")
        try:
            os.replace(self.app_root, backup_root)
            os.replace(staging_root, self.app_root)
        except OSError as exc:
            if not self.app_root.exists() and backup_root.exists():
                try:
                    os.replace(backup_root, self.app_root)
                except OSError:
                    pass
            if staging_root.exists():
                try:
                    shutil.rmtree(staging_root)
                except OSError:
                    pass
            raise UpdateError(f"Nie udało się podmienić katalogu aplikacji: {exc}") from exc
        try:
            shutil.rmtree(backup_root)
        except OSError:
            pass

    def _download_file(self, url: str, destination: Path) -> None:
        try:
            with request.urlopen(url, timeout=30) as response, destination.open("wb") as output:
                shutil.copyfileobj(response, output)
        except (URLError, OSError) as exc:
            raise UpdateError(f"Nie udaĹ‚o siÄ™ pobraÄ‡ pliku z {url}: {exc}") from exc

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
            raise UpdateError(f"NieprawidĹ‚owa Ĺ›cieĹĽka docelowa: {target}.")
        target.parent.mkdir(parents=True, exist_ok=True)
        temp_target = target.with_suffix(target.suffix + ".new")
        shutil.copy2(source, temp_target)
        os.replace(temp_target, target)
        self.logger(f"[Updater] ZastÄ…piono plik: {target.relative_to(self.app_root)}")

    def _replace_file_with_backup(self, source: Path, target: Path, backup_root: Path) -> dict:
        if not self._is_safe_path(target):
            raise UpdateError(f"Nieprawidlowa sciezka docelowa: {target}.")
        target.parent.mkdir(parents=True, exist_ok=True)
        existed = target.exists()
        backup_path = None
        if existed:
            backup_path = backup_root / target.relative_to(self.app_root)
            backup_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(target, backup_path)
        try:
            self._replace_file(source, target)
        except UpdateError:
            raise
        except OSError as exc:
            raise UpdateError(
                f"Nie mozna podmienic pliku {target}: {exc}. Zamknij aplikacje i sprobuj ponownie."
            ) from exc
        return {"target": target, "backup": backup_path, "existed": existed}

    def _rollback_replacements(self, applied: list[dict]) -> None:
        for item in reversed(applied):
            target = item.get("target")
            backup = item.get("backup")
            existed = item.get("existed")
            try:
                if backup and Path(backup).exists():
                    target.parent.mkdir(parents=True, exist_ok=True)
                    os.replace(backup, target)
                elif not existed and target and Path(target).exists():
                    Path(target).unlink()
            except OSError:
                pass

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
                raise UpdateError(f"NieprawidĹ‚owa Ĺ›cieĹĽka w manifeĹ›cie: {path}.")
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
            raise UpdateError(f"Nie udaĹ‚o siÄ™ odczytaÄ‡ manifestu: {exc}") from exc







