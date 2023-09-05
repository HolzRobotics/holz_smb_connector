import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import IO

from pydantic_settings import BaseSettings
from smb.SMBConnection import SMBConnection


class SMBSettings(BaseSettings):
    username: str
    password: str
    shared_folder: str
    work_dir: str
    host: str
    port: int = 445


@dataclass
class SMBFile:
    name: str
    is_dir: bool
    read_only: bool


class SMBConnector:
    settings: SMBSettings

    def __init_subclass__(cls, **kwargs):
        if not hasattr(cls, "settings"):
            raise NotImplementedError('Attribute "settings" must be implemented.')
        super().__init_subclass__(**kwargs)

    def __init__(self):
        self.conn = SMBConnection(
            username=self.settings.username,
            password=self.settings.password,
            my_name="server_host",
            remote_name="target_host",
            is_direct_tcp=True,
        )
        self.shared_folder = self.settings.shared_folder
        self.work_dir = self.settings.work_dir
        self.host = self.settings.host
        self.port = self.settings.port

    def __enter__(self):
        assert self.conn.connect(ip=self.host, port=self.port)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()

    def list_dir(self, path: str = "") -> list[SMBFile]:
        full_path = "/".join([self.work_dir, path])
        _files_list = self.conn.listPath(self.shared_folder, full_path)
        files_list = [
            SMBFile(
                name=f.filename,
                is_dir=f.isDirectory,
                read_only=f.isReadOnly,
            )
            for f in _files_list
        ]
        for file in files_list[:]:
            if file.is_dir and file.name in (".", ".."):
                files_list.remove(file)
        return files_list

    @contextmanager
    def retrieve_file(self, path: str) -> Iterator[bytes]:
        full_path = "/".join([self.work_dir, path])
        file_obj = tempfile.NamedTemporaryFile()
        try:
            _, _ = self.conn.retrieveFile(self.shared_folder, full_path, file_obj)
            file_obj.seek(0)
            yield bytes(file_obj)
        finally:
            file_obj.close()

    def store_file(self, path: str, file_obj: IO) -> bool:
        full_path = "/".join([self.work_dir, path])
        bytes_count = self.conn.storeFile(self.shared_folder, full_path, file_obj)
        if bytes_count:
            return True
        return False

    def delete_files(self, file_pattern: str, delete_folders: bool = False) -> None:
        full_pattern = "/".join([self.work_dir, file_pattern])
        self.conn.deleteFiles(self.shared_folder, full_pattern, delete_folders)

    def create_dir(self, path: str) -> None:
        full_path = "/".join([self.work_dir, path])
        self.conn.createDirectory(self.shared_folder, full_path)

    def delete_dir(self, path: str):
        full_path = "/".join([self.work_dir, path])
        self.conn.deleteDirectory(self.shared_folder, full_path)

    def copy_file(self, old_path: str, new_path: str) -> None:
        full_old_path = "/".join([self.work_dir, old_path])
        full_new_path = "/".join([self.work_dir, new_path])
        with tempfile.NamedTemporaryFile() as file_obj:
            self.conn.retrieveFile(self.shared_folder, full_old_path, file_obj)
            file_obj.seek(0)
            self.conn.storeFile(self.shared_folder, full_new_path, file_obj)

    def move_file(self, old_path: str, new_path: str) -> None:
        self.copy_file(old_path=old_path, new_path=new_path)
        full_old_path = "/".join([self.work_dir, old_path])
        self.conn.deleteFiles(self.shared_folder, full_old_path)
