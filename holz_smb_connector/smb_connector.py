import fnmatch
import os
import pathlib
import tempfile
import types
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import IO, Optional

from pydantic_settings import BaseSettings
from smbprotocol import Dialects
from smbprotocol.connection import Connection
from smbprotocol.exceptions import SMBException
from smbprotocol.file_info import FileInformationClass
from smbprotocol.open import (
    CreateDisposition,
    CreateOptions,
    FileAttributes,
    FilePipePrinterAccessMask,
    ImpersonationLevel,
    Open,
    ShareAccess,
)
from smbprotocol.session import Session
from smbprotocol.structure import BytesField
from smbprotocol.tree import TreeConnect


class OpenContextManager:
    def __init__(self, tree: TreeConnect, name: str):
        self.tree = tree
        self.name = name
        self.handle: Optional[Open] = None

    def __enter__(self):
        self.handle = Open(self.tree, self.name)
        return self.handle

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.handle:
            self.handle.close()


@dataclass
class SMBFile:
    name: str
    is_dir: bool
    read_only: bool
    full_path: str


class SMBSettings(BaseSettings):
    username: str = ""
    password: str = ""
    shared_folder: str = ""
    work_dir: str = ""
    host: str = ""
    port: int = 445


class SMBConnector:
    settings: SMBSettings = SMBSettings()

    def __init__(
        self,
        host: str = "",
        username: str = settings.username.strip(),
        password: str = settings.password.strip(),
        shared_folder: str = settings.shared_folder.strip(),
        port: int = settings.port,
        work_dir: str = settings.work_dir.strip(),
    ):
        self.username = username
        self.password = password
        self.shared_folder = shared_folder
        self.work_dir = work_dir.strip("/").strip("\\")
        self.host = host
        self.port = port

        self._connection: Connection | None = None
        self._session: Session | None = None
        self._tree: TreeConnect | None = None

    def __enter__(self):
        self._connection = Connection(
            guid=uuid.uuid4(),
            server_name=self.host,
            port=self.port,
            require_signing=False,
        )
        self._connection.connect(dialect=Dialects.SMB_3_1_1)

        self._session = Session(
            self._connection,
            self.username,
            self.password,
            require_encryption=False,
        )
        self._session.connect()

        self._tree = TreeConnect(
            session=self._session,
            share_name=fr"\\{self.host}\{self.shared_folder}",
        )
        self._tree.connect()

        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ):
        if self._tree:
            self._tree.disconnect()

        if self._session:
            self._session.disconnect()

        if self._connection:
            self._connection.disconnect()

    def _normalize_path(self, path: str) -> str:
        if self.work_dir and not path.startswith(self.work_dir):
            return str(pathlib.PureWindowsPath(self.work_dir).joinpath(path))
        return str(pathlib.PureWindowsPath(path))

    def list_dir(self, path: str = "") -> list[SMBFile]:
        full_path = self._normalize_path(path)
        return self.full_path_list_dir(full_path=full_path)

    def full_path_list_dir(self, full_path: str) -> list[SMBFile]:
        if not full_path:
            full_path = self._normalize_path("")

        files = []
        with OpenContextManager(tree=self._tree, name=full_path) as dir_handle:
            dir_handle.create(
                impersonation_level=ImpersonationLevel.Impersonation,
                desired_access=FilePipePrinterAccessMask.FILE_READ_ATTRIBUTES | FilePipePrinterAccessMask.FILE_READ_DATA,  # noqa: E501
                file_attributes=FileAttributes.FILE_ATTRIBUTE_DIRECTORY,
                share_access=ShareAccess.FILE_SHARE_READ,
                create_disposition=CreateDisposition.FILE_OPEN,
                create_options=CreateOptions.FILE_DIRECTORY_FILE,
            )
            query = dir_handle.query_directory(
                pattern="*",
                file_information_class=FileInformationClass.FILE_DIRECTORY_INFORMATION,
            )

            for file_info in query:
                file_name = self._decode_bytes_field(file_info['file_name'])
                if file_name in (".", ".."):
                    continue
                attrs = file_info['file_attributes'].get_value()
                files.append(
                    SMBFile(
                        name=file_name,
                        is_dir=bool(attrs & FileAttributes.FILE_ATTRIBUTE_DIRECTORY),
                        read_only=bool(attrs & FileAttributes.FILE_ATTRIBUTE_READONLY),
                        full_path=f"{full_path}\\{file_name}",
                    ),
                )

        return files

    @contextmanager
    def retrieve_file(self, path: str) -> Iterator[IO[bytes]]:
        full_path = self._normalize_path(path)
        temp_file = tempfile.NamedTemporaryFile()
        try:
            with OpenContextManager(tree=self._tree, name=full_path) as file_handle:
                file_handle.create(
                    impersonation_level=ImpersonationLevel.Impersonation,
                    desired_access=FilePipePrinterAccessMask.FILE_READ_DATA,
                    file_attributes=FileAttributes.FILE_ATTRIBUTE_NORMAL,
                    share_access=ShareAccess.FILE_SHARE_READ,
                    create_disposition=CreateDisposition.FILE_OPEN,
                    create_options=CreateOptions.FILE_NON_DIRECTORY_FILE,
                )
                length = int(file_handle.end_of_file)
                file_data = file_handle.read(offset=0, length=length)
            temp_file.write(file_data)
            temp_file.seek(0)
            yield temp_file.file
        finally:
            temp_file.close()

    def store_file(self, path: str, file_obj: IO) -> bool:
        full_path = self._normalize_path(path)
        with OpenContextManager(tree=self._tree, name=full_path) as file_handle:
            file_handle.create(
                impersonation_level=ImpersonationLevel.Impersonation,
                desired_access=FilePipePrinterAccessMask.FILE_WRITE_DATA,
                file_attributes=FileAttributes.FILE_ATTRIBUTE_NORMAL,
                share_access=ShareAccess.FILE_SHARE_WRITE,
                create_disposition=CreateDisposition.FILE_OVERWRITE_IF,
                create_options=CreateOptions.FILE_NON_DIRECTORY_FILE,
            )
            file_data = file_obj.read()
            file_handle.write(data=file_data, offset=0)
            return True

    def delete_files(self, file_pattern: str, delete_folders: bool = False) -> None:
        full_pattern = self._normalize_path(file_pattern)
        self.delete_files_by_full_pattern(full_pattern, delete_folders)

    def delete_files_by_full_pattern(self, full_pattern: str, delete_folders: bool = False) -> None:
        dir_path = os.path.dirname(full_pattern)
        pattern = os.path.basename(full_pattern)

        for file in self.full_path_list_dir(dir_path):
            if not fnmatch.fnmatch(file.full_path, pattern):
                continue
            if file.is_dir:
                if delete_folders:
                    self.delete_dir(file.full_path)
            else:
                self._delete_single_file(file.full_path)

    def _delete_single_file(self, path: str) -> None:
        with OpenContextManager(tree=self._tree, name=path) as file_handle:
            file_handle.create(
                impersonation_level=ImpersonationLevel.Impersonation,
                desired_access=FilePipePrinterAccessMask.DELETE,
                file_attributes=FileAttributes.FILE_ATTRIBUTE_NORMAL,
                share_access=ShareAccess.FILE_SHARE_DELETE,
                create_disposition=CreateDisposition.FILE_OPEN,
                create_options=CreateOptions.FILE_DELETE_ON_CLOSE,
            )

    def create_dir(self, path: str) -> None:
        full_path = self._normalize_path(path)
        dirs = full_path.strip("/").split("/")
        current_path = []

        for dir_name in dirs:
            current_path.append(dir_name)
            try:
                with OpenContextManager(tree=self._tree, name="/".join(current_path)) as dir_handle:
                    dir_handle.create(
                        impersonation_level=ImpersonationLevel.Impersonation,
                        desired_access=FilePipePrinterAccessMask.GENERIC_ALL,
                        file_attributes=FileAttributes.FILE_ATTRIBUTE_DIRECTORY,
                        share_access=ShareAccess.FILE_SHARE_READ | ShareAccess.FILE_SHARE_WRITE,
                        create_disposition=CreateDisposition.FILE_CREATE,
                        create_options=CreateOptions.FILE_DIRECTORY_FILE,
                    )
            except SMBException:
                # Если уже существует
                continue

    def delete_dir(self, path: str) -> None:
        full_path = self._normalize_path(path)

        # удалять непустую папку нельзя, сначала очищаем ее
        for file in self.full_path_list_dir(full_path):
            file_path = str(pathlib.PureWindowsPath(full_path).joinpath(file.name))
            if file.is_dir:
                self.delete_dir(file_path)
            else:
                self._delete_single_file(file_path)

        with OpenContextManager(tree=self._tree, name=full_path) as file_handle:
            file_handle.create(
                impersonation_level=ImpersonationLevel.Impersonation,
                desired_access=FilePipePrinterAccessMask.DELETE,
                file_attributes=FileAttributes.FILE_ATTRIBUTE_DIRECTORY,
                share_access=ShareAccess.FILE_SHARE_DELETE,
                create_disposition=CreateDisposition.FILE_OPEN,
                create_options=CreateOptions.FILE_DIRECTORY_FILE | CreateOptions.FILE_DELETE_ON_CLOSE,
            )

    def copy_file(self, old_path: str, new_path: str) -> None:
        full_old_path = self._normalize_path(old_path)
        full_new_path = self._normalize_path(new_path)

        with self.retrieve_file(full_old_path) as file_obj:
            self.store_file(full_new_path, file_obj)

    def move_file(self, old_path: str, new_path: str) -> None:
        self.copy_file(old_path, new_path)
        full_old_path = self._normalize_path(old_path)
        self._delete_single_file(full_old_path)

    @staticmethod
    def _decode_bytes_field(data: BytesField):
        return data.get_value().decode("UTF-16-LE")
