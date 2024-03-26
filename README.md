# holz_smb_connector
Base SMB connector

Примеры использования:

    smb_connector = SMBConnector(...)
    # Чтобы скачать файл
    with smb_connector.retrive_file(...) as file_obj:
        ...
