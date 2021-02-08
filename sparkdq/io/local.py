import os


def check_exist_for_local(path):
    return os.path.exists(path)


def delete_file_on_local(path):
    os.remove(path)


def rename_file_on_local(old_path, new_path):
    os.rename(old_path, new_path)


def write_file_to_local(path, content):
    with open(path, 'wb') as f:
        f.write(content)


def read_file_from_local(path):
    with open(path, "rb") as f:
        return f.read()
