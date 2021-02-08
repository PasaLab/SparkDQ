from hdfs import Client

from sparkdq.conf.Context import Context


def get_client():
    hdfs_url = Context().config.get_config["hdfs.url"]
    return Client(hdfs_url, root="/", session=False)


def write_file_on_hdfs(path, content, over_write):
    client = get_client()
    client.write(path, content, over_write)


def check_exist_for_hdfs(path):
    client = get_client()
    return client.status(path, False) is not None


def delete_file_on_hdfs(path):
    client = get_client()
    client.delete(path)


def rename_file_on_hdfs(old_name, new_name):
    client = get_client()
    client.rename(old_name, new_name)


def read_file_from_hdfs(path):
    client = get_client()
    with client.read(path) as reader:
        return reader.read()
