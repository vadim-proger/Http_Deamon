import os
import hashlib


class FileHandler():
    """
    Class manages storage.
    """

    def __init__(self, path_store):
        if not os.path.exists(path_store):
            os.makedirs(path_store)
        elif os.path.isfile(path_store):
            pass

        self.work_directory = path_store

        # первые два символа хэша файла
        self.symbols = 2

    def download(self, hash_file: str):
        """
        Get file from stogare
        :param hash_file: name file
        :return: file
        """
        name_directory = hash_file[0:self.symbols]

        if os.path.isdir(os.path.join(self.work_directory, name_directory)):
            if os.path.isfile(os.path.join(self.work_directory, name_directory, hash_file)):
                return os.path.join(self.work_directory, name_directory), hash_file

        return None, None

    def delete(self, hash_file: str):
        """
        Delete file from storage
        :param hash_file:
        :return: bool
        """
        name_directory = hash_file[0:self.symbols]

        if os.path.isdir(os.path.join(self.work_directory, name_directory)):
            if os.path.isfile(os.path.join(self.work_directory, name_directory, hash_file)):
                # удаление файла
                try:
                    os.remove(os.path.join(self.work_directory, name_directory, hash_file))
                except:
                    return False
                # удаление директории если она пустая
                try:
                    if len(os.listdir(os.path.join(self.work_directory, name_directory)) ) == 0:
                        os.rmdir(os.path.join(self.work_directory, name_directory))
                except:
                    pass

                return True

        return False

    def upload(self, file):
        """
        Create file in storge
        :param file:
        :return: hash
        """
        # получаем хэш файла
        hash_file = hashlib.md5(file.read()).hexdigest()

        # получаем имя дериктории
        name_directory = hash_file[0:self.symbols]

        if os.path.exists(os.path.join(self.work_directory, name_directory)):
            if os.path.exists(os.path.join(self.work_directory, name_directory, hash_file)):
                return None
        else:
            os.makedirs(os.path.join(self.work_directory, name_directory))

        file.stream.seek(0)
        file.save(os.path.join(self.work_directory, name_directory, hash_file))
        return hash_file
