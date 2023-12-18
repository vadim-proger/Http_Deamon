import os
import sqlite3
import logging


class DataBaseStorage():
    """
    Class manages database.
    Structure:
    hash - string, primary key
    name file - string
    """

    def __init__(self, database_path, database_name):
        self.path = os.path.join(database_path, f"{database_name}.db")

        if not os.path.exists(database_path):
            os.makedirs(database_path)
        # проверяем наличие существующей базы данных
        if not os.path.isfile(self.path):
            db = self.get_db(self.path)
            # если нет, то создаем скриптом из файла
            with open(os.path.join(
                    os.path.dirname(__file__), 'Create_storage.sql')) as f:
                db.executescript(f.read())
                db.close()

    def get_db(self, path):
        """
        Setting the connection to the base
        :param path:path to database
        :return: sqlite object
        """
        db = sqlite3.connect(
            path,
            detect_types=sqlite3.PARSE_DECLTYPES)

        #Возвращение строк словаря
        db.row_factory = sqlite3.Row
        return db

    def close(self, db):
        """
        Closing the connection to the base
        :return:none
        """
        if not db:
            db.close()

    def insert(self, hash_file: str, name_file: str):
        """
        Insert data in base
        :param hash_file: hash of file
        :param name_file: name file
        :return: None
        """
        db = self.get_db(self.path)
        try:
            db.execute(
                'insert into STORAGE (HASH,NAME) values (?,?)',
                (hash_file, name_file)
            )
            db.commit()
            db.close()
        except Exception as e:
            logging.error(str(e))
        finally:
            self.close(db)

    def delete(self, hash_file:str):
        """
        Delete data from base
        :param hash_file: hash of file
        :return: None
        """
        db = self.get_db(self.path)
        try:
            db.execute(
                'DELETE from STORAGE where HASH = ?', (hash_file,)
            ).fetchone()
            db.commit()
            db.close()
        except Exception as e:
            logging.error(str(e))
        finally:
            self.close(db)

    def get_name_file(self, hash_file:str):
        """
        Returns name file
        :param hash_file: hash of file
        :return: name file -- str
        """
        db = self.get_db(self.path)
        try:
            name_file = db.execute(
                'select NAME from STORAGE where HASH = ?', (hash_file,)
            ).fetchone()['NAME']
            db.commit()
            db.close()
            return name_file
        except Exception as e:
            logging.error(str(e))
        finally:
            self.close(db)

    def print_table(self, size: int):
        """
        Prints firts size lines
        :param size: count lines
        :return: tab;e in stdout
        """
        db = self.get_db(self.path)
        lines = db.execute("select * from STORAGE").fetchmany(size)
        print(f"Table monitoring first {size} lines")
        if lines:
            print(lines[0].keys())
            for line in lines:
                for k in line.keys():
                    print(line[k], end='\t')
                print('')
        else:
            print("Table is empty")
        db.close()


if __name__ == '__main__':
    storage = DataBaseStorage()
    storage.print_table(3)
    storage.close()
