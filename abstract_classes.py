from abc import ABC, abstractmethod


class ApiEngine(ABC):

    @abstractmethod
    def get_vacancies(self):
        pass


class JsonManager(ABC):

    @abstractmethod
    def save_to_json(self, json_file_name):
        pass

    @abstractmethod
    def get_json(self, json_file_name):
        pass


class DBOperator(ABC):

    @abstractmethod
    def create_db(self, params, db_name):
        pass

    @abstractmethod
    def create_tables(self, cursor, fill_script_file):
        pass

    @abstractmethod
    def fill_tables(self, cursor, vacancies_dict, vacancies_list):
        pass