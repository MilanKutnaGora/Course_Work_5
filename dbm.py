import psycopg2
import json
from abstract_classes import DBOperator

class DBManager:
    """Класс получения информации из БД"""

    def __init__(self, cursor):
        self.cursor = cursor

    def get_companies_and_vacancies_count(self):
        """Получение списка компаний и кол-ва вакансий"""
        self.cursor.execute("SELECT employer_name, COUNT(*) AS vacancies_count "
                            "FROM vacancies "
                            "JOIN employers USING(employer_id) "
                            "GROUP BY employer_name "
                            "ORDER BY vacancies_count DESC")

        return self.cursor.fetchall()

    def get_all_vacancies(self):
        """Получение списка всех вакансий"""
        self.cursor.execute("SELECT employer_name, vacancy_name, vacancy_salary_from, "
                            "vacancy_salary_to, vacancy_url "
                            "FROM vacancies "
                            "JOIN employers USING(employer_id)")

        return self.cursor.fetchall()

    def get_avg_salary(self, func_num):
        """Получение средней зарплаты по всем найденным вакансиям"""
        if func_num == "1":
            self.cursor.execute("SELECT AVG(vacancy_salary_from) FROM vacancies")
        elif func_num == "2":
            self.cursor.execute("SELECT AVG(vacancy_salary_to) FROM vacancies")

        return self.cursor.fetchall()

    def get_vacancies_with_higher_salary(self, func_sal_num):
        """
        Получение списка вакансий с ЗП больше чем средняя "от" и средняя "до" по всем вакансиям

        """
        if func_sal_num == "1":
            self.cursor.execute("SELECT vacancy_id, vacancy_name, employer_id, "
                                "vacancy_salary_from, vacancy_url FROM vacancies "
                                "JOIN employers USING(employer_id) "
                                "WHERE vacancy_name IS NOT NULL "
                                "GROUP BY vacancy_id, vacancy_name, employer_id, "
                                "vacancy_salary_from, vacancy_url "
                                "HAVING vacancy_salary_from > (SELECT AVG(vacancy_salary_from) "
                                "FROM vacancies)")

        elif func_sal_num == "2":
            self.cursor.execute("SELECT vacancy_id, vacancy_name, employer_id, "
                                "vacancy_salary_to, vacancy_url FROM vacancies "
                                "JOIN employers USING(employer_id) "
                                "WHERE vacancy_name IS NOT NULL "
                                "GROUP BY vacancy_id, vacancy_name, employer_id, "
                                "vacancy_salary_to, vacancy_url "
                                "HAVING vacancy_salary_to > (SELECT AVG(vacancy_salary_to) "
                                "FROM vacancies)")

        return self.cursor.fetchall()

    def get_vacancies_with_keyword(self, keyword):
        """Получение списка вакансий по ключевому слову"""
        self.cursor.execute(f"SELECT * FROM vacancies WHERE vacancy_name LIKE '%{keyword}%'")
        return self.cursor.fetchall()

class DBScriptor(DBOperator):
    """Класс для выполнения скриптов создания БД, таблиц и заполнения таблиц"""

    def __init__(self):
        pass

    def create_db(self, params, db_name) -> None:
        """Создание БД"""
        connection = psycopg2.connect(params)
        connection.autocommit = True
        cur = connection.cursor()
        cur.execute(f"DROP DATABASE IF EXISTS {db_name};")
        cur.execute(f"CREATE DATABASE {db_name};")
        cur.close()
        connection.close()

    def create_tables(self, cursor, fill_script_file) -> None:
        """Выполняет скрипт из файла create_tables_script для создания таблиц в БД"""
        with open(fill_script_file, 'r') as file:
            cursor.execute(file.read())

    def fill_tables(self, cursor, vacancies_dict: dict, vacancies_list: list):
        """Заполнение таблиц данными о вакансиях из json файла"""
        for k_name, v_id in vacancies_dict.items():
            cursor.execute(f"INSERT INTO employers VALUES (%s, %s)",
                           (int(v_id), k_name))

        for item in vacancies_list:
            cursor.execute(f"INSERT INTO vacancies VALUES (%s, %s, %s, %s, %s, %s)",
                           (int(item['id']), int(item['employer']['id']), item['name'],
                            item['salary']['from'], item['salary']['to'], item['alternate_url']))