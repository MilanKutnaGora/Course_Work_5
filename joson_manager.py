from abstract_classes import JsonManager
import json


class HHJsonManager(JsonManager):
    """Класс создания json файла с вакансиями и получения вакансий"""
    def __init__(self, vacancies_list):
        self.vacancies_list = vacancies_list

    def save_to_json(self, json_file_name):
        """Сохранение вакансий в json"""
        with open(json_file_name, "w", encoding="utf-8") as file:
            json.dump(self.vacancies_list, file, indent=2, ensure_ascii=False)

    def get_json(self, json_file_name):
        """Получение вакансий из json"""
        with open(json_file_name, "r", encoding="utf-8") as file:
            vacancies_info = json.load(file)
            return vacancies_info