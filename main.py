import psycopg2
from api_hh import HHApiEngine
from dbm import DBScriptor
from employs import get_empl_items
from joson_manager import HHJsonManager
from configuration import config

from employs import get_empl_items
from dbm import DBManager

# Создание переменных с названиями файлов
json_file_name = 'json_vac_info.json'
db_name = 'cw5_db'
ini_config_file = 'database.ini'
section_params = 'postgresql'
script_file = 'create_tables_script.sql'

if __name__ == "__main__":
    print("Привет! Я программа, которая собирает вакансии с лучших компаний HeadHunter!\n"
          "Сейчас в моей базе 10 компаний, а именно: СБЕР, Газпром нефть, 2ГИС, "
          "Центр винного туризма Абрау Дюрсо, VERTEX, КАМАЗ, ФосАгро, Россети Ленэнерго, "
          "Skyeng и Битрикс24\n"
          "Нажмите Enter, чтобы найти и сохранить в базу данных все вакансии "
          "с полной информацией и продолжим!")

    while True:
        user_input = input("> ")
        if user_input != "":
            print("Сперва нажмите Enter!")
        else:
            break
    employee_name = HHApiEngine()
    vacancies_list = employee_name.get_vacancies()  # Получение всех вакансий с ЗП с HeadHunter

    json_instance = HHJsonManager(vacancies_list)
    json_instance.save_to_json(json_file_name)
    print(f"Запись вакансий в библиотеку прошла успешно")
    json_vac_list = json_instance.get_json(json_file_name)

    params = config(ini_config_file, section_params)  # Создание переменной параметров доступа к БД

    db_script = DBScriptor()

    db_script.create_db(params, db_name)
    print(f"База данных компаний и вакансий создана")

    params.update({'dbname': db_name})

    vacancies_id_dict = get_empl_items()  # Достаем словарь с компаниями из employees_ids

    connection = psycopg2.connect(**params)  # Открываем соединение для создания БД
    try:
        with connection as conn:
            with conn.cursor() as cur:
                db_script.create_tables(cur, script_file)
                print(f"Таблицы компаний и вакансий созданы")

                db_script.fill_tables(cur, vacancies_id_dict, json_vac_list)
                print(f"Таблицы компаний и вакансий успешно заполнены")

    finally:
        connection.close()

    connection = psycopg2.connect(**params)  # Открываем соединение для работы с пользователем

    with connection as conn:
        with conn.cursor() as cur:
            db_manager = DBManager(cur)

            # Функционал пользователя
            while True:
                print("\nДоступны следующие действия:\n"
                      "1 - Вывести весь список компаний с указанием "
                      "кол-ва найденных вакансий (по убыванию)\n"
                      "2 - Вывести весь список вакансий\n"
                      "3 - Вывести среднюю ЗП по всем вакансиям\n"
                      "4 - Вывести список вакансий у которых ЗП "
                      "выше средней по всем вакансия\n"
                      "5 - Вывести список всех вакансий по ключевому "
                      "слову в названии вакансии\n"
                      "0 - Выйти")
                user_func_input = input("> ")
                if user_func_input == "1":
                    companies = db_manager.get_companies_and_vacancies_count()
                    for company in companies:
                        print(f"Компания: {company[0]}\nКол-во вакансий: {company[1]}")
                        print()

                if user_func_input == "2":
                    vacancies = db_manager.get_all_vacancies()
                    for vacancy in vacancies:
                        print(f"Компания: {vacancy[0]}\nВакансия: {vacancy[1]}\n"
                              f"Зарплата от: {vacancy[2]}\nЗарплата до: {vacancy[3]}\n"
                              f"Ссылка: {vacancy[4]}")
                        print()

                if user_func_input == "3":
                    while True:
                        sal_func_input = input("1 - По зарплате 'от'\n2 - По зарплате 'до'\n> ")
                        if sal_func_input in ("1", "2"):
                            print(f"{round(db_manager.get_avg_salary(sal_func_input)[0][0])} руб.")
                            break
                        else:
                            print("Такой функции нет. Выберите из списка!\n")

                if user_func_input == "4":
                    while True:
                        sal_func_input = input("1 - По нижней границе ЗП\n"
                                               "2 - По верхней границе ЗП\n> ")
                        if sal_func_input in ("1", "2"):
                            valid_vac = db_manager.get_vacancies_with_higher_salary(sal_func_input)
                            for vac in valid_vac:
                                print(f"Вакансия: {vac[1]}\nЗарплата: {vac[3]}\nСсылка: {vac[4]}")
                                print()
                            break
                        else:
                            print("Такой функции нет. Выберите из списка!\n")

                if user_func_input == "5":
                    while True:
                        keyword_input = input("Введите слово, например Менеджер или Водитель, "
                                              "или введите 0 для выхода:\n> ")
                        vac_res_list = db_manager.get_vacancies_with_keyword(keyword_input)
                        if keyword_input == "0":
                            break
                        if len(vac_res_list) == 0:
                            print("С таким словом вакансий нет :(\nПопробуйте снова!\n")
                        else:
                            for vac in vac_res_list:
                                print(f"Вакансия: {vac[2]}\nЗарплата от: {vac[3]}\n"
                                      f"Зарплата до: {vac[4]}\nСсылка: {vac[5]}")
                                print()
                            break

                if user_func_input == "0":
                    print("Пока пока!")
                    break


