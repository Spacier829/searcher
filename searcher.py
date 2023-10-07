import sqlite3


class Seacher:

    # Фиксация изменений в БД
    def dbcommit(self):
        self.con.commit()

    # 0. Конструктор
    def __init__(self, dbFileName):
        # Подключение к БД
        self.con = sqlite3.connect(dbFileName)

    # 0. Деструктор
    def __del__(self):
        # Закрытие соединения с БД
        self.con.close()

    # 1. Получение идентификаторов для каждого слова в запросе
    def getWordsIds(self, queryString):
        # :param queryString: поисковый запрос пользователя
        # :return: список wordlist.rowid искомых слов

        # Приведение поискового запроса к нижнему регистру
        queryString = queryString.lower()

        # Разделение на отдельные искомые слова
        queryWordsList = queryString.split(" ")

        # Список для хранения результата
        rowidList = list()

        # Для каждого искомого слова
        for word in queryWordsList:
            # Формирование sql-запрос для получения rowid слова, указано ограничение на кол-во возвращаемых результатов (LIMIT 1)
            sql = "SELECT rowid FROM wordList WHERE word =\"{}\" LIMIT 1; ".format(
                word)

            # Выполнение sql-запроса. В качестве результата ожидаем строки содержащие целочисленный идентификатор rowid
            result_row = self.con.execute(sql).fetchone()

            # Если слово было найдено и rowid получен
            if result_row != None:
                # Искомое rowid является элементом строки ответа от БД (особенность получаемого результата)
                word_rowid = result_row[0]

                # Помещение rowid в список результата
                rowidList.append(word_rowid)
                print("Слово:", word, "id:", word_rowid)
            else:
                # В случае, если слово не найдено - остановка работы (генерация исключения)
                raise Exception(
                    "Одно из слов поискового запроса не найдено:" + word)

        # Вернуть список идентификаторов
        return rowidList

    # 2. Поиск комбинаций из всех искомых слов в проиндексированных url-адресах
    def getMatchRows(self, queryString):
        # :param queryString: поисковый запрос пользователя
        # :return: 1) список вхождений формата (urlId, loc_q1, loc_q2, ...) loc_qN позиция на странице Nго слова из поискового запроса  "q1 q2 ..."

        # Разбить поисковый запрос на слова по пробелам
        queryString = queryString.lower()
        wordsList = queryString.split(' ')

        # Получить идентификаторы искомых слов
        wordsidList = self.getWordsIds(queryString)

        # Создать переменную для полного SQL-запроса
        sqlFullQuery = """"""

        # Создать объекты-списки для дополнений SQL-запроса
        sqlpart_Name = list()  # Имена столбцов
        sqlpart_Join = list()  # INNER JOIN
        sqlpart_Condition = list()  # Условия WHERE

        # Конструктор SQL-запроса (заполнение обязательной и дополнительных частей)
        # Обход в цикле каждого искомого слова и добавление в SQL-запрос соответствующих частей
        for wordIndex in range(0, len(wordsList)):

            # Получение идентификатора слова
            wordID = wordsidList[wordIndex]

            if wordIndex == 0:
                # Обязательная часть для первого слова
                sqlpart_Name.append(
                    """w0.fk_urlId    fk_urlId  --идентификатор url-адреса""")
                sqlpart_Name.append(
                    """   , w0.location w0_loc --положение первого искомого слова""")
                sqlpart_Condition.append(
                    """WHERE w0.fk_wordId={}     -- совпадение w0 с первым словом """.format(wordID))

            else:
                # Дополнительная часть для 2,3,.. искомых слов
                if len(wordsList) >= 2:
                    # Проверка, если текущее слово - второе и более
                    # Добавить в имена столбцов
                    sqlpart_Name.append(
                        """ , w{}.location w{}_loc --положение следующего искомого слова""".format(wordIndex, wordIndex))

                    # Добавить в sql INNER JOIN
                    sqlpart_Join.append("""INNER JOIN wordLocation w{}  -- назначим псевдоним w{} для второй из соединяемых таблиц
                      on w0.fk_urlId=w{}.fk_urlId -- условие объединения""".format(wordIndex, wordIndex, wordIndex))
                    # Добавить в sql ограничивающее условие
                    sqlpart_Condition.append(
                        """  AND w{}.fk_wordId={} -- совпадение w{}... с cоответсвующим словом """.format(wordIndex, wordID, wordIndex))
                    pass
            pass

        # Объеднение запроса из отдельных частей

        # Команда SELECT
        sqlFullQuery += "SELECT "

        # Все имена столбцов для вывода
        for sqlpart in sqlpart_Name:
            sqlFullQuery += "\n"
            sqlFullQuery += sqlpart

        # обязательная часть таблица-источник
        sqlFullQuery += "\n"
        sqlFullQuery += "FROM wordLocation w0 "

        # часть для объединения таблицы INNER JOIN
        for sqlpart in sqlpart_Join:
            sqlFullQuery += "\n"
            sqlFullQuery += sqlpart

        # обязательная часть и дополнения для блока WHERE
        for sqlpart in sqlpart_Condition:
            sqlFullQuery += "\n"
            sqlFullQuery += sqlpart

        # Выполнить SQL-запроса и извлечь ответ от БД
        print(sqlFullQuery)
        cur = self.con.execute(sqlFullQuery)
        rows = [row for row in cur]

        return rows, wordsidList

    # 3. Нормализация ранга
    def normalizeScores(self, scores, smallIsBetter=0):
        resultDict = dict()  # словарь с результатом

        vsmall = 0.00001  # создать переменную vsmall - малая величина, вместо деления на 0
        minscore = min(scores.values())  # получить минимум
        maxscore = max(scores.values())  # получить максимум

        # перебор каждой пары ключ значение
        for (key, val) in scores.items():

            if smallIsBetter:
                # Режим МЕНЬШЕ вх. значение => ЛУЧШЕ
                # ранг нормализованный = мин. / (тек.значение  или малую величину)
                resultDict[key] = float(minscore) / max(vsmall, val)
            else:
                # Режим БОЛЬШЕ  вх. значение => ЛУЧШЕ вычислить макс и разделить каждое на макс
                # вычисление ранга как доли от макс.
                # ранг нормализованный = тек. значения / макс.
                resultDict[key] = float(val) / maxscore

        return resultDict


# ------------------------------------------

# Основная функция main()


def main():
    mySeacher = Seacher("test.db")
    mySearchQuery = "новости россии"
    rowsLoc, wordsidList = mySeacher.getMatchRows(mySearchQuery)
    print("-----------------------")
    print(mySearchQuery)
    print(wordsidList)
    for location in rowsLoc:
        print(location)


# -----------------------------------------
main()
