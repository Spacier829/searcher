import requests
import sqlite3
import bs4
import re

ignoreWordList = ["и", "а", "но", "однако", "или", "что", "чтобы", "да", "также", "словно",
                  "будто", "тоже", "как", "зато", "либо", "ибо", "хотя", "пусть", "пускай", "точно"]


class Crawler:

  # 0. Конструктор Инициалиализации паука с параметрами БД
    def __init__(self, dbFileName):
        print("0. Конструктор")
        # Подключение к файлу БД
        self.connection = sqlite3.connect(dbFileName)
        print("БД {} создана".format(dbFileName))

        print("------------------------------------")

    # 0. Деструктор
    def __del__(self):
        # Закрытие соединения с БД
        self.connection.close()

    # 1. Индексирование одной страницы
    def addIndex(self, soup, url):
        print("1. Индексирование страницы: ", url)
        if self.isIndexed(url):
            print("Уже проиндексирована")
            return
        else:
            # Получение сплошного текста
            text = self.getTextOnly(soup)

            # Разделение этого текста на слова
            words = self.separateWords(text)

            # Получение идентификатора url из таблицы
            urlId = self.getEntryId('urlList', 'url', url)

            # Связывание каждого слова с url
            for i in range(len(words)):
                word = words[i]
                # Проверка входимости слова в игнорируемый список
                if word in ignoreWordList:
                    continue
                else:
                    # Получение wordId из wordList для формирования таблицы wordLocation
                    wordId = self.getEntryId('wordList', 'word', word)
                    sqlInsert = "INSERT INTO wordLocation ( fk_urlId, fk_wordId, location ) VALUES ( '{}', '{}', '{}' ) ;".format(
                        urlId, wordId, i)
                    self.cursor.execute(sqlInsert)
        self.connection.commit()

        print("------------------------------------")

    # 2. Получение текста страницы
    def getTextOnly(self, soup):
        # Преобразование супа к строке
        soupText = soup.get_text()
        # Если преобразование не удалось
        if soupText == None:
            # Разбиение разметки на части
            soupContent = soup.contents
            resultText = ''
            # Перебор элементов разметки, чтобы был найден текстовый элемент
            for sub in soupContent:
                subText = self.getTextOnly(sub)
                resultText += subText+'\n'
            return resultText
        else:
            # Устранение
            return soupText.strip()

    # 3. Разбиение текста на слова
    def separateWords(self, text):
        # Регулярное выражение для разделения
        splitter = re.compile('\W')
        return [s.lower() for s in splitter.split(text) if s != '']

    # 4. Проиндексирован ли URL (проверка наличия URL в БД)

    def isIndexed(self, url):
        # Поиск в таблице записи с этим url
        sqlSelect = "SELECT rowid FROM urllist WHERE url= '{}';".format(url)
        # print(sqlSelect)
        result = self.cursor.execute(sqlSelect).fetchone()
        #self.connection.commit()
        if result == None:
            # Строки нет
            return False
        else:
            # Строка есть
            sqlSelect = "SELECT rowid FROM wordLocation WHERE fk_urlId = '{}';".format(
                result)
            result = self.cursor.execute(sqlSelect).fetchone()
            #self.connection.commit()
            if result == None:
                return False
            else:
                return True

    # 5. Добавление ссылки с одной страницы на другую
    def addLinkRef(self, urlFrom, urlTo, linkText):
        # Вставка в таблицу fromUrlId и toUrlId
        sqlInsert = "INSERT INTO linkBetweenUrl (fk_fromUrlId, fk_toUrlId) VALUES ('{}','{}');".format(
            urlFrom, urlTo)
        # print(sqlInsert)
        self.cursor.execute(sqlInsert)

        # Разбиение текста ссылка на слова
        linkTextWords = self.separateWords(linkText)

        # Цикл для занесения в таблицу linkWords связи названия ссылки и url
        for i in range(len(linkTextWords)):
            linkTextWord = linkTextWords[i]
            if linkTextWord in ignoreWordList:
                continue
            else:
                linkTextWordId = self.getEntryId(
                    'wordList', 'word', linkTextWord)
                sqlSelect = "SELECT rowid FROM linkBetweenUrl WHERE fk_fromUrlId= '{}' AND fk_toUrlId = '{}';".format(
                    urlFrom, urlTo)
                # Получение urlId с fromUrl и toUrl
                linkUrlId = self.cursor.execute(sqlSelect).fetchone()[0]

                # Наполнение таблицы linkWords по urlId и wordId
                sqlInsert = "INSERT INTO linkWords (fk_wordId, fk_linkUrlId) VALUES ('{}', '{}');".format(
                    linkTextWordId, linkUrlId)
                # print(sqlInsert)
                self.cursor.execute(sqlInsert)

    # 6. Паук
    def crawl(self, urlList, depth=2):
        print("6. Обход страниц")
        for currentDepth in range(depth):
            # Заготовка для нового списка url, которые будут найдены на текущем url
            newUrlList = set()
            counter = 0
            # Обход каждого url из переданного списка
            for url in urlList.copy():

                try:
                    # Текст для красивого отображения
                    print(
                        "{} - {}/{} Попытка открыть страницу {}".format(currentDepth + 1, counter + 1, len(urlList.copy()), url))

                    # Получение HTML-разметки страницы текущего url
                    htmlDoc = requests.get(url).text
                except Exception as error:
                    print(error)
                    print("Не удалось открыть страницу")
                    # counter += 1
                    continue

                # Парсирование для работы тегов
                soup = bs4.BeautifulSoup(htmlDoc, "html.parser")

                # Удаление параметров, которые не содердат "текстовый" смысл
                if soup.title != None:
                    soup.title.decompose()
                listUnwantedItems = ['script', 'style',
                                     'meta', 'head', 'class', 'span', 'media', '.apk']
                for script in soup.find_all(listUnwantedItems):
                    script.decompose()

                # Добавление содержимого в индекс
                self.addIndex(soup, url)

                # Получение списка тегов <a> с текущей страницы
                links = soup.findAll('a')
                for link in links:
                    # Проверка наличия атрибута 'href'
                    if ('href' in link.attrs):
                        newUrl = link.attrs['href']
                        # Работа только со ссылками, начинающимися на 'http', отбрасывая якори и пустые
                        if newUrl[0:4] == 'http' and not self.isIndexed(newUrl) and newUrl[-4:] != '.apk':
                            print(newUrl)
                            # Добавление ссылки в новый список для обхода
                            newUrlList.add(newUrl)
                            # Вывод текста ссылки
                            linkText = self.getTextOnly(link)
                            print(linkText)
                            # Вставка ссылки с одной страницы на другую
                            self.addLinkRef(url, newUrl, linkText)
                counter += 1
                if (counter % 10 == 0):
                    self.monitorDB(counter)
            # Формирование нового списка для обхода
            urlList = newUrlList
            self.monitorDB(counter)
            # Применение изменений в БД
            self.connection.commit()

    # 7. Инициализация таблиц в БД
    def initDB(self):
        print("7. Инициирование таблиц")
        # Курсор для программирования БД
        self.cursor = self.connection.cursor()

        # 1. Создание таблицы wordList
        # Удаление таблицы, если она уже создана
        sqlDropTable = "DROP TABLE IF EXISTS wordList"
        # print(sqlDropTable)
        # Выполнение инструкции SQL
        self.cursor.execute(sqlDropTable)

        # Создание новой таблицы, если такой еще нет
        sqlCreateTable = """CREATE TABLE IF NOT EXISTS wordList(
                          rowId INTEGER PRIMARY KEY AUTOINCREMENT,
                          word TEXT NOT NULL,
                          isFiltred INTEGER NOT NULL);"""
        # print(sqlCreateTable)
        self.cursor.execute(sqlCreateTable)

        # 2. Создание таблицы urlList
        # Удаление таблицы, если она уже создана
        sqlDropTable = "DROP TABLE IF EXISTS urlList"
        # print(sqlDropTable)
        # Выполнение инструкции SQL
        self.cursor.execute(sqlDropTable)

        # Создание новой таблицы, если такой еще нет
        sqlCreateTable = """CREATE TABLE IF NOT EXISTS urlList(
                          rowId INTEGER PRIMARY KEY AUTOINCREMENT,
                          url TEXT NOT NULL);"""
        # print(sqlCreateTable)
        self.cursor.execute(sqlCreateTable)

        # 3. Создание таблицы wordLocation
        # Удаление таблицы, если она уже создана
        sqlDropTable = "DROP TABLE IF EXISTS wordLocation"
        # print(sqlDropTable)
        # Выполнение инструкции SQL
        self.cursor.execute(sqlDropTable)

        # Создание новой таблицы, если такой еще нет
        sqlCreateTable = """CREATE TABLE IF NOT EXISTS wordLocation(
                          rowId INTEGER PRIMARY KEY AUTOINCREMENT,
                          fk_urlId INTEGER NOT NULL,
                          fk_wordId INTEGER NOT NULL,
                          location INTEGER NOT NULL,
                          FOREIGN KEY (fk_urlId) REFERENCES urlList (rowid) ON DELETE CASCADE ON UPDATE NO ACTION,
                          FOREIGN KEY (fk_wordId) REFERENCES wordList (rowid) ON DELETE CASCADE ON UPDATE NO ACTION);"""
        # print(sqlCreateTable)
        self.cursor.execute(sqlCreateTable)

        # 4. Создание таблицы linkBetweenUrl
        # Удаление таблицы, если она уже создана
        sqlDropTable = "DROP TABLE IF EXISTS linkBetweenUrl"
        # print(sqlDropTable)
        # Выполнение инструкции SQL
        self.cursor.execute(sqlDropTable)

        # Создание новой таблицы, если такой еще нет
        sqlCreateTable = """CREATE TABLE IF NOT EXISTS linkBetweenUrl(
                          rowId INTEGER PRIMARY KEY AUTOINCREMENT,
                          fk_fromUrlId INTEGER NOT NULL,
                          fk_toUrlId INTEGER NOT NULL,
                          FOREIGN KEY (fk_fromUrlId) REFERENCES urlList (rowid) ON DELETE CASCADE ON UPDATE NO ACTION,
                          FOREIGN KEY (fk_toUrlId) REFERENCES urlList (rowid) ON DELETE CASCADE ON UPDATE NO ACTION);"""
        # print(sqlCreateTable)
        self.cursor.execute(sqlCreateTable)

        # 5. Создание таблицы linkWords
        # Удаление таблицы, если она уже создана
        sqlDropTable = "DROP TABLE IF EXISTS linkWords"
        # print(sqlDropTable)
        # Выполнение инструкции SQL
        self.cursor.execute(sqlDropTable)

        # Создание новой таблицы, если такой еще нет
        sqlCreateTable = """CREATE TABLE IF NOT EXISTS linkWords(
                          rowId INTEGER PRIMARY KEY AUTOINCREMENT,
                          fk_wordId INTEGER NOT NULL,
                          fk_linkUrlId INTEGER NOT NULL,
                          FOREIGN KEY (fk_wordId) REFERENCES wordList (rowid) ON DELETE CASCADE ON UPDATE NO ACTION,
                          FOREIGN KEY (fk_linkUrlId) REFERENCES linkBetweenUrl (rowid) ON DELETE CASCADE ON UPDATE NO ACTION);"""
        # print(sqlCreateTable)
        self.cursor.execute(sqlCreateTable)

        # 6. Создание таблицы couterRows
        # Удаление таблицы, если она уже создана
        sqlDropTable = "DROP TABLE IF EXISTS counterRows"
        # print(sqlDropTable)
        # Выполнение инструкции SQL
        self.cursor.execute(sqlDropTable)

        # Создание новой таблицы, если такой еще нет
        sqlCreateTable = """CREATE TABLE IF NOT EXISTS counterRows(
                          rowId INTEGER PRIMARY KEY AUTOINCREMENT,
                          wordList INTEGER,
                          urlList INTEGER,
                          wordLocation INTEGER,
                          linkBetweenUrl INTEGER,
                          linkWords INTEGER);"""
        # print(sqlCreateTable)
        self.cursor.execute(sqlCreateTable)

        # Применение изменений в БД
        self.connection.commit()

        print("------------------------------------")

    # 8. Получение идентификатора и добавление записи, если такой еще нет
    def getEntryId(self, tableName, fieldName, value):
        print("------------------------------------")
        print("8. Получение ID элемента")

        sqlSelect = "SELECT rowid FROM {} WHERE {} =  '{}';".format(
            tableName,  fieldName, value)
        print(sqlSelect)
        result = self.cursor.execute(sqlSelect).fetchone()

        if result == None:
            # Внесение, если нет записи в БД
            sqlInsert = "INSERT INTO {} ( {} ) VALUES ( '{}' )  ;".format(
                tableName, fieldName, value)

            # у wordList есть доп. свойство "isFiltred", которое сейчас не используется, но в методочке указано, возможно потом пригодится, поэтому его обрабаываем отдельно, если такой записи нет
            if (tableName == 'wordList'):
                sqlInsert = "INSERT INTO wordList ( {}, {} ) VALUES ( '{}', '{}' )  ;".format(
                    fieldName, "isFiltred", value, 0)
            print(sqlInsert)
            result = self.cursor.execute(sqlInsert)
            # Если запись новая, то она в конце, поэтому возвращаем последнюю строку
            return result.lastrowid
        else:
            # Возвращаем ту, которая соответсвует, в выводе есть запятая, поэтому берем нулевой элемент
            return result[0]

    # 9. Метод мониторинга
    def monitorDB(self, counter):
        print("------------------------------------")
        print("9. Мониторинг БД")
        print("Пройдено {} страниц".format(counter))
        # Выбираем все записи в таблице
        sqlSelect = "SELECT * FROM wordList;"
        # print(sqlSelect)
        sqlExecute = self.cursor.execute(sqlSelect)

        wordListRows = len(sqlExecute.fetchall())
        # В print выводим количество этих записей
        print("Кол-во записей в wordList: ", wordListRows)

        sqlSelect = "SELECT * FROM urlList;"
        # print(sqlSelect)
        sqlExecute = self.cursor.execute(sqlSelect)
        urlListRows = len(sqlExecute.fetchall())
        print("Кол-во записей в urlList: ", urlListRows)

        sqlSelect = "SELECT * FROM wordLocation;"
        # print(sqlSelect)
        sqlExecute = self.cursor.execute(sqlSelect)
        wordLocationRows = len(sqlExecute.fetchall())
        print("Кол-во записей в wordLocation: ", wordLocationRows)

        sqlSelect = "SELECT * FROM linkBetweenUrl;"
        # print(sqlSelect)
        sqlExecute = self.cursor.execute(sqlSelect)
        linkBetweenRows = len(sqlExecute.fetchall())
        print("Кол-во записей в linkBetweenUrl: ", linkBetweenRows)

        sqlSelect = "SELECT * FROM linkWords;"
        # print(sqlSelect)
        sqlExecute = self.cursor.execute(sqlSelect)
        linkWordsRows = len(sqlExecute.fetchall())
        print("Кол-во записей в linkWords: ", linkWordsRows)

        # Внесение данных в БД для анализа
        sqlInsert = """INSERT INTO counterRows (wordList, urlList, wordLocation, linkWords, linkBetweenUrl) values ('{}', '{}', '{}', '{}', '{}');""".format(
            wordListRows, urlListRows, wordLocationRows, linkWordsRows, linkBetweenRows)
        self.cursor.execute(sqlInsert)

        print("------------------------------------")


# Создаем класс и передаем ему имя файла БД
test = Crawler('test.db')
# Вызываем метод инициализации БД для создания скелета таблиц
test.initDB()

# Объявляем список url и добавляем в него ссылки
urlList = list()
urlList.append("https://ria.ru/20230916/boloto-1895894559.html")
# urlList.append("https://ria.ru/20230909/kabachki-1894764175.html")
# urlList.append("https://ria.ru/20230922/istorii-1898101454.html")


# Запускаем паука для обхода страниц
test.crawl(urlList)
