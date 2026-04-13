import pprint
import sqlite3
conn = sqlite3.connect('BD_CANDY_SHOP.bd')
c = conn.cursor()
pp = pprint.PrettyPrinter(indent=1, width=80, compact=False)
pp1 = pprint.PrettyPrinter(indent=2, width=20, compact=False)
c.execute('''PRAGMA foreign_keys =1''')
c.execute('''drop table if exists Calorie_Level''')
c.execute('''drop table if exists Magazine_Sales''')
c.execute('''drop table if exists Sale''')
c.execute('''drop table if exists Customers''')
c.execute('''drop table if exists Product_Ingredients''')
c.execute('''drop table if exists Ingredients''')
c.execute('''drop table if exists Products''')
c.execute('''drop table if exists Difficulty_Level''')
c.execute('''drop table if exists Category''')
#Категория
c.execute('''create table if not exists Category (
  id_category integer NOT NULL,
  category_name char(50) NOT NULL,
  parent_category_name char(50) NOT NULL,
  CONSTRAINT PK_Category_id_category PRIMARY KEY (id_category)
  )''')
#Уровень сожности
c.execute('''create table if not exists Difficulty_Level (
  difficulty_level integer NOT NULL,
  difficulty_name char(50) NOT NULL,
  price double NOT NULL,
  CONSTRAINT CH_Difficulty_Level_Price check (price > 0),
  CONSTRAINT PK_Difficulty_Level_difficulty_level PRIMARY KEY (difficulty_level)
  )''')
#Товары
c.execute('''create table if not exists Products (
  id_product integer NOT NULL,
  difficulty_level int NOT NULL,
  id_category integer NOT NULL,
  name char(100) NOT NULL,
  CONSTRAINT PK_Products_Id_product PRIMARY KEY (id_product)
  CONSTRAINT FK_Product_difficulty_level FOREIGN KEY (difficulty_level) REFERENCES Difficulty_Level( difficulty_level),
  CONSTRAINT FK_Product_id_category FOREIGN KEY (id_category) REFERENCES Category(id_category)
  )''')
#Ингредиенты
c.execute('''create table if not exists Ingredients (
  id_ingredients integer NOT NULL, 
  name char(50) NOT NULL,
  portion char(50) NOT NULL,
  calories int(10) NOT NULL,
  price double NOT NULL,
  CONSTRAINT CH_Ingredients_Price check (price > 0),
  CONSTRAINT PK_Ingredients_id_ingredients PRIMARY KEY (id_ingredients)
  )''')
#Ингредиенты в товаре
c.execute('''create table if not exists Product_Ingredients (
  id_product int NOT NULL,
  id_ingredients int NOT NULL,
  quantity double NOT NULL
  CONSTRAINT CH_Product_Ingredients_quantity check (quantity > 0),
  CONSTRAINT PK_Product_Ingredients_id_ingredients_id_product PRIMARY KEY (id_ingredients, id_product),
  CONSTRAINT FK_Product_Ingredients_id_ingredients FOREIGN KEY (id_ingredients) REFERENCES Ingredients( id_ingredients),
  CONSTRAINT FK_Product_Ingredients_id_product FOREIGN KEY (id_product) REFERENCES Products(id_product)
  )''')
#Покупатели
c.execute('''create table if not exists Customers (
  id_customer integer NOT NULL, 
  name char(50) NOT NULL,
  phone char(11) NOT NULL,
  CONSTRAINT UQ_Customers_Phone UNIQUE (phone),
  CONSTRAINT PK_Customers_Id_customer PRIMARY KEY (id_customer)
  )''')
#Покупка
c.execute('''create table if not exists Sale (
  id_sale integer NOT NULL,
  id_customer int NOT NULL,
  date_sale text NOT NULL default current_date,
  CONSTRAINT PK_Sale_Id_sale PRIMARY KEY (id_sale),
  CONSTRAINT FK_Sale_Id_customer FOREIGN KEY (id_customer) REFERENCES Customers(id_customer))''')
#Журнал покупок
c.execute('''create table if not exists Magazine_Sales (
  id_sale integer NOT NULL,
  id_product int NOT NULL,
  quantity int NOT NULL,
  price double NOT NULL,
  CONSTRAINT CH_Magazine_Sales_Price check (price > 0),
  CONSTRAINT CH_Magazine_Sales_quantity check (quantity > 0),
  CONSTRAINT PK_Magazine_Sales_id_sale_id_product PRIMARY KEY (id_sale, id_product),
  CONSTRAINT FK_Magazine_Sales_id_sale FOREIGN KEY ( id_sale) REFERENCES Sale( id_sale),
  CONSTRAINT FK_Magazine_Sales_id_product FOREIGN KEY (id_product) REFERENCES Products( id_product)
  )''')
#Уровни калорийности
c.execute('''create table if not exists Calorie_Level (
  id_level integer NOT NULL,
  range int(10) NOT NULL
  )''')

#Триггеры
# Триггер для обновления цен при изменении стоимости ингредиентов
c.execute('''CREATE TRIGGER update_price_on_ingredient_price_change
AFTER UPDATE OF price ON Ingredients
FOR EACH ROW
WHEN OLD.price <> NEW.price
BEGIN
    UPDATE Magazine_Sales
    SET price = (
        SELECT ROUND(
            (SELECT SUM(i.price * pi.quantity) * 1.3 --(наценка 30%)
                FROM Product_Ingredients pi
                JOIN Ingredients i ON pi.id_ingredients = i.id_ingredients
                WHERE pi.id_product = Magazine_Sales.id_product) + 
            (SELECT dl.price
                FROM Products p
                JOIN Difficulty_Level dl ON p.difficulty_level = dl.difficulty_level
                WHERE p.id_product = Magazine_Sales.id_product), 2))
    WHERE id_product IN (SELECT id_product FROM Product_Ingredients 
    WHERE id_ingredients = NEW.id_ingredients);
END''')

#При добавлении нового ингредиента в продукт
c.execute('''CREATE TRIGGER update_price_on_ingredient_insert
AFTER INSERT ON Product_Ingredients
FOR EACH ROW
BEGIN
    UPDATE Magazine_Sales
    SET price = ROUND(
      (SELECT SUM(i.price * pi.quantity) * 1.3 
        FROM Product_Ingredients pi
        JOIN Ingredients i ON pi.id_ingredients = i.id_ingredients
        WHERE pi.id_product = NEW.id_product) + 
      (SELECT dl.price
        FROM Products p
        JOIN Difficulty_Level dl ON p.difficulty_level = dl.difficulty_level
        WHERE p.id_product = NEW.id_product), 2)
    WHERE id_product = NEW.id_product;
END''')

#при изменении состава товара
c.execute('''CREATE TRIGGER update_price_on_ingredient_update
AFTER UPDATE ON Product_Ingredients
FOR EACH ROW
BEGIN
    UPDATE Magazine_Sales
    SET price = ROUND((SELECT SUM(i.price * pi.quantity) * 1.3
        FROM Product_Ingredients pi
        JOIN Ingredients i ON pi.id_ingredients = i.id_ingredients
        WHERE pi.id_product = NEW.id_product) + 
      (SELECT dl.price
        FROM Products p
        JOIN Difficulty_Level dl ON p.difficulty_level = dl.difficulty_level
        WHERE p.id_product = NEW.id_product), 2)
    WHERE id_product = NEW.id_product;
END''')

#Наполнение 

# Категории
categories = [
    (1, 'Шоколадное', 'Печенье'),
    (2, 'Ореховое', 'Печенье'),
    (3, 'Вегетарианские', 'Печенье'),
    (4, 'С ягодами', 'Печенье'),
    (5, 'С фруктами', 'Печенье'),
    (6, 'Дрожжевое', 'Тесто'),
    (7, 'Бездрожжевое', 'Тесто')
]
c.executemany('INSERT INTO Category VALUES (?, ?, ?)', categories)

# Уровни сложности
difficulty_levels = [
    (1, 'Легкий', 50.0),
    (2, 'Средний', 100.0),
    (3, 'Сложный', 150.0)
]
c.executemany('INSERT INTO Difficulty_Level VALUES (?, ?, ?)', difficulty_levels)

# Товары
products = [
    (1, 2, 1, 'Шоколадное печенье'),
    (2, 1, 2, 'Ореховое печенье'),
    (3, 3, 3, 'Веганское печенье'),
    (4, 2, 4, 'Клубнично-ванильное печенье'),
    (5, 1, 5, 'Лимонное печенье'),
    (6, 3, 7, 'Песочное тесто без дрожжей')
]
c.executemany('INSERT INTO Products VALUES (?, ?, ?, ?)', products)

# Ингредиенты
ingredients_data = [
    (1, 'Мука пшеничная', '100g', 364, 7.9),
    (2, 'Сахар', '100g', 398, 11.9),
    (3, 'Сливочное масло 82.5%', '100g', 717, 105.0),
    (4, 'Яйца куриные', '100g', 155, 25.8),
    (5, 'Разрыхлитель теста', '100g', 79, 250.0),
    (6, 'Соль', '100g', 0, 6.9),
    (7, 'Ванильный экстракт', '100ml', 288, 745.0),
    (8, 'Какао-порошок', '100g', 228, 169.0),
    (9, 'Грецкие орехи', '100g', 654, 199.0),
    (10, 'Миндаль', '100g', 578, 333.0),
    (11, 'Фундук', '100g', 628, 266.0),
    (12, 'Корица молотая', '100g', 247, 595.0),
    (13, 'Кокосовое масло', '100ml', 862, 174.5),
    (14, 'Яблочное пюре', '100g', 52, 69.0),
    (15, 'Сушеная клубника', '100g', 286, 622.5),
    (16, 'Лимонный сок', '100ml', 22, 24.9)
]
c.executemany('INSERT INTO Ingredients VALUES (?, ?, ?, ?, ?)', ingredients_data)
#Ингредиенты в товаре
product_ingredients = [
    #Шоколадное печенье
    (1,1,1.0),#Мука
    (1,2,0.5),#Сахар
    (1,3,0.7),#Масло
    (1,4,0.5),#Яйцо
    (1,8,0.3),#Какао
    (1,5,0.05),#Разрыхлитель
    #Ореховое печенье
    (2,1,1.0),#Мука
    (2,2,0.6),#Сахар
    (2,3,0.8),#Масло
    (2,4,0.5),#Яйцо
    (2,9,0.3),#Грецкие орехи
    (2,10,0.2),#Миндаль
    (2,11,0.2),#Фундук
    #Веганское печенье
    (3,1,1.0),#Мука
    (3,13,0.6),#Кокосовое масло
    (3,14,0.5),#Яблочное пюре
    #Клубнично-ванильное печенье
    (4,1,1.0),
    (4,2,0.7),
    (4,3,0.6),
    (4,15,0.3),#Сушеная клубника
    (4,7,0.05),#Ванильный экстракт
    #Лимонное печенье
    (5,1,1.0),
    (5,2,0.6),
    (5,3,0.7),
    (5,16,0.2),#Лимонный сок
    #Песочное тесто без дрожжей
    (6,1,6.0),
    (6,3,4.0)
]
c.executemany('''INSERT INTO Product_Ingredients VALUES (?, ?, ?) ''', product_ingredients)
#Покупатели
customers = [
    (1, 'Иванов Сергей', '89101234567'),
    (2, 'Ленская Катя', '89213456789'),
    (3, 'Демидов Олег', '89317654321'),
    (4, 'Афанасьев Виктор', '89409876543'),
    (5, 'Пажская Вера', '89512345678')
]
c.executemany('''INSERT INTO Customers VALUES (?, ?, ?)''', customers)
#Покупки
sales = [
    (1, 1, '2025-01-01'),
    (2, 3, '2025-01-02'),
    (3, 2, '2025-01-03'),
    (4, 5, '2025-01-04'),
    (5, 4, '2025-01-04'),
    (6, 1, '2025-01-05')
]
c.executemany('''INSERT INTO Sale VALUES (?, ?, ?)''', sales)
#журнала покупок
magazine_sales = [
    (1, 1, 2, 350.0),
    (1, 3, 1, 200.0),
    (2, 2, 3, 600.0),
    (3, 5, 1, 170.0),
    (4, 4, 2, 350.0),
    (5, 6, 1, 500.0),
    (5, 1, 1, 350.0),  
    (6, 2, 1, 600.0),
]
c.executemany('''INSERT INTO Magazine_Sales VALUES (?, ?, ?, ?)''', magazine_sales)

# уровни калорийности
calorie_levels = [
    (1, 500),
    (2, 1000),
    (3, 5000)
]
c.executemany('''INSERT INTO Calorie_Level VALUES (?, ?)''', calorie_levels)

# Новые таблицы для фильтрации
c.execute('''create table if not exists Occasions (
    id_occasion integer PRIMARY KEY,
    occasion_name char(50) NOT NULL
)''')

c.execute('''create table if not exists Product_Occasion (
    id_product integer NOT NULL,
    id_occasion integer NOT NULL,
    PRIMARY KEY (id_product, id_occasion),
    FOREIGN KEY (id_product) REFERENCES Products(id_product),
    FOREIGN KEY (id_occasion) REFERENCES Occasions(id_occasion)
)''')

c.execute('''ALTER TABLE Ingredients ADD COLUMN ingredient_group TEXT''')

# Праздники
occasions_data = [
    (1, 'Пасха'),
    (2, 'Новый год'),
    (3, 'Детский праздник'),
    (4, 'День рождения')
]
c.executemany('INSERT OR IGNORE INTO Occasions VALUES (?, ?)', occasions_data)

# Связь продуктов с праздниками (примеры)
product_occasion_data = [
    (1, 3),  # Шоколадное печенье -> Детский праздник
    (2, 3),  # Ореховое печенье -> Детский праздник
    (2, 4),  # Ореховое печенье -> День рождения
    (3, 1),  # Веганское печенье -> Пасха? (можно настроить)
    (4, 4),  # Клубнично-ванильное -> День рождения
    (5, 2),  # Лимонное печенье -> Новый год
    (6, 3),  # Песочное тесто -> Детский праздник
]
c.executemany('INSERT OR IGNORE INTO Product_Occasion VALUES (?, ?)', product_occasion_data)

# Заполняем группы ингредиентов
# Сначала обновим существующие записи
ingredient_groups = [
    (1, 'мука'),
    (2, 'сахар'),
    (3, 'молочные'),
    (4, 'яйца'),
    (5, 'разрыхлитель'),
    (6, 'соль'),
    (7, 'ароматизаторы'),
    (8, 'какао'),
    (9, 'орехи'),
    (10, 'орехи'),
    (11, 'орехи'),
    (12, 'специи'),
    (13, 'масла'),
    (14, 'фрукты/ягоды'),
    (15, 'фрукты/ягоды'),
    (16, 'фрукты/ягоды'),
]
for ing_id, group in ingredient_groups:
    c.execute('UPDATE Ingredients SET ingredient_group = ? WHERE id_ingredients = ?', (group, ing_id))

# Добавляем колонку price в Products
c.execute('''ALTER TABLE Products ADD COLUMN price REAL''')
# Примерные цены (можно вычислить позже, но пока вручную)
product_prices = [
    (1, 350.0),
    (2, 600.0),
    (3, 200.0),
    (4, 350.0),
    (5, 170.0),
    (6, 500.0),
]
for prod_id, price in product_prices:
    c.execute('UPDATE Products SET price = ? WHERE id_product = ?', (price, prod_id))

# Добавляем диетические флаги
c.execute('''ALTER TABLE Products ADD COLUMN has_nuts INTEGER DEFAULT 0''')
c.execute('''ALTER TABLE Products ADD COLUMN has_gluten INTEGER DEFAULT 1''')
c.execute('''ALTER TABLE Products ADD COLUMN has_dairy INTEGER DEFAULT 1''')
c.execute('''ALTER TABLE Products ADD COLUMN is_vegan INTEGER DEFAULT 0''')

# Обновим флаги для каждого продукта на основе ингредиентов
# Пройдём по всем продуктам
products_rows = c.execute('SELECT id_product FROM Products').fetchall()
for (prod_id,) in products_rows:
    # Проверяем наличие орехов
    nuts = c.execute('''
        SELECT 1 FROM Product_Ingredients pi
        JOIN Ingredients i ON pi.id_ingredients = i.id_ingredients
        WHERE pi.id_product = ? AND i.ingredient_group = 'орехи' LIMIT 1
    ''', (prod_id,)).fetchone()
    has_nuts = 1 if nuts else 0

    # Проверяем наличие глютена (мука пшеничная – содержит глютен)
    gluten = c.execute('''
        SELECT 1 FROM Product_Ingredients pi
        JOIN Ingredients i ON pi.id_ingredients = i.id_ingredients
        WHERE pi.id_product = ? AND i.name LIKE '%мука%' LIMIT 1
    ''', (prod_id,)).fetchone()
    has_gluten = 1 if gluten else 0  # предполагаем, что мука пшеничная есть

    # Проверяем наличие молочных продуктов (ингредиенты с группой 'молочные')
    dairy = c.execute('''
        SELECT 1 FROM Product_Ingredients pi
        JOIN Ingredients i ON pi.id_ingredients = i.id_ingredients
        WHERE pi.id_product = ? AND i.ingredient_group = 'молочные' LIMIT 1
    ''', (prod_id,)).fetchone()
    has_dairy = 1 if dairy else 0

    # Проверяем, является ли продукт веганским (нет яиц, молочных, мёда и т.п.)
    # Яйца и молочные не должны присутствовать
    non_vegan = c.execute('''
        SELECT 1 FROM Product_Ingredients pi
        JOIN Ingredients i ON pi.id_ingredients = i.id_ingredients
        WHERE pi.id_product = ? 
          AND (i.ingredient_group IN ('яйца', 'молочные') OR i.name LIKE '%мёд%')
        LIMIT 1
    ''', (prod_id,)).fetchone()
    is_vegan = 0 if non_vegan else 1

    c.execute('''
        UPDATE Products 
        SET has_nuts = ?, has_gluten = ?, has_dairy = ?, is_vegan = ?
        WHERE id_product = ?
    ''', (has_nuts, has_gluten, has_dairy, is_vegan, prod_id))


#Фиксация изменений базы
conn.commit()
#Закрытие связи с базой
conn.close()
