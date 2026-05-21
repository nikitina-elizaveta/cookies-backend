# ГЛАВА 3. РЕАЛИЗАЦИЯ СИСТЕМЫ

## 3.1. Реализация базы данных

### Создание таблиц

База данных реализована на основе спроектированной логической модели. Все таблицы были созданы с использованием SQL-скрипта, который определяет структуру, типы данных и ограничения целостности.

**Листинг 3.1.1. Создание таблицы Products**

```sql
CREATE TABLE Products (
  id_product integer NOT NULL,
  difficulty_level int NOT NULL,
  id_category integer NOT NULL,
  name char(100) NOT NULL, 
  price REAL, 
  has_nuts INTEGER DEFAULT 0, 
  has_gluten INTEGER DEFAULT 1, 
  has_dairy INTEGER DEFAULT 1, 
  is_vegan INTEGER DEFAULT 0, 
  image TEXT, 
  description TEXT, 
  weight TEXT, 
  has_artificial_flavorings INTEGER DEFAULT 0,
  CONSTRAINT PK_Products_Id_product PRIMARY KEY (id_product),
  CONSTRAINT FK_Product_difficulty_level FOREIGN KEY (difficulty_level) 
    REFERENCES Difficulty_Level(difficulty_level),
  CONSTRAINT FK_Product_id_category FOREIGN KEY (id_category) 
    REFERENCES Category(id_category)
);
```

**Особенности реализации:**

1. **Значения по умолчанию**: Для диетических флагов установлены значения по умолчанию, соответствующие наиболее распространённому случаю (например, `has_gluten DEFAULT 1` — большинство изделий содержат глютен).

2. **Внешние ключи**: Ограничения FOREIGN KEY обеспечивают ссылочную целостность — невозможно создать товар с несуществующей категорией или уровнем сложности.

3. **Гибкие типы данных**: Поля image, description, weight имеют тип TEXT для хранения данных переменной длины.

### Наполнение данными

После создания структуры таблицы были наполнены тестовыми данными для отладки системы. Данные включают:

- **Категории**: "Торты", "Пирожные", "Печенье", "Конфеты";
- **Уровни сложности**: "Простой", "Средний", "Сложный";
- **Ингредиенты**: Более 50 наименований с указанием калорийности и группы;
- **Продукция**: Ассортимент из нескольких десятков кондитерских изделий;
- **Праздники**: "День рождения", "Свадьба", "Новый год", "8 Марта", "День святого Валентина".

### Таблица FilterEvents для аналитики

Отдельного внимания заслуживает таблица FilterEvents, которая была специально разработана для системы аналитики:

```sql
CREATE TABLE FilterEvents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    filters TEXT NOT NULL,
    results_count INTEGER,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
```

**Обоснование структуры:**

- **session_id**: Позволяет идентифицировать уникальные сессии пользователей без использования cookies на сервере. Клиент генерирует уникальный идентификатор при первом посещении.
  
- **filters**: Хранит JSON-сериализованный объект с применёнными фильтрами. Использование TEXT позволяет сохранять произвольную структуру фильтров без изменения схемы БД.

- **results_count**: Количество товаров, найденных после применения фильтров. Позволяет анализировать, какие фильтры дают узкие/широкие результаты.

- **created_at**: Автоматическая временная метка для анализа поведения пользователей во времени.

## 3.2. Реализация серверной части (Backend)

### Структура проекта

Backend реализован в одном файле `main.py`, что обусловлено относительно небольшим размером проекта. Однако код структурирован по функциональным блокам:

```
main.py
├── Импорт библиотек
├── Модели данных (Pydantic)
├── Инициализация приложения FastAPI
├── Настройка CORS
├── Вспомогательные функции
├── Публичные endpoints
├── Endpoints авторизации
├── Защищённые endpoints (админка)
└── Endpoints аналитики
```

### Используемые библиотеки

**Листинг 3.2.1. Файл requirements.txt**

```txt
fastapi==0.135.1
uvicorn==0.42.0
pydantic==2.12.5
starlette==0.52.1
```

**Назначение библиотек:**

- **FastAPI**: Основной фреймворк для создания API;
- **Uvicorn**: ASGI-сервер для запуска приложения;
- **Pydantic**: Валидация данных и сериализация;
- **Starlette**: Асинхронный веб-фреймворк (используется внутри FastAPI).

### Модели данных Pydantic

Для валидации входных и выходных данных определены следующие модели:

**Листинг 3.2.2. Модели данных**

```python
class AdminLogin(BaseModel):
    username: str
    password: str

class FilterEvent(BaseModel):
    session_id: str
    filters: dict
    results_count: int | None = None

class OrderItem(BaseModel):
    product_id: int
    quantity: int
    price: float

class OrderRequest(BaseModel):
    customer_name: str
    customer_phone: str
    comment: Optional[str] = None
    items: List[OrderItem]
```

**Обоснование выбора Pydantic:**

1. **Автоматическая валидация**: При получении запроса FastAPI автоматически проверяет соответствие данных модели. Если данные некорректны, возвращается ошибка 422 с подробным описанием проблемы.

2. **Документация**: Модели используются для генерации схемы OpenAPI, что улучшает документацию.

3. **Сериализация**: Pydantic автоматически преобразует данные в JSON при возврате ответа.

### Настройка CORS

**Листинг 3.2.3. Конфигурация CORS**

```python
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:4200",
        "https://nikitina-elizaveta.github.io",
        "https://cookies-backend-0s6p.onrender.com",
        "https://nikitina-elizaveta-candy-shop-b1c6.twc1.net" 
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)
```

**Обоснование:**

CORS (Cross-Origin Resource Sharing) необходим, так как frontend и backend размещены на разных доменах. Список разрешённых origin ограничен конкретными адресами для безопасности.

### Функция подключения к базе данных

**Листинг 3.2.4. Функция get_db()**

```python
def get_db():
    conn = sqlite3.connect('BD_CANDY_SHOP.bd')
    conn.row_factory = sqlite3.Row 
    return conn
```

**Особенности:**

- **row_factory = sqlite3.Row**: Позволяет обращаться к полям результата по имени, а не по индексу, что улучшает читаемость кода.
  
- **Отсутствие пула соединений**: Для SQLite это допустимо, так как база данных работает в однопоточном режиме и файл блокируется при записи.

### Endpoint получения товаров с фильтрацией

Это ключевой endpoint клиентской части, реализующий сложную логику фильтрации.

**Листинг 3.2.5. Endpoint /api/products (фрагмент)**

```python
@app.get("/api/products")
def get_products(
    occasions: Optional[str] = Query(None),
    ingredient_groups: Optional[str] = Query(None),
    no_nuts: bool = False,
    no_gluten: bool = False,
    no_dairy: bool = False,
    vegan: bool = False,
    no_aroma: bool = False,
    sort: Optional[str] = Query(None, pattern="^(price_asc|price_desc)$")
):
    conn = get_db()
    query = "SELECT * FROM Products WHERE 1=1"
    params = []

    if occasions:
        occasion_ids = [int(id.strip()) for id in occasions.split(',')]
        placeholders = ','.join(['?'] * len(occasion_ids))
        query += f" AND id_product IN (SELECT id_product FROM Product_Occasion WHERE id_occasion IN ({placeholders}))"
        params.extend(occasion_ids)

    if ingredient_groups:
        groups = ingredient_groups.split(',')
        for group in groups:
            query += f" AND id_product IN (SELECT pi.id_product FROM Product_Ingredients pi JOIN Ingredients i ON pi.id_ingredients = i.id_ingredients WHERE i.ingredient_group = ?)"
            params.append(group)

    if no_nuts:
        query += " AND has_nuts = 0"
    if no_gluten:
        query += " AND has_gluten = 0"
    if no_dairy:
        query += " AND has_dairy = 0"
    if vegan:
        query += " AND is_vegan = 1"
    if no_aroma:
        query += " AND has_artificial_flavorings = 0"
    
    if sort == "price_asc":
        query += " ORDER BY price ASC"
    elif sort == "price_desc":
        query += " ORDER BY price DESC"

    cursor = conn.execute(query, params)
    products = cursor.fetchall()
    conn.close()
    return [dict(row) for row in products]
```

**Обоснование технических решений:**

1. **Динамическое построение запроса**: Запрос формируется постепенно в зависимости от переданных параметров. Это позволяет избежать избыточных условий в SQL.

2. **Защищённость от SQL-инъекций**: Используются параметризованные запросы (`?` плейсхолдеры), что полностью исключает возможность SQL-инъекций.

3. **Подмножества для связанных таблиц**: Для фильтрации по праздникам и ингредиентам используются подзапросы с IN, так как связь многие-ко-многим реализована через промежуточные таблицы.

4. **Валидация сортировки**: Параметр sort имеет паттерн `^(price_asc|price_desc)$`, что гарантирует безопасность ORDER BY (нельзя передать произвольный SQL-код).

5. **WHERE 1=1**: Начальное условие упрощает добавление последующих AND без проверки на первое условие.

### Endpoint сохранения событий фильтрации

**Листинг 3.2.6. Endpoint /api/analytics/filter**

```python
@app.post("/api/analytics/filter")
def save_filter_event(event: FilterEvent):
    try:
        conn = get_db()
        conn.execute(
            "INSERT INTO FilterEvents (session_id, filters, results_count) VALUES (?, ?, ?)",
            (event.session_id, json.dumps(event.filters), event.results_count)
        )
        conn.commit()
        conn.close()
        return {"status": "ok"}
    except Exception as e:
        print("ОШИБКА В /api/analytics/filter:", e)
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
```

**Особенности:**

- **json.dumps()**: Сериализация словаря фильтров в JSON-строку для хранения в TEXT-поле.
  
- **Обработка ошибок**: Try-except блок с логированием и traceback помогает отладке. Ошибка возвращается клиенту с кодом 500.

### Создание заказа с транзакцией

**Листинг 3.2.7. Endpoint /api/orders (фрагмент)**

```python
@app.post("/api/orders")
def create_order(order: OrderRequest):
    conn = get_db()
    try:
        conn.execute("BEGIN")
        
        # Вставляем покупателя
        cursor = conn.execute(
            "INSERT INTO Customers (name, phone) VALUES (?, ?) RETURNING id_customer",
            (order.customer_name, order.customer_phone)
        )
        customer_row = cursor.fetchone()
        if customer_row is None:
            raise Exception("Не удалось создать покупателя")
        customer_id = customer_row["id_customer"]

        # Создаём запись о продаже
        cursor = conn.execute(
            "INSERT INTO Sale (id_customer, date_sale, comment) VALUES (?, date('now'), ?) RETURNING id_sale",
            (customer_id, order.comment)
        )
        sale_row = cursor.fetchone()
        if sale_row is None:
            raise Exception("Не удалось создать продажу")
        sale_id = sale_row["id_sale"]

        # Добавляем товары в заказ
        for item in order.items:
            conn.execute(
                "INSERT INTO Magazine_Sales (id_sale, id_product, quantity, price) VALUES (?, ?, ?, ?)",
                (sale_id, item.product_id, item.quantity, item.price)
            )

        conn.commit()
        return {"status": "ok", "sale_id": sale_id}
    
    except Exception as e:
        conn.rollback()
        print("Ошибка при создании заказа:", e)
        raise HTTPException(status_code=500, detail=str(e))
    
    finally:
        conn.close()
```

**Критически важные аспекты:**

1. **Транзакционность**: Использование BEGIN/COMMIT/ROLLBACK гарантирует атомарность операции. Если любая часть заказа не выполнится, вся транзакция будет откатана.

2. **RETURNING**: SQLite-специфичный синтаксис для получения ID созданной записи в том же запросе.

3. **Циклическая вставка товаров**: Каждый элемент заказа вставляется отдельным запросом.

4. **Rollback при ошибке**: В случае исключения выполняется откат изменений, чтобы не осталось частичных данных.

5. **Finally блок**: Гарантирует закрытие соединения даже при возникновении ошибки.

## 3.3. Реализация системы аналитики

Система аналитики представляет собой набор endpoints, каждый из которых решает конкретную задачу по анализу продаж.

### Аналитика популярных товаров

**Листинг 3.3.1. Endpoint /api/admin/analytics/popular-products**

```python
@app.get("/api/admin/analytics/popular-products", dependencies=[Depends(verify_admin_token)])
def popular_products(start_date: Optional[str] = None, end_date: Optional[str] = None):
    conn = get_db()
    query = """
        SELECT p.name, SUM(ms.quantity) as total_quantity
        FROM Magazine_Sales ms
        JOIN Products p ON ms.id_product = p.id_product
        JOIN Sale s ON ms.id_sale = s.id_sale
        WHERE 1=1
    """
    params = []
    if start_date and end_date:
        query += " AND s.date_sale BETWEEN ? AND ?"
        params = [start_date, end_date]
    query += " GROUP BY p.id_product ORDER BY total_quantity DESC LIMIT 10"
    data = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(row) for row in data]
```

**Характеристики запроса:**

- **JOIN трёх таблиц**: Magazine_Sales → Products → Sale для получения названий товаров и дат продаж.
  
- **Агрегация SUM()**: Подсчёт общего количества проданных единиц по каждому товару.
  
- **GROUP BY**: Группировка по id_product для агрегации.
  
- **ORDER BY ... DESC**: Сортировка от наиболее к наименее популярному.
  
- **LIMIT 10**: Возврат только топ-10 товаров.
  
- **Опциональный фильтр по датам**: Позволяет получить статистику за конкретный период.

### Аналитика продаж по группам ингредиентов

**Листинг 3.3.2. Endpoint /api/admin/analytics/sales-by-ingredient-group**

```python
@app.get("/api/admin/analytics/sales-by-ingredient-group", dependencies=[Depends(verify_admin_token)])
def sales_by_ingredient_group(year: Optional[int] = None, month: Optional[int] = None):
    conn = get_db()
    
    if year is None or month is None:
        max_date = conn.execute("SELECT MAX(date_sale) FROM Sale").fetchone()[0]
        if max_date:
            year = int(max_date[:4])
            month = int(max_date[5:7])
        else:
            conn.close()
            return []
    
    date_condition = f"strftime('%Y', s.date_sale) = '{year}' AND strftime('%m', s.date_sale) = '{month:02d}'"
    query = f"""
        SELECT i.ingredient_group,
               SUM(ms.quantity * ms.price) as total
        FROM Magazine_Sales ms
        JOIN Products p ON ms.id_product = p.id_product
        JOIN Product_Ingredients pi ON p.id_product = pi.id_product
        JOIN Ingredients i ON pi.id_ingredients = i.id_ingredients
        JOIN Sale s ON ms.id_sale = s.id_sale
        WHERE {date_condition}
        GROUP BY i.ingredient_group
        ORDER BY total DESC
    """
    data = conn.execute(query).fetchall()
    conn.close()
    return [dict(row) for row in data if row['ingredient_group'] is not None]
```

**Особенности реализации:**

1. **Автоопределение периода**: Если год и месяц не указаны, система автоматически определяет последний месяц с продажами.

2. **SQLite функция strftime()**: Используется для извлечения года и месяца из текстовой даты.

3. **Цепочка JOIN**: Magazine_Sales → Products → Product_Ingredients → Ingredients → Sale для связи продаж с группами ингредиентов.

4. **Расчёт выручки**: `SUM(ms.quantity * ms.price)` считает общую сумму продаж, а не просто количество.

5. **Фильтрация NULL**: Исключаются записи с неопределённой группой ингредиентов.

### Аналитика продаж по праздникам

**Листинг 3.3.3. Endpoint /api/admin/analytics/sales-by-occasion**

```python
@app.get("/api/admin/analytics/sales-by-occasion", dependencies=[Depends(verify_admin_token)])
def sales_by_occasion(year: Optional[int] = None, month: Optional[int] = None):
    conn = get_db()
    # ... определение year/month аналогично предыдущему ...
    
    date_condition = f"strftime('%Y', s.date_sale) = '{year}' AND strftime('%m', s.date_sale) = '{month:02d}'"
    query = f"""
        SELECT o.occasion_name,
               SUM(ms.quantity * ms.price) as total
        FROM Magazine_Sales ms
        JOIN Products p ON ms.id_product = p.id_product
        JOIN Product_Occasion po ON p.id_product = po.id_product
        JOIN Occasions o ON po.id_occasion = o.id_occasion
        JOIN Sale s ON ms.id_sale = s.id_sale
        WHERE {date_condition}
        GROUP BY o.occasion_name
        ORDER BY total DESC
    """
    data = conn.execute(query).fetchall()
    conn.close()
    return [dict(row) for row in data]
```

**Бизнес-ценность:**

Этот отчёт позволяет магазину:
- Выявить наиболее прибыльные праздники;
- Планировать производство к сезону;
- Оптимизировать маркетинговый бюджет.

### Аналитика по диетическим характеристикам

**Листинг 3.3.4. Endpoint /api/admin/analytics/sales-by-dietary**

```python
@app.get("/api/admin/analytics/sales-by-dietary", dependencies=[Depends(verify_admin_token)])
def sales_by_dietary(dietary: str):
    field_map = {
        'no_nuts': 'has_nuts',
        'no_gluten': 'has_gluten',
        'no_dairy': 'has_dairy',
        'vegan': 'is_vegan'
    }
    field = field_map.get(dietary)
    if not field:
        raise HTTPException(status_code=400, detail="Неверный параметр dietary")
    
    if dietary == 'vegan':
        condition = f"{field} = 1"
    else:
        condition = f"{field} = 0"
    
    query = f"""
        SELECT strftime('%Y-%m', s.date_sale) as month,
               SUM(ms.quantity * ms.price) as total
        FROM Sale s
        JOIN Magazine_Sales ms ON s.id_sale = ms.id_sale
        JOIN Products p ON ms.id_product = p.id_product
        WHERE {condition}
        GROUP BY month
        ORDER BY month
    """
    data = conn.execute(query).fetchall()
    conn.close()
    return [dict(row) for row in data]
```

**Уникальность решения:**

- **Маппинг параметров**: Преобразование человеко-читаемых имён (no_nuts) в имена полей БД (has_nuts).
  
- **Разная логика условий**: Для vegan ищем `= 1`, для "без" параметров ищем `= 0`.
  
- **Временная динамика**: Группировка по месяцам показывает тренды спроса на диетическую продукцию.

### Статистика использования фильтров

**Листинг 3.3.5. Endpoint /api/admin/analytics/filter-stats**

```python
@app.get("/api/admin/analytics/filter-stats", dependencies=[Depends(verify_admin_token)])
def filter_stats():
    conn = get_db()
    
    # Общее количество событий фильтрации
    total_events = conn.execute("SELECT COUNT(*) FROM FilterEvents").fetchone()[0]
    
    # Количество уникальных сессий
    unique_sessions = conn.execute(
        "SELECT COUNT(DISTINCT session_id) FROM FilterEvents"
    ).fetchone()[0]
    
    # Популярные фильтры
    filter_usage = conn.execute("""
        SELECT filters, COUNT(*) as count 
        FROM FilterEvents 
        GROUP BY filters 
        ORDER BY count DESC 
        LIMIT 10
    """).fetchall()
    
    # Среднее количество результатов
    avg_results = conn.execute(
        "SELECT AVG(results_count) FROM FilterEvents WHERE results_count IS NOT NULL"
    ).fetchone()[0]
    
    conn.close()
    
    return {
        "total_events": total_events,
        "unique_sessions": unique_sessions,
        "popular_filters": [dict(row) for row in filter_usage],
        "average_results": avg_results
    }
```

**Аналитическая ценность:**

- **total_events**: Общая активность пользователей по фильтрации.
  
- **unique_sessions**: Количество уникальных посетителей, использующих фильтры.
  
- **popular_filters**: Какие комбинации фильтров наиболее востребованы.
  
- **average_results**: Среднее количество товаров в выдаче — помогает оценить, не слишком ли узкие/широкие фильтры.

## 3.4. Авторизация администратора

### Простая токеновая аутентификация

Для защиты административной панели реализована упрощённая система аутентификации.

**Листинг 3.4.1. Login endpoint**

```python
@app.post("/api/admin/login")
def admin_login(credentials: AdminLogin):
    if credentials.username == "admin" and credentials.password == "admin123":
        token = "admin-simple-token-123"
        return {"token": token}
    else:
        raise HTTPException(status_code=401, detail="Invalid credentials")
```

**Листинг 3.4.2. Функция верификации токена**

```python
def verify_admin_token(x_admin_token: str = Header(...)):
    if x_admin_token != "admin-simple-token-123":
        raise HTTPException(status_code=401, detail="Unauthorized")
    return True
```

**Листинг 3.4.3. Применение зависимости**

```python
@app.get("/api/admin/orders", dependencies=[Depends(verify_admin_token)])
def get_orders():
    # ... код доступен только авторизованным ...
```

**Обоснование выбора:**

1. **Простота**: Для учебного проекта и демонстрации достаточно простой схемы.
  
2. **Stateless**: Токен передаётся в заголовке, сервер не хранит сессию.
  
3. **Зависимости FastAPI**: Механизм Depends позволяет элегантно добавить проверку ко всем защищённым endpoints.

**Ограничения:**

- Токен не истекает;
- Нет шифрования пароля;
- Хардкод учётных данных.

*Для промышленного использования следует внедрить JWT с expiration, хеширование паролей (bcrypt) и хранение пользователей в БД.*

## 3.5. Конфигурация развёртывания

### Файл amvera.yml

**Листинг 3.5.1. Конфигурация деплоя**

```yaml
meta:
  environment: python
  toolchain:
    name: pip
    version: "3.10"
build:
  requirementsPath: requirements.txt
run:
  scriptName: main.py
  persistenceMount: /data
  containerPort: 8000
```

**Описание параметров:**

- **environment: python**: Указывает на использование Python-окружения.
  
- **requirementsPath**: Путь к файлу с зависимостями для установки.
  
- **scriptName**: Точка входа приложения.
  
- **persistenceMount**: Директория для постоянного хранения данных (база данных).
  
- **containerPort**: Порт, на котором приложение слушает запросы.

### Запуск локально

Для локального тестирования используется команда:

```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

**Параметры:**

- **--reload**: Автоматическая перезагрузка при изменении кода (для разработки).
  
- **--host 0.0.0.0**: Доступ с любых сетевых интерфейсов.
  
- **--port 8000**: Порт для прослушивания.

---
