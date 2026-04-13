from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import sqlite3
from pydantic import BaseModel
import json
from fastapi import HTTPException
from typing import Optional
from fastapi import Query
from pydantic import BaseModel
from fastapi import Header, HTTPException, Depends
from typing import List

class AdminLogin(BaseModel):
    username: str
    password: str

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class FilterEvent(BaseModel):
    session_id: str
    filters: dict
    results_count: int | None = None
def get_db():
    conn = sqlite3.connect('BD_CANDY_SHOP.bd')
    conn.row_factory = sqlite3.Row 
    return conn

class OrderItem(BaseModel):
    product_id: int
    quantity: int
    price: float

class OrderRequest(BaseModel):
    customer_name: str
    customer_phone: str
    comment: Optional[str] = None
    items: List[OrderItem]

from fastapi import Response

@app.options("/{path:path}")
async def options_handler():
    return Response(status_code=200)

@app.get("/")
def root():
    return {"message": "Candy Shop API"}

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
    # Диагностика
    print("=== DEBUG ===")
    print("occasions:", occasions)
    print("ingredient_groups:", ingredient_groups)
    print("no_nuts:", no_nuts)
    print("no_gluten:", no_gluten)
    print("no_dairy:", no_dairy)
    print("vegan:", vegan)
    print("sort:", sort)

    conn = get_db()
    query = "SELECT * FROM Products WHERE 1=1"
    params = []

    if occasions:
        occasion_ids = [int(id.strip()) for id in occasions.split(',')]
        placeholders = ','.join(['?'] * len(occasion_ids))
        query += f" AND id_product IN (SELECT id_product FROM Product_Occasion WHERE id_occasion IN ({placeholders}))"
        params.extend(occasion_ids)
        print("Добавлено условие по праздникам, SQL:", query, "params:", params)

    if ingredient_groups:
        groups = ingredient_groups.split(',')
        for group in groups:
            query += f" AND id_product IN (SELECT pi.id_product FROM Product_Ingredients pi JOIN Ingredients i ON pi.id_ingredients = i.id_ingredients WHERE i.ingredient_group = ?)"
            params.append(group)
        print("Добавлено условие по группам ингредиентов, SQL:", query, "params:", params)

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

    print("Итоговый SQL:", query)
    print("Параметры:", params)

    cursor = conn.execute(query, params)
    products = cursor.fetchall()
    conn.close()
    return [dict(row) for row in products]
@app.get("/api/filters")
def get_filters():
    conn = get_db()
    occasions = conn.execute("SELECT id_occasion as id, occasion_name as name FROM Occasions").fetchall()
    groups = conn.execute("SELECT DISTINCT ingredient_group FROM Ingredients WHERE ingredient_group IS NOT NULL").fetchall()
    conn.close()

    # Список групп, которые НЕ нужно показывать
    exclude = ['молочные','соль', 'сахар', 'разрыхлитель', 'яйца', 'ароматизаторы', 'мука', 'специи', 'масла']
    ingredient_groups = [g["ingredient_group"] for g in groups if g["ingredient_group"] and g["ingredient_group"] not in exclude]

    return {
        "occasions": [dict(o) for o in occasions],
        "ingredientGroups": ingredient_groups,
        "dietaryFilters": [
            {"id": "no_nuts", "name": "Без орехов"},
            {"id": "no_gluten", "name": "Без глютена"},
            {"id": "no_dairy", "name": "Без молочных"},
            {"id": "vegan", "name": "Веган"}        ]
    }
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
        # Чтобы увидеть подробности в терминале
        import traceback
        traceback.print_exc()
        # Возвращаем ошибку клиенту, но с заголовками CORS (FastAPI сделает это автоматически, если исключение поднято)
        from fastapi import HTTPException
        raise HTTPException(status_code=500, detail=str(e))
@app.post("/api/admin/login")
def admin_login(credentials: AdminLogin):
    # Здесь можно проверять по базе, но для простоты захардкодим
    if credentials.username == "admin" and credentials.password == "admin123":
        # Генерируем простой токен (можно использовать JWT, но для диплома подойдёт)
        token = "admin-simple-token-123"
        return {"token": token}
    else:
        raise HTTPException(status_code=401, detail="Invalid credentials")

def verify_admin_token(x_admin_token: str = Header(...)):
    if x_admin_token != "admin-simple-token-123":
        raise HTTPException(status_code=401, detail="Unauthorized")
    return True

@app.get("/api/admin/orders", dependencies=[Depends(verify_admin_token)])
def get_orders():
    conn = get_db()
    orders = conn.execute("""
        SELECT s.id_sale, c.name, c.phone, s.date_sale, 
               COALESCE(SUM(ms.quantity * ms.price), 0) as total
        FROM Sale s
        JOIN Customers c ON s.id_customer = c.id_customer
        LEFT JOIN Magazine_Sales ms ON s.id_sale = ms.id_sale
        GROUP BY s.id_sale
        ORDER BY s.date_sale DESC
    """).fetchall()
    conn.close()
    return [dict(o) for o in orders]

@app.get("/api/admin/orders/{sale_id}", dependencies=[Depends(verify_admin_token)])
def get_order_details(sale_id: int):
    conn = get_db()
    # информация о заказе
    order = conn.execute("""
        SELECT s.id_sale, c.name, c.phone, s.date_sale, s.comment
        FROM Sale s
        JOIN Customers c ON s.id_customer = c.id_customer
        WHERE s.id_sale = ?
    """, (sale_id,)).fetchone()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    # товары в заказе
    items = conn.execute("""
        SELECT p.name, ms.quantity, ms.price
        FROM Magazine_Sales ms
        JOIN Products p ON ms.id_product = p.id_product
        WHERE ms.id_sale = ?
    """, (sale_id,)).fetchall()
    conn.close()
    return {
        "order": dict(order),
        "items": [dict(i) for i in items]
    }




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

@app.get("/api/admin/analytics/sales-by-ingredient-group", dependencies=[Depends(verify_admin_token)])
def sales_by_ingredient_group(year: Optional[int] = None, month: Optional[int] = None):
    """
    Возвращает сумму продаж для каждой группы ингредиентов за указанный месяц.
    Если year и month не указаны, берётся последний месяц (максимальная дата в таблице Sale).
    """
    conn = get_db()
    # Если год и месяц не заданы, определяем максимальный месяц из таблицы Sale
    if year is None or month is None:
        max_date = conn.execute("SELECT MAX(date_sale) FROM Sale").fetchone()[0]
        if max_date:
            year = int(max_date[:4])
            month = int(max_date[5:7])
        else:
            # Если продаж нет, возвращаем пустой список
            conn.close()
            return []
    
    # Строим условие для фильтрации по году и месяцу
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

@app.get("/api/admin/analytics/sales-by-occasion", dependencies=[Depends(verify_admin_token)])
def sales_by_occasion(year: Optional[int] = None, month: Optional[int] = None):
    """
    Возвращает сумму продаж для каждого праздника за указанный месяц.
    """
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

@app.get("/api/admin/analytics/sales-by-dietary", dependencies=[Depends(verify_admin_token)])
def sales_by_dietary(dietary: str):
    """
    Возвращает продажи по месяцам для товаров с заданным диетическим свойством.
    dietary может быть 'no_nuts', 'no_gluten', 'no_dairy', 'vegan'
    """
    # Сопоставляем строку с реальным полем таблицы Products
    field_map = {
        'no_nuts': 'has_nuts',
        'no_gluten': 'has_gluten',
        'no_dairy': 'has_dairy',
        'vegan': 'is_vegan'
    }
    field = field_map.get(dietary)
    if not field:
        raise HTTPException(status_code=400, detail="Неверный параметр dietary")
    
    conn = get_db()
    # Для флага "без орехов" нам нужно has_nuts = 0, для "без глютена" has_gluten = 0 и т.д.
    # Но так как поле булево, то значение 0 – отсутствие, 1 – наличие.
    # Для "без" мы ищем товары с 0, для "vegan" – is_vegan = 1.
    # Обработаем:
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

@app.get("/api/admin/analytics/sales-by-occasion-pie", dependencies=[Depends(verify_admin_token)])
def sales_by_occasion_pie(year: Optional[int] = None, month: Optional[int] = None):
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
@app.get("/api/admin/analytics/sales-by-ingredient-group-last-month", dependencies=[Depends(verify_admin_token)])
def sales_by_ingredient_group_last_month():
    """
    Возвращает сумму продаж для каждой группы ингредиентов за последний месяц.
    """
    conn = get_db()
    data = conn.execute("""
        SELECT i.ingredient_group,
               SUM(ms.quantity * ms.price) as total
        FROM Magazine_Sales ms
        JOIN Products p ON ms.id_product = p.id_product
        JOIN Product_Ingredients pi ON p.id_product = pi.id_product
        JOIN Ingredients i ON pi.id_ingredients = i.id_ingredients
        JOIN Sale s ON ms.id_sale = s.id_sale
        WHERE s.date_sale >= date('now', '-1 month')
        GROUP BY i.ingredient_group
        ORDER BY total DESC
    """).fetchall()
    conn.close()
    return [dict(row) for row in data if row['ingredient_group'] is not None]

@app.get("/api/admin/analytics/sales-by-occasion-last-month", dependencies=[Depends(verify_admin_token)])
def sales_by_occasion_last_month():
    """
    Возвращает сумму продаж для каждого праздника за последний месяц.
    """
    conn = get_db()
    data = conn.execute("""
        SELECT o.occasion_name,
               SUM(ms.quantity * ms.price) as total
        FROM Magazine_Sales ms
        JOIN Products p ON ms.id_product = p.id_product
        JOIN Product_Occasion po ON p.id_product = po.id_product
        JOIN Occasions o ON po.id_occasion = o.id_occasion
        JOIN Sale s ON ms.id_sale = s.id_sale
        WHERE s.date_sale >= date('now', '-1 month')
        GROUP BY o.occasion_name
        ORDER BY total DESC
    """).fetchall()
    conn.close()
    return [dict(row) for row in data]

@app.post("/api/orders")
def create_order(order: OrderRequest):
    print("=== НОВЫЙ ЗАКАЗ ===")
    print(order.dict())
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
        print("customer_id:", customer_id)

        # Создаём запись о продаже с комментарием
        cursor = conn.execute(
            "INSERT INTO Sale (id_customer, date_sale, comment) VALUES (?, date('now'), ?) RETURNING id_sale",
            (customer_id, order.comment)
        )
        sale_row = cursor.fetchone()
        if sale_row is None:
            raise Exception("Не удалось создать запись о продаже")
        sale_id = sale_row["id_sale"]
        print("sale_id:", sale_id)

        # Добавляем товары
        for item in order.items:
            conn.execute(
                "INSERT INTO Magazine_Sales (id_sale, id_product, quantity, price) VALUES (?, ?, ?, ?)",
                (sale_id, item.product_id, item.quantity, item.price)
            )
            print(f"Добавлен товар {item.product_id} x {item.quantity}")

        conn.commit()
        return {"id_sale": sale_id, "status": "created"}
    except Exception as e:
        conn.rollback()
        print("!!! ОШИБКА ПРИ СОХРАНЕНИИ ЗАКАЗА:")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        conn.close()


# ========== НОВАЯ АНАЛИТИКА ==========

@app.get("/api/admin/analytics/sales-by-month", dependencies=[Depends(verify_admin_token)])
def sales_by_month(start_date: Optional[str] = None, end_date: Optional[str] = None):
    conn = get_db()
    query = """
        SELECT strftime('%Y-%m', date_sale) as month,
               SUM(ms.quantity * ms.price) as total
        FROM Sale s
        JOIN Magazine_Sales ms ON s.id_sale = ms.id_sale
    """
    params = []
    if start_date and end_date:
        query += " WHERE date_sale BETWEEN ? AND ?"
        params = [start_date, end_date]
    query += " GROUP BY month ORDER BY month"
    data = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(row) for row in data]

@app.get("/api/admin/analytics/sales-by-ingredient-group-over-time", dependencies=[Depends(verify_admin_token)])
def sales_by_ingredient_group_over_time(group: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None):
    conn = get_db()
    date_filter = ""
    params = []
    if start_date and end_date:
        date_filter = " AND s.date_sale BETWEEN ? AND ?"
        params = [start_date, end_date]
    if group:
        data = conn.execute(f"""
            SELECT strftime('%Y-%m', s.date_sale) as month,
                   SUM(ms.quantity) as total
            FROM Sale s
            JOIN Magazine_Sales ms ON s.id_sale = ms.id_sale
            JOIN Products p ON ms.id_product = p.id_product
            JOIN Product_Ingredients pi ON p.id_product = pi.id_product
            JOIN Ingredients i ON pi.id_ingredients = i.id_ingredients
            WHERE i.ingredient_group = ? {date_filter}
            GROUP BY month
            ORDER BY month
        """, (group,) + tuple(params)).fetchall()
        conn.close()
        return [dict(row) for row in data]
    else:
        groups = conn.execute("SELECT DISTINCT ingredient_group FROM Ingredients WHERE ingredient_group IS NOT NULL").fetchall()
        result = {}
        for g in groups:
            gr = g[0]
            data = conn.execute(f"""
                SELECT strftime('%Y-%m', s.date_sale) as month,
                       SUM(ms.quantity) as total
                FROM Sale s
                JOIN Magazine_Sales ms ON s.id_sale = ms.id_sale
                JOIN Products p ON ms.id_product = p.id_product
                JOIN Product_Ingredients pi ON p.id_product = pi.id_product
                JOIN Ingredients i ON pi.id_ingredients = i.id_ingredients
                WHERE i.ingredient_group = ? {date_filter}
                GROUP BY month
                ORDER BY month
            """, (gr,) + tuple(params)).fetchall()
            result[gr] = [dict(row) for row in data]
        conn.close()
        return result

@app.get("/api/admin/analytics/sales-by-occasion-over-time", dependencies=[Depends(verify_admin_token)])
def sales_by_occasion_over_time(occasion_id: Optional[int] = None, start_date: Optional[str] = None, end_date: Optional[str] = None):
    conn = get_db()
    date_filter = ""
    params = []
    if start_date and end_date:
        date_filter = " AND s.date_sale BETWEEN ? AND ?"
        params = [start_date, end_date]
    if occasion_id:
        data = conn.execute(f"""
            SELECT strftime('%Y-%m', s.date_sale) as month,
                   SUM(ms.quantity) as total
            FROM Sale s
            JOIN Magazine_Sales ms ON s.id_sale = ms.id_sale
            JOIN Products p ON ms.id_product = p.id_product
            JOIN Product_Occasion po ON p.id_product = po.id_product
            WHERE po.id_occasion = ? {date_filter}
            GROUP BY month
            ORDER BY month
        """, (occasion_id,) + tuple(params)).fetchall()
        conn.close()
        return [dict(row) for row in data]
    else:
        occasions = conn.execute("SELECT id_occasion, occasion_name FROM Occasions").fetchall()
        result = {}
        for occ in occasions:
            occ_id, occ_name = occ
            data = conn.execute(f"""
                SELECT strftime('%Y-%m', s.date_sale) as month,
                       SUM(ms.quantity) as total
                FROM Sale s
                JOIN Magazine_Sales ms ON s.id_sale = ms.id_sale
                JOIN Products p ON ms.id_product = p.id_product
                JOIN Product_Occasion po ON p.id_product = po.id_product
                WHERE po.id_occasion = ? {date_filter}
                GROUP BY month
                ORDER BY month
            """, (occ_id,) + tuple(params)).fetchall()
            result[occ_name] = [dict(row) for row in data]
        conn.close()
        return result
    

@app.get("/api/admin/analytics/average-check-by-month", dependencies=[Depends(verify_admin_token)])
def average_check_by_month(start_date: Optional[str] = None, end_date: Optional[str] = None):
    conn = get_db()
    query = """
        SELECT strftime('%Y-%m', s.date_sale) as month,
               AVG(ms_total.total) as avg_check
        FROM Sale s
        JOIN (
            SELECT id_sale, SUM(quantity * price) as total
            FROM Magazine_Sales
            GROUP BY id_sale
        ) ms_total ON s.id_sale = ms_total.id_sale
    """
    params = []
    if start_date and end_date:
        query += " WHERE s.date_sale BETWEEN ? AND ?"
        params = [start_date, end_date]
    query += " GROUP BY month ORDER BY month"
    data = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(row) for row in data]

@app.get("/api/admin/analytics/unpopular-products", dependencies=[Depends(verify_admin_token)])
def unpopular_products(limit: int = 5):
    """
    Возвращает товары с наименьшим количеством продаж (например, за всё время).
    """
    conn = get_db()
    data = conn.execute("""
        SELECT p.id_product, p.name, COALESCE(SUM(ms.quantity), 0) as total_sold
        FROM Products p
        LEFT JOIN Magazine_Sales ms ON p.id_product = ms.id_product
        GROUP BY p.id_product
        ORDER BY total_sold ASC
        LIMIT ?
    """, (limit,)).fetchall()
    conn.close()
    return [dict(row) for row in data]

@app.get("/api/admin/analytics/popular-ingredient-month", dependencies=[Depends(verify_admin_token)])
def popular_ingredient_last_month():
    """
    Возвращает самую популярную группу ингредиентов за последний месяц.
    """
    conn = get_db()
    data = conn.execute("""
        SELECT i.ingredient_group, SUM(ms.quantity * ms.price) as total
        FROM Magazine_Sales ms
        JOIN Products p ON ms.id_product = p.id_product
        JOIN Product_Ingredients pi ON p.id_product = pi.id_product
        JOIN Ingredients i ON pi.id_ingredients = i.id_ingredients
        JOIN Sale s ON ms.id_sale = s.id_sale
        WHERE s.date_sale >= date('now', '-1 month')
        GROUP BY i.ingredient_group
        ORDER BY total DESC
        LIMIT 1
    """).fetchone()
    conn.close()
    return {"ingredient_group": data[0] if data else None, "total": data[1] if data else 0}

@app.get("/api/admin/analytics/popular-products-by-month", dependencies=[Depends(verify_admin_token)])
def popular_products_by_month(year: Optional[int] = None, month: Optional[int] = None):
    conn = get_db()
    if year is None or month is None:
        # если не указаны, берем последний месяц с продажами
        max_date = conn.execute("SELECT MAX(date_sale) FROM Sale").fetchone()[0]
        if max_date:
            year = int(max_date[:4])
            month = int(max_date[5:7])
        else:
            conn.close()
            return []
    date_condition = f"strftime('%Y', s.date_sale) = '{year}' AND strftime('%m', s.date_sale) = '{month:02d}'"
    query = f"""
        SELECT p.name, SUM(ms.quantity) as total_quantity
        FROM Magazine_Sales ms
        JOIN Products p ON ms.id_product = p.id_product
        JOIN Sale s ON ms.id_sale = s.id_sale
        WHERE {date_condition}
        GROUP BY p.id_product
        ORDER BY total_quantity DESC
        LIMIT 10
    """
    data = conn.execute(query).fetchall()
    conn.close()
    return [dict(row) for row in data]

@app.get("/api/admin/analytics/filter-stats", dependencies=[Depends(verify_admin_token)])
def filter_stats():
    conn = get_db()
    rows = conn.execute("""
        SELECT filters, created_at
        FROM FilterEvents
        WHERE created_at >= date('now', '-30 days')
    """).fetchall()
    occasions = {row[0]: row[1] for row in conn.execute("SELECT id_occasion, occasion_name FROM Occasions").fetchall()}
    stats = {}
    for row in rows:
        try:
            filters = json.loads(row['filters'])
        except:
            continue
        parts = []
        if 'occasions' in filters and filters['occasions']:
            occ_names = [occasions.get(occ_id, str(occ_id)) for occ_id in filters['occasions']]
            parts.append(f"Праздник: {', '.join(occ_names)}")
        if 'ingredientGroups' in filters and filters['ingredientGroups']:
            parts.append(f"Ингредиенты: {', '.join(filters['ingredientGroups'])}")
        if 'dietary' in filters and filters['dietary']:
            dietary_names = {
                'no_nuts': 'Без орехов',
                'no_gluten': 'Без глютена',
                'no_dairy': 'Без молочных',
                'vegan': 'Веган',
                'no_aroma': 'Без ароматизаторов'
            }
            for key, val in filters['dietary'].items():
                if val:
                    parts.append(dietary_names.get(key, key))
        if 'sort' in filters and filters['sort']:
            sort_name = 'По возрастанию цены' if filters['sort'] == 'price_asc' else 'По убыванию цены'
            parts.append(sort_name)
        if parts:
            filter_name = ', '.join(parts)
            stats[filter_name] = stats.get(filter_name, 0) + 1
    # Убираем "Без фильтров", если есть (у нас не должно быть)
    stats.pop("Без фильтров", None)
    # Сортируем по убыванию
    sorted_items = sorted(stats.items(), key=lambda x: x[1], reverse=True)
    top10 = [{"filter": k, "count": v} for k, v in sorted_items[:10]]
    # Наименее популярные (не менее 1 применения) – последние 10
    bottom10 = [{"filter": k, "count": v} for k, v in sorted_items[-10:]] if len(sorted_items) > 10 else []
    conn.close()
    return {"top": top10, "bottom": bottom10}

@app.get("/api/admin/analytics/sales-by-ingredient-group-range", dependencies=[Depends(verify_admin_token)])
def sales_by_ingredient_group_range(group: str, start_date: Optional[str] = None, end_date: Optional[str] = None):
    conn = get_db()
    query = """
        SELECT strftime('%Y-%m', s.date_sale) as month,
               SUM(ms.quantity * ms.price) as total
        FROM Sale s
        JOIN Magazine_Sales ms ON s.id_sale = ms.id_sale
        JOIN Products p ON ms.id_product = p.id_product
        JOIN Product_Ingredients pi ON p.id_product = pi.id_product
        JOIN Ingredients i ON pi.id_ingredients = i.id_ingredients
        WHERE i.ingredient_group = ?
    """
    params = [group]
    if start_date and end_date:
        query += " AND s.date_sale BETWEEN ? AND ?"
        params.extend([start_date, end_date])
    query += " GROUP BY month ORDER BY month"
    data = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(row) for row in data]

@app.get("/api/admin/analytics/sales-by-occasion-range", dependencies=[Depends(verify_admin_token)])
def sales_by_occasion_range(occasion_id: int, start_date: Optional[str] = None, end_date: Optional[str] = None):
    conn = get_db()
    query = """
        SELECT strftime('%Y-%m', s.date_sale) as month,
               SUM(ms.quantity * ms.price) as total
        FROM Sale s
        JOIN Magazine_Sales ms ON s.id_sale = ms.id_sale
        JOIN Products p ON ms.id_product = p.id_product
        JOIN Product_Occasion po ON p.id_product = po.id_product
        WHERE po.id_occasion = ?
    """
    params = [occasion_id]
    if start_date and end_date:
        query += " AND s.date_sale BETWEEN ? AND ?"
        params.extend([start_date, end_date])
    query += " GROUP BY month ORDER BY month"
    data = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(row) for row in data]

@app.get("/api/admin/analytics/unpopular-products-range", dependencies=[Depends(verify_admin_token)])
def unpopular_products_range(limit: int = 5, start_date: Optional[str] = None, end_date: Optional[str] = None):
    conn = get_db()
    query = """
        SELECT p.id_product, p.name, COALESCE(SUM(ms.quantity), 0) as total_sold
        FROM Products p
        LEFT JOIN Magazine_Sales ms ON p.id_product = ms.id_product
        LEFT JOIN Sale s ON ms.id_sale = s.id_sale
        WHERE 1=1
    """
    params = []
    if start_date and end_date:
        query += " AND s.date_sale BETWEEN ? AND ?"
        params = [start_date, end_date]
    query += " GROUP BY p.id_product ORDER BY total_sold ASC LIMIT ?"
    params.append(limit)
    data = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(row) for row in data]

@app.get("/api/admin/analytics/filter-stats-range", dependencies=[Depends(verify_admin_token)])
def filter_stats_range(start_date: Optional[str] = None, end_date: Optional[str] = None):
    conn = get_db()
    date_filter = ""
    params = []
    if start_date and end_date:
        date_filter = " WHERE created_at BETWEEN ? AND ?"
        params = [start_date, end_date]
    rows = conn.execute(f"""
        SELECT filters, created_at
        FROM FilterEvents
        {date_filter}
    """, params).fetchall()
    occasions = {row[0]: row[1] for row in conn.execute("SELECT id_occasion, occasion_name FROM Occasions").fetchall()}
    stats = {}
    for row in rows:
        try:
            filters = json.loads(row['filters'])
        except:
            continue
        parts = []
        if 'occasions' in filters and filters['occasions']:
            occ_names = [occasions.get(occ_id, str(occ_id)) for occ_id in filters['occasions']]
            parts.append(f"Праздник: {', '.join(occ_names)}")
        if 'ingredientGroups' in filters and filters['ingredientGroups']:
            parts.append(f"Ингредиенты: {', '.join(filters['ingredientGroups'])}")
        if 'dietary' in filters and filters['dietary']:
            dietary_names = {
                'no_nuts': 'Без орехов',
                'no_gluten': 'Без глютена',
                'no_dairy': 'Без молочных',
                'vegan': 'Веган',
                'no_aroma': 'Без ароматизаторов'
            }
            for key, val in filters['dietary'].items():
                if val:
                    parts.append(dietary_names.get(key, key))
        if 'sort' in filters and filters['sort']:
            sort_name = 'По возрастанию цены' if filters['sort'] == 'price_asc' else 'По убыванию цены'
            parts.append(sort_name)
        if parts:
            filter_name = ', '.join(parts)
            stats[filter_name] = stats.get(filter_name, 0) + 1
    stats.pop("Без фильтров", None)
    sorted_items = sorted(stats.items(), key=lambda x: x[1], reverse=True)
    top10 = [{"filter": k, "count": v} for k, v in sorted_items[:10]]
    bottom10 = [{"filter": k, "count": v} for k, v in sorted_items[-10:]] if len(sorted_items) > 10 else []
    conn.close()
    return {"top": top10, "bottom": bottom10}