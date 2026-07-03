import sqlite3
import random
from datetime import datetime, timedelta
import calendar

DB_PATH = 'BD_CANDY_SHOP.bd'

# Список русских имён и фамилий
FIRST_NAMES = ['Александр', 'Дмитрий', 'Максим', 'Сергей', 'Андрей', 'Алексей', 'Иван', 'Евгений', 'Михаил', 'Роман',
               'Ольга', 'Екатерина', 'Наталья', 'Ирина', 'Татьяна', 'Елена', 'Светлана', 'Анна', 'Мария', 'Виктория']
LAST_NAMES = ['Иванов', 'Петров', 'Сидоров', 'Кузнецов', 'Смирнов', 'Попов', 'Васильев', 'Николаев', 'Морозов', 'Новиков',
              'Козлова', 'Лебедева', 'Соколова', 'Орлова', 'Волкова', 'Зайцева', 'Павлова', 'Голубева', 'Виноградова', 'Белова']

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def get_products_with_details():
    """Получает список товаров с их ингредиентами и праздниками."""
    conn = get_db()
    products = conn.execute("""
        SELECT p.id_product, p.name, p.price,
               GROUP_CONCAT(DISTINCT i.ingredient_group) AS ingredient_groups,
               GROUP_CONCAT(DISTINCT o.occasion_name) AS occasion_names
        FROM Products p
        LEFT JOIN Product_Ingredients pi ON p.id_product = pi.id_product
        LEFT JOIN Ingredients i ON pi.id_ingredients = i.id_ingredients
        LEFT JOIN Product_Occasion po ON p.id_product = po.id_product
        LEFT JOIN Occasions o ON po.id_occasion = o.id_occasion
        GROUP BY p.id_product
    """).fetchall()
    conn.close()

    product_list = []
    for row in products:
        groups = row['ingredient_groups'].split(',') if row['ingredient_groups'] else []
        occasions = row['occasion_names'].split(',') if row['occasion_names'] else []
        groups = [g.strip() for g in groups if g.strip()]
        occasions = [o.strip() for o in occasions if o.strip()]
        product_list.append({
            'id': row['id_product'],
            'name': row['name'],
            'price': row['price'],
            'ingredient_groups': groups,
            'occasions': occasions
        })
    return product_list

def generate_phone():
    first = random.choice(['7', '8'])
    rest = ''.join([str(random.randint(0, 9)) for _ in range(10)])
    return first + rest

def generate_customer(conn):
    """Создаёт нового клиента или возвращает существующего."""
    name = random.choice(LAST_NAMES) + ' ' + random.choice(FIRST_NAMES)
    phone = generate_phone()
    cursor = conn.execute("SELECT id_customer FROM Customers WHERE phone = ?", (phone,))
    existing = cursor.fetchone()
    if existing:
        return existing['id_customer']
    cursor = conn.execute(
        "INSERT INTO Customers (name, phone) VALUES (?, ?) RETURNING id_customer",
        (name, phone)
    )
    return cursor.fetchone()['id_customer']

def generate_orders_for_month(year, month, target_revenue, product_list, trend_params):
    conn = get_db()
    cursor = conn.cursor()
    
    existing_customers = conn.execute("SELECT id_customer FROM Customers").fetchall()
    customer_ids = [c['id_customer'] for c in existing_customers] if existing_customers else []

    avg_check = random.randint(1450, 1550)
    num_orders = max(5, int(target_revenue / avg_check))

    total_revenue = 0
    orders_data = []

    for _ in range(num_orders):
        if customer_ids and random.random() < 0.7:
            customer_id = random.choice(customer_ids)
        else:
            customer_id = generate_customer(conn)
            customer_ids.append(customer_id)

        first_day = datetime(year, month, 1)
        last_day = datetime(year, month, calendar.monthrange(year, month)[1])
        date_sale = first_day + timedelta(days=random.randint(0, (last_day - first_day).days))
        date_str = date_sale.strftime('%Y-%m-%d')

        num_items = random.randint(1, 3)
        selected_products = []
        for _ in range(num_items):
            weights = []
            for prod in product_list:
                weight = 1.0
                if any('фрукт' in g or 'ягод' in g for g in prod['ingredient_groups']):
                    weight *= trend_params['fruit_weight']
                if any('орех' in g for g in prod['ingredient_groups']):
                    weight *= 1.2
                if any('какао' in g for g in prod['ingredient_groups']):
                    weight *= 1.1
                if 'Новый год' in prod['occasions']:
                    weight *= trend_params['new_year_weight']
                weight *= random.uniform(0.8, 1.2)
                weights.append(weight)

            total_weight = sum(weights)
            r = random.random() * total_weight
            cum = 0
            for idx, w in enumerate(weights):
                cum += w
                if r <= cum:
                    selected_products.append(product_list[idx])
                    break

        unique_products = []
        for p in selected_products:
            if p not in unique_products:
                unique_products.append(p)
        if not unique_products:
            unique_products = [random.choice(product_list)]

        cursor.execute(
            "INSERT INTO Sale (id_customer, date_sale, comment) VALUES (?, ?, ?)",
            (customer_id, date_str, f"Тестовый заказ за {date_str}")
        )
        sale_id = cursor.lastrowid

        order_total = 0
        for prod in unique_products:
            quantity = random.randint(1, 3)
            price = prod['price']
            cursor.execute(
                "INSERT INTO Magazine_Sales (id_sale, id_product, quantity, price) VALUES (?, ?, ?, ?)",
                (sale_id, prod['id'], quantity, price)
            )
            order_total += quantity * price
        total_revenue += order_total

        orders_data.append({
            'sale_id': sale_id,
            'date': date_str,
            'customer_id': customer_id,
            'total': order_total
        })

    conn.commit()
    conn.close()
    return len(orders_data), total_revenue

def seed():
    """
    Основная функция, которую будем вызывать из main.py.
    Возвращает словарь с результатом.
    """
    # Проверяем, есть ли уже заказы за апрель 2026
    conn = get_db()
    cursor = conn.execute("SELECT COUNT(*) FROM Sale WHERE date_sale LIKE '2026-04%'")
    existing_count = cursor.fetchone()[0]
    conn.close()
    if existing_count > 0:
        return {
            "status": "skipped",
            "message": "Данные за апрель-июнь 2026 уже есть в БД, повторно не добавляем.",
            "existing_orders": existing_count
        }

    # Загружаем товары
    products = get_products_with_details()
    if not products:
        return {"status": "error", "message": "Нет товаров в БД. Сначала добавьте товары."}

    months = [
        {'year': 2026, 'month': 4, 'target_revenue': 12000, 'fruit_weight': 1.0, 'new_year_weight': 1.5},
        {'year': 2026, 'month': 5, 'target_revenue': 14000, 'fruit_weight': 1.3, 'new_year_weight': 1.2},
        {'year': 2026, 'month': 6, 'target_revenue': 12000, 'fruit_weight': 1.8, 'new_year_weight': 0.7},
    ]

    results = []
    total_orders = 0
    total_revenue = 0
    for m in months:
        count, revenue = generate_orders_for_month(
            year=m['year'],
            month=m['month'],
            target_revenue=m['target_revenue'],
            product_list=products,
            trend_params={
                'fruit_weight': m['fruit_weight'],
                'new_year_weight': m['new_year_weight']
            }
        )
        results.append(f"{m['year']}-{m['month']:02d}: {count} заказов, выручка {revenue} руб.")
        total_orders += count
        total_revenue += revenue

    return {
        "status": "ok",
        "message": "Данные успешно добавлены.",
        "details": results,
        "total_orders": total_orders,
        "total_revenue": total_revenue
    }

if __name__ == '__main__':
    # Если запускаем напрямую, просто выполняем seed и выводим результат
    result = seed()
    print(result)