import sqlite3
import random
from datetime import datetime, timedelta
import calendar

conn = sqlite3.connect('BD_CANDY_SHOP.bd')
c = conn.cursor()

# Получаем список покупателей
customers = c.execute("SELECT id_customer, name, phone FROM Customers").fetchall()
if not customers:
    for i in range(1, 21):
        name = random.choice(['Иван', 'Петр', 'Сергей', 'Анна', 'Мария', 'Елена', 'Алексей', 'Дмитрий', 'Ольга', 'Татьяна'])
        last = random.choice(['Иванов', 'Петров', 'Сидоров', 'Кузнецов', 'Смирнов', 'Попов', 'Васильев', 'Федоров', 'Морозов', 'Волков'])
        phone = f"89{random.randint(100000000, 999999999)}"
        c.execute("INSERT INTO Customers (id_customer, name, phone) VALUES (?, ?, ?)", (i, f"{name} {last}", phone))
    customers = c.execute("SELECT id_customer, name, phone FROM Customers").fetchall()

# Получаем товары с группами и праздниками
products = c.execute("""
    SELECT p.id_product, p.name, p.price,
           GROUP_CONCAT(DISTINCT i.ingredient_group) as groups,
           GROUP_CONCAT(DISTINCT po.id_occasion) as occasions
    FROM Products p
    LEFT JOIN Product_Ingredients pi ON p.id_product = pi.id_product
    LEFT JOIN Ingredients i ON pi.id_ingredients = i.id_ingredients
    LEFT JOIN Product_Occasion po ON p.id_product = po.id_product
    GROUP BY p.id_product
""").fetchall()

product_info = {}
for p in products:
    pid = p[0]
    product_info[pid] = {
        'name': p[1],
        'price': p[2],
        'groups': p[3].split(',') if p[3] else [],
        'occasions': [int(x) for x in p[4].split(',')] if p[4] else []
    }

# Параметры
months = [(2026,1,'январь'), (2026,2,'февраль'), (2026,3,'март')]
orders_per_month = {(2026,1): 8, (2026,2): 8, (2026,3): 12}

target_groups = {
    'фрукты/ягоды': {(2026,1):0, (2026,2):0, (2026,3):5},
    'молочные':     {(2026,1):20, (2026,2):20, (2026,3):20}
}
target_occasions = {
    2: {(2026,1):5, (2026,2):2, (2026,3):1},   # Новый год
    3: {(2026,1):2, (2026,2):5, (2026,3):12},  # Детский праздник
    1: {(2026,1):0, (2026,2):5, (2026,3):20}   # Пасха
}

def select_product(month, year, target_groups, target_occasions, product_info):
    prefer_groups = [g for g, t in target_groups.items() if t.get((year, month), 0) > 0]
    prefer_occasions = [o for o, t in target_occasions.items() if t.get((year, month), 0) > 0]
    candidates = []
    for pid, info in product_info.items():
        if prefer_groups and any(g in info['groups'] for g in prefer_groups):
            candidates.append(pid)
        elif prefer_occasions and any(o in info['occasions'] for o in prefer_occasions):
            candidates.append(pid)
    if not candidates:
        candidates = list(product_info.keys())
    return random.choice(candidates)

current_id_sale = c.execute("SELECT MAX(id_sale) FROM Sale").fetchone()[0] or 0
current_id_sale += 1

for year, month, month_name in months:
    num_orders = orders_per_month.get((year, month), 7)
    first_day = datetime(year, month, 1)
    last_day = datetime(year, month, calendar.monthrange(year, month)[1])
    
    group_counts = {g: 0 for g in target_groups}
    occasion_counts = {o: 0 for o in target_occasions}
    
    for _ in range(num_orders):
        day = random.randint(1, last_day.day)
        date_sale = datetime(year, month, day).strftime('%Y-%m-%d')
        customer = random.choice(customers)
        
        # Количество товаров в заказе (1–4)
        num_items = random.randint(1, 4)
        items = []
        used_products = set()
        
        # Выбираем уникальные товары
        for _ in range(num_items):
            # Если уже использовали все товары, выходим
            if len(used_products) >= len(product_info):
                break
            # Пытаемся выбрать товар, которого ещё нет в заказе
            pid = None
            for _ in range(50):  # 50 попыток найти подходящий уникальный товар
                candidate = select_product(month, year, target_groups, target_occasions, product_info)
                if candidate not in used_products:
                    pid = candidate
                    break
            if pid is None:
                # если не нашли уникальный, берём любой
                pid = random.choice([p for p in product_info if p not in used_products])
            used_products.add(pid)
            info = product_info[pid]
            price = info['price']
            quantity = random.randint(1, 3)
            items.append((pid, quantity, price))
        
        if not items:
            continue  # на случай, если не удалось выбрать товары (бывает редко)
        
        total_price = sum(q * p for _, q, p in items)
        if total_price > 3000:
            items = items[:2]
            total_price = sum(q * p for _, q, p in items)
        
        c.execute("""
            INSERT INTO Sale (id_sale, id_customer, date_sale) 
            VALUES (?, ?, ?)
        """, (current_id_sale, customer[0], date_sale))
        
        for pid, quantity, price in items:
            c.execute("""
                INSERT INTO Magazine_Sales (id_sale, id_product, quantity, price) 
                VALUES (?, ?, ?, ?)
            """, (current_id_sale, pid, quantity, price))
            info = product_info[pid]
            for g in info['groups']:
                if g in group_counts:
                    group_counts[g] += quantity
            for o in info['occasions']:
                if o in occasion_counts:
                    occasion_counts[o] += quantity
        
        current_id_sale += 1
    
    print(f"\n{month_name} {year}: создано {num_orders} заказов")
    for g, target in target_groups.items():
        actual = group_counts.get(g, 0)
        print(f"  {g}: {actual} (цель {target.get((year, month), 0)})")
    for o, target in target_occasions.items():
        actual = occasion_counts.get(o, 0)
        print(f"  Праздник {o}: {actual} (цель {target.get((year, month), 0)})")
    
    conn.commit()

print("\nГенерация завершена.")
conn.close()