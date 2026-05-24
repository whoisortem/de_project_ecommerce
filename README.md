# E-commerce ETL Pipeline

Пайплайн для обработки данных e-commerce магазина. Данные взял с Kaggle, прогнал через три слоя (Staging → ODS → CDM) и положил в S3 и PostgreSQL.

## Стек

- **S3 (MinIO)** — Data Lake
- **PostgreSQL** — реляционное хранилище
- **PySpark** — обработка данных
- **Airflow** — оркестрация
- **Jupyter Notebook** — разработка и проверка джобов

## Как устроено

**Staging** — сырые данные как есть, загружены через API.

**ODS** — очищенные данные, Snowflake Schema. Три таблицы фактов: `f_orders`, `f_order_items`, `f_order_payments`. Три справочника: `d_customers`, `d_products`, `d_sellers`. Отсюда можно строить BI-отчётность.

**CDM** — витрины для бизнеса:
- `dm_count_status_orders` — заказы по статусам на сегодня
- `dm_top5_category_per_month` — топ-5 категорий товаров в разрезе месяц/год

## Структура репозитория

```
├── dags/          # оркестрация Airflow
└── spark-jobs/    # джобы для построения витрин
```

## Скриншоты

![DAG-и отработали корректно](screenshots/status_dags.png)
![Таблицы в PostgreSQL](screenshots/postgresql_tables.png)
![dm_count_status_orders](screenshots/dm_count_status_orders.png)
![dm_top5_category_per_month в Data Lake](screenshots/dm_top5_category_per_month.png)
![f_order_items в PostgreSQL](screenshots/select_f_order_items.png)

## Контакты

Telegram: [@whoisortem](https://t.me/whoisortem)