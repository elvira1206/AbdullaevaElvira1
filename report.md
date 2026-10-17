# Отчёт по практической работе №1

Развёртывание локального Lakehouse, ELT-преобразование и бенчмарк форматов/сжатия

## 1. Вариант и масштаб

| Поле | Значение |
|---|---|
| ФИО (как в VK Education) | Абдуллаева Эльвира Кожохматовна |
| Вариант (запрос) | 2 — Q10 «Returned Item Reporting» |
| Масштаб sfN | sf1 |
| Почему выбрал такой масштаб | sf1 даёт около 6 млн строк в lineitem (самая большая таблица). Этого достаточно, чтобы увидеть разницу между кодеками сжатия и между слоями, и при этом всё помещается в память и на диск ноутбука, а запросы выполняются за секунды. Больший масштаб (sf10 и выше) сильно замедлил бы загрузку и бенчмарк без новой информации для задачи. Один и тот же sf1 использован на всех слоях. |

## 2. Поднятые сервисы — и почему именно они

| Сервис | Роль в стеке | Почему именно он (а не альтернатива) |
|---|---|---|
| MinIO | S3-совместимое объектное хранилище (сюда физически падают parquet-файлы) | Открытый исходный код, S3 API, поэтому любой движок работает с ним как с облачным S3, но локально и бесплатно. Образ взят из репозитория Chainguard (`cgr.dev/chainguard/minio`), так как MinIO перестал публиковать образы на Docker Hub и Quay.|
| PostgreSQL | БД, где Lakekeeper хранит метаданные каталога | Lakekeeper требует реляционную БД для метаданных, Postgres надёжен, открыт и штатно поддерживается. Данные хранятся в именованном томе `pg-data`, поэтому настройки каталога переживают пересоздание контейнера.|
| Lakekeeper | Iceberg REST Catalog — каталог метаданных таблиц | Реализует открытый REST-протокол Iceberg, поэтому каталог не привязан к конкретному движку (Trino, Spark и т.д. работают с одним каталогом). Лёгкий, запускается одним контейнером. Авторизация отключена осознанно: стенд локальный и учебный. |
| Trino | SQL-движок: ходит в Lakekeeper за метаданными и читает/пишет данные в MinIO | Распределённый SQL-движок с коннектором Iceberg и встроенным генератором данных TPC-H (`tpch.sfN`), поэтому ничего не нужно скачивать. Стандартный SQL, подключение из DBeaver по JDBC. |
| (Superset / Metabase) | Отчёт/дашборд поверх данных | |

### Скрипты, которыми поднимал стек

```yaml
services:
  minio:
    volumes: ["minio-data:/data"]
    image: cgr.dev/chainguard/minio:latest
    container_name: lakehouse-minio
    command: server /data --console-address ":9001"
    environment: {MINIO_ROOT_USER: admin, MINIO_ROOT_PASSWORD: password123}
    ports: ["9000:9000", "9001:9001"]
  pg:
    volumes: ["pg-data:/var/lib/postgresql/data"]
    image: postgres:17
    container_name: lakehouse-pg
    environment: {POSTGRES_USER: lk, POSTGRES_PASSWORD: lk, POSTGRES_DB: lk}
    healthcheck: {test: ["CMD-SHELL","pg_isready -U lk"], interval: 5s, retries: 10}
  migrate:
    image: quay.io/lakekeeper/catalog:latest-main
    command: ["migrate"]
    environment: &lk
      LAKEKEEPER__PG_ENCRYPTION_KEY: "secretkey123"
      LAKEKEEPER__PG_DATABASE_URL_READ: postgresql://lk:lk@pg:5432/lk
      LAKEKEEPER__PG_DATABASE_URL_WRITE: postgresql://lk:lk@pg:5432/lk
    depends_on: {pg: {condition: service_healthy}}
  lakekeeper:
    image: quay.io/lakekeeper/catalog:latest-main
    container_name: lakekeeper
    command: ["serve"]
    environment: *lk
    ports: ["8181:8181"]
    depends_on: {migrate: {condition: service_completed_successfully}}
  trino:
    image: trinodb/trino:latest
    container_name: trino
    ports: ["8080:8080"]
    volumes: ["./trino/catalog:/etc/trino/catalog"]
    depends_on: [lakekeeper]
volumes:
  minio-data:
  pg-data:
```

Файлы каталогов Trino:

```
# trino/catalog/tpch.properties
connector.name=tpch

# trino/catalog/lake.properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://lakekeeper:8181/catalog
iceberg.rest-catalog.warehouse=lakehouse
fs.s3.enabled=true
s3.endpoint=http://minio:9000
s3.path-style-access=true
s3.region=us-east-1
s3.aws-access-key=admin
s3.aws-secret-key=password123
```

Ручные шаги: в MinIO создан бакет `lakehouse`; в Lakekeeper выполнен bootstrap и создан warehouse `lakehouse` (тип S3 compatible, endpoint `http://minio:9000`, path-style включён, vended credentials и remote signing выключены); затем `docker restart trino`.

Вывод `docker ps`:

![Терминал](img/docker_ps.png)


## 3. Разбор запроса — какие данные нужны

Q10 находит топ-20 клиентов по выручке от возвращённых позиций за 4-й квартал 1993 года.

| Таблица TPC-H | Нужные колонки | Что запрос с ними делает (фильтр / джойн / агрегат / вывод) |
|---|---|---|
| customer | custkey, name, acctbal, address, phone, comment, nationkey | custkey: джойн с orders и группировка; name, acctbal, address, phone, comment: вывод и группировка; nationkey: джойн с nation |
| orders | orderkey, custkey, orderdate | custkey: джойн с customer; orderkey: джойн с lineitem; orderdate: фильтр по Q4 1993 (с 1993-10-01 до 1994-01-01) |
| lineitem | orderkey, extendedprice, discount, returnflag | orderkey: джойн с orders; extendedprice и discount: агрегат sum(extendedprice * (1 - discount)) как revenue; returnflag: фильтр = 'R' (возвращённые позиции) |
| nation | nationkey, name | nationkey: джойн с customer; name: вывод как nation |

Остальные колонки TPC-H запросу не нужны, поэтому в слои они не переносились.

## 4. Медальон — решения по каждому слою

Во всех слоях формат Parquet, сжатие ZSTD (в Bronze дополнительно сравнивается с GZIP в разделе 5), партиционирование не применялось. Размеры взяты из `SELECT sum(file_size_in_bytes)/1024.0/1024 FROM <таблица>$files`, число строк из `SELECT count(*)`.

### Bronze (сырой)

| Таблица | Колонки (что оставил) | Строк | Вес, МБ | Формат | Сжатие | Партиционирование | Обоснование |
|---|---|---|---|---|---|---|---|
| customer | custkey, name, address, nationkey, phone, acctbal, comment | 150000 | 7.23 | PARQUET | ZSTD | нет | ✍️ |
| orders | orderkey, custkey, orderdate | 1500000 | 6.66 | PARQUET | ZSTD | нет | ✍️ |
| lineitem | orderkey, extendedprice, discount, returnflag | 6001215 | 30.96 | PARQUET | ZSTD | нет | ✍️ |
| nation | nationkey, name | 25 | 0.0006 | PARQUET | ZSTD | нет | ✍️ |
| **Итого Bronze** | | 7651240 | **44.86** | | | | |

Расчёт итога: 7.2306 + 6.6608 + 30.9634 + 0.0006 = 44.8554 МБ (≈ 44.86).

Подсказка для обоснования: Parquet колоночный, запрос читает только нужные колонки; ZSTD даёт хорошее сжатие при малой цене CPU; партиции не нужны, потому что самая большая таблица весит около 31 МБ, и разбиение на много мелких файлов только замедлило бы чтение. В Bronze оставлены только нужные запросу колонки (а не все колонки TPC-H), чтобы не хранить лишнее; это осознанное отступление от «полностью сырых» данных.

### Silver (очищенный)

| Таблица | Колонки (что оставил) | Строк | Вес, МБ | Формат | Сжатие | Партиционирование | Обоснование |
|---|---|---|---|---|---|---|---|
| customer_nation | custkey, name, address, phone, acctbal, comment, nation | 150000 | 7.23 | PARQUET | ZSTD | нет | Джойн customer и nation выполнен заранее, чтобы в Gold не повторять его и сразу иметь название страны. ✍️ |
| orders_q4_1993 | orderkey, custkey, orderdate | 57069 | 0.36 | PARQUET | ZSTD | нет | Срез по кварталу: оставлены только заказы с 1993-10-01 до 1994-01-01. ✍️ |
| lineitem_returned | orderkey, extendedprice, discount | 1478870 | 7.74 | PARQUET | ZSTD | нет | Оставлены только возвращённые позиции (returnflag = 'R'); сама колонка returnflag больше не нужна. ✍️ |

Эффект фильтрации относительно Bronze:
- orders: 1 500 000 → 57 069 строк (≈ 3,8 %), вес 6.66 → 0.36 МБ;
- lineitem: 6 001 215 → 1 478 870 строк (≈ 24,6 %), вес 30.96 → 7.74 МБ.

Замечание: данные TPC-H генерируются уже чистыми (без пропусков и дублей), поэтому «очистка» в Silver сводится к фильтрации по условиям запроса, отбору колонок и обогащению (джойн со странами). Партиционирование по месяцу `orders_q4_1993` не применялось: в срезе всего три месяца и 0.36 МБ, партиции дали бы лишь лишние мелкие файлы. ✍️ (если пробовали партиции, опишите)

### Gold (витрина)

| Таблица | Колонки | Строк | Вес, МБ | Формат | Сжатие | Партиционирование | Обоснование |
|---|---|---|---|---|---|---|---|
| top20_returns | custkey, name, revenue, acctbal, nation, address, phone, comment | 20 | 0.0031 | PARQUET | ZSTD | нет | Готовый ответ на Q10: топ-20 клиентов по выручке от возвратов за Q4 1993. 20 строк (3 КБ), партиционирование бессмысленно. ✍️ |

SQL, которым создавались слои:

```sql
-- Bronze (повторить для customer, orders, lineitem, nation со своими колонками)
CREATE SCHEMA lake.bronze;
CREATE TABLE lake.bronze.lineitem
WITH (format='PARQUET', compression_codec='ZSTD') AS
SELECT orderkey, extendedprice, discount, returnflag FROM tpch.sf1.lineitem;
-- ✍️ вставьте остальные три CREATE TABLE для Bronze

-- Silver
CREATE SCHEMA lake.silver;
CREATE TABLE lake.silver.customer_nation
WITH (format='PARQUET', compression_codec='ZSTD') AS
SELECT c.custkey, c.name, c.address, c.phone, c.acctbal, c.comment, n.name AS nation
FROM lake.bronze.customer c JOIN lake.bronze.nation n ON c.nationkey = n.nationkey;

CREATE TABLE lake.silver.orders_q4_1993
WITH (format='PARQUET', compression_codec='ZSTD') AS
SELECT orderkey, custkey, orderdate FROM lake.bronze.orders
WHERE orderdate >= DATE '1993-10-01' AND orderdate < DATE '1994-01-01';

CREATE TABLE lake.silver.lineitem_returned
WITH (format='PARQUET', compression_codec='ZSTD') AS
SELECT orderkey, extendedprice, discount FROM lake.bronze.lineitem WHERE returnflag = 'R';

-- Gold
CREATE SCHEMA lake.gold;
CREATE TABLE lake.gold.top20_returns
WITH (format='PARQUET', compression_codec='ZSTD') AS
SELECT c.custkey, c.name, sum(l.extendedprice * (1 - l.discount)) AS revenue,
       c.acctbal, c.nation, c.address, c.phone, c.comment
FROM lake.silver.customer_nation c
JOIN lake.silver.orders_q4_1993 o ON c.custkey = o.custkey
JOIN lake.silver.lineitem_returned l ON l.orderkey = o.orderkey
GROUP BY c.custkey, c.name, c.acctbal, c.nation, c.address, c.phone, c.comment
ORDER BY revenue DESC LIMIT 20;
```

Слои связаны: Gold строится из Silver, Silver из Bronze, Bronze из `tpch.sf1`. Результат Gold: 20 строк, первая строка — клиент 57040 с выручкой около 734 235.

## 5. Тесты производительности

Тестовый запрос — Q10, выполненный по таблицам `lake.bronze.*` (с джойнами customer, orders, lineitem, nation).

| Сжатие на Bronze | Время запроса, с (3 запуска) | Среднее, с | Размер Bronze, МБ | Твой вывод (что лучше и почему) |
|---|---|---|---|---|
| GZIP | ✍️ / ✍️ / ✍️ | ✍️ | ✍️ | ✍️ |
| ZSTD | 4.115 / 2.013 / 1.575 | 2.57 | 44.86 | ✍️ |

Расчёт среднего для ZSTD: (4.115 + 2.013 + 1.575) / 3 = 7.703 / 3 ≈ 2.57 с. Без первого запуска (прогрев) среднее (2.013 + 1.575) / 2 ≈ 1.79 с.

Как замерял: Q10 запускался 3 раза подряд на каждом кодеке; время взято из `system.runtime.queries` как разница `end` и `created`; первый запуск включает прогрев кэшей и метаданных, поэтому для сравнения использованы одинаковые условия для обоих кодеков. Размер Bronze — сумма `file_size_in_bytes` по четырём таблицам из `$files`.

Подсказка для вывода: сравните оба столбца (время и размер) и ответьте, что выгоднее. Обычно GZIP даёт чуть меньший размер, но медленнее при чтении/записи, а ZSTD близок по размеру и быстрее; но на 45 МБ разница в секундах может быть в пределах шума. Пишите то, что получилось у вас, а не это предположение.

## 6. Часть 2 — осмысление и работа с инструментами

### 6.1. Понять и объяснить

| Вопрос | Твой ответ |
|---|---|
| Зачем нужны большие данные и lakehouse (а не обычная БД/хранилище)? | ✍️ 4–5 предложений своими словами (идеи: объёмы не помещаются в одну БД; данные в дешёвом объектном хранилище, вычисления отдельно; открытые форматы Parquet/Iceberg без привязки к вендору; ACID, schema evolution и time travel поверх файлов; одни и те же данные для BI и для ML) |
| Пример schema evolution на Silver: добавил/переименовал колонку — что произошло? | ✍️ (результат ваших запросов ниже) |
| Пример скрипта восстановления данных из снапшота (time travel) | см. скрипт ниже |

Команды для schema evolution (выполнить в DBeaver и описать, что увидели):

```sql
ALTER TABLE lake.silver.customer_nation ADD COLUMN test_col varchar;
SELECT * FROM lake.silver.customer_nation LIMIT 5;   -- у старых строк test_col = NULL
ALTER TABLE lake.silver.customer_nation RENAME COLUMN test_col TO note;
```

Команды для time travel:

```sql
SELECT snapshot_id, committed_at FROM lake.silver."customer_nation$snapshots";
SELECT count(*) FROM lake.silver.customer_nation FOR VERSION AS OF <snapshot_id>;
CALL lake.system.rollback_to_snapshot('silver', 'customer_nation', <snapshot_id>);
```

### 6.2. Подключить инструмент к lakehouse

| Инструмент | Почему выбрал именно его |
|---|---|
| ✍️ | ✍️ |

Скрипт развертывания:

```yaml
# ✍️
```

Мои действия в UI/консоли (как подключился и увидел данные Bronze):

1. ✍️
2. ✍️

Скриншоты: ✍️ (`![описание](img/файл.png)`)

## 7. Выводы

✍️ 3–5 предложений своими словами. Опорные тезисы: медальон разделяет сырые, очищенные и готовые для аналитики данные, поэтому Gold весит 0.0031 МБ против 44.86 МБ Bronze, а запрос к нему мгновенный; Parquet + сжатие сильно уменьшает объём; сравнение GZIP и ZSTD показало ✍️; выбранный стек открыт и заменяем по частям (MinIO, Lakekeeper, Trino независимы).