# Проект 8-го спринта - Потоковая обработка данных

### Обшее описание задачи
В продукт планируют внедрить систему - добавление ресторана в "Избранное". Только тогда пользователю будут поступать уведомления о специальных акциях с ограниченным сроком действия.

### Основная цель:
- [] 1. Читать данные из Kafka с помощью Spark Structured Streaming и Python в режиме реального времени.
- [] 2. Отправлять тестовое сообщение в Kafka с помощью kcat - (не будет входного потока данных в Kafka от генератора данных.)
- [] 3. Получать список подписчиков из базы данных Postgres.
- [] 4. джойнить данные из Kafka с данными из БД.
- [x] 5. сохранять в памяти полученные данные, чтобы не собирать их заново после отправки в Postgres или Kafka.
- [] 6.отправлять выходное сообщение в Kafka с информацией об акции, пользователе со списком избранного и ресторане, а ещё вставлять записи в Postgres, чтобы впоследствии получить фидбэк от пользователя. Сервис push-уведомлений будет читать сообщения из Kafka и формировать готовые уведомления. 

## 1. Задача **Читать данные из Kafka** 
Задача реализована в `DAG_initial_load.py`

## 2. Задача **Отправлять тестовое сообщение в Kafka**
Задача реализована в `DAG_main_calc_marts.py` и `calculating_user_analitics_job.py`; `calculating_geo_analitics_job.py`.

## 3. Задача **Витрина в разрезе зон**
Задача реализована в `DAG_main_calc_marts.py` и `calculating_geo_analitics_job.py`.
Логика расчета:

#### Состав витрины - `df_geo_analitics_mart` (разрезе зон):
* `month` — месяц расчёта;
* `week` — неделя расчёта;
* `zone_id` — идентификатор зоны (города);
* `week_message` — количество сообщений за неделю;
* `week_reaction` — количество реакций за неделю;
* `week_subscription` — количество подписок за неделю;
* `week_user` — количество регистраций за неделю;
* `month_message` — количество сообщений за месяц;
* `month_reaction` — количество реакций за месяц;
* `month_subscription` — количество подписок за месяц;
* `month_user` — количество регистраций за месяц.

```
+-------+-------------------+-------------------+------------+-------------+---------+----------+-------------+--------------+-----------------+------------------+
|zone_id|               week|              month|week_message|month_message|week_user|month_user|week_reaction|month_reaction|week_subscription|month_subscription|
+-------+-------------------+-------------------+------------+-------------+---------+----------+-------------+--------------+-----------------+---------
```

## 4. Задача **Витрина для рекомендации друзей**
Задача реализована в `DAG_main_calc_marts.py` и `calculating_frends_recomendations_job.py`.
##### Логика расчета:
	I. Из слоя Subscription получаем все подписки (убрав дубли, если есть)
		df_all_subscriptions 
			|-- chanel_id: long (nullable = false)
			|-- user_id: long (nullable = false)
	
```
root
 |-- user_right: long (nullable = true)
 |-- user_left: long (nullable = true)
 |-- zone_id: long (nullable = true)
 |-- processed_dttm: timestamp (nullable = false)
 |-- local_time: timestamp (nullable = true)
```


#### Слои хранилища
* `RAW` - /user/master/data/geo/events/ (**hdfs**)
* `STG` - /user/dosperados/data/events/ (**hdfs**)
* `DDS - geo` - /user/dosperados/data/citygeodata/geo.csv (**hdfs**)
* `DDS - events` - /user/dosperados/data/events/date=yyyy-mm-dd (**hdfs**)
* `Mart` - /user/dosperados/data/marts (**hdfs**)
	* geo  - разрезе зон
	* users - в разрезе пользователей


### Описание схемы RAW-слоя
```
root
 |-- event: struct (nullable = true)
 |    |-- admins: array (nullable = true)
 |    |    |-- element: long (containsNull = true)
 |    |-- channel_id: long (nullable = true)
 |    |-- datetime: string (nullable = true)
 |    |-- media: struct (nullable = true)
 |    |    |-- media_type: string (nullable = true)
 |    |    |-- src: string (nullable = true)
 |    |-- message: string (nullable = true)
 |    |-- message_channel_to: long (nullable = true)
 |    |-- message_from: long (nullable = true)
 |    |-- message_group: long (nullable = true)
 |    |-- message_id: long (nullable = true)
 |    |-- message_to: long (nullable = true)
 |    |-- message_ts: string (nullable = true)
 |    |-- reaction_from: string (nullable = true)
 |    |-- reaction_type: string (nullable = true)
 |    |-- subscription_channel: long (nullable = true)
 |    |-- subscription_user: string (nullable = true)
 |    |-- tags: array (nullable = true)
 |    |    |-- element: string (containsNull = true)
 |    |-- user: string (nullable = true)
 |-- event_type: string (nullable = true)
 |-- lat: double (nullable = true)
 |-- lon: double (nullable = true)
 
 +--------------------+------------+-------------------+------------------+
|               event|  event_type|                lat|               lon|
+--------------------+------------+-------------------+------------------+
|[,, 2022-05-21 02...|subscription|-33.543102424518636|151.48624064210895|
|[[103235], 859291...|     message| -33.94742527623598|151.32387878072961|
|[,, 2022-05-21 12...|    reaction| -37.12220179510437|144.28231815733022|
|[,, 2022-05-21 17...|subscription| -20.81900702450072|149.59015699102054|
|[[107808], 865707...|     message|-11.539551427762717|131.17148068495302|
+--------------------+------------+-------------------+------------------+
```

## Схема взаимодействия
Airflow startr DAG -> PySpark read -> Hadoop -> PySpark calculation metric -> Store result Hadoop



## Запуск проекта
1. Загрузить **variables** из файла `/src/variables.json` в Airflow - Variable
2. Инициальная загрузка данных **DAG_initial_load.py** выполняется в ручную (один раз) далее используется скрипт инкрементальной загрузки и расчетов
3. Обновление STG слоя - **DAG_main_calc_marts.py** автозапуск каждый день и расчета витрины.
4. Результаты расчетов сохранены в формате `parquet` в соответвующих папках
	- hdfs:/user/dosperados/marts/users
	- hdfs:/user/dosperados/marts/geo
	- dfs:/user/dosperados/marts/friend_recomendation

### Структура репозитория

Внутри `src` расположены две папки:
- `/src/dags` - DAG - файлы
	- `DAG_initial_load.py` — Инициальная партиционорование данных  выполняется в ручную (однакратно)  
	- `DAG_main_calc_marts.py` — DAG обновляет слой STG из источника и произвоодит расчет метирик ветрины (на заданную глубину).
	
- `/src/sqripts` - py файлы c job'ами
	- `initial_load_job.py` — Job инициальной загрузки.
	- `calculating_user_analitics_job.py` — Job расчета пользовательских метрик и сохранения витрины.
	- `calculating_geo_analitics_job.py` — Job расчета geo метрик и сохранения витрины.
	- `calculating_friend_recomendation_analitics_job.py` - Job расчета метрик ветрины рекомендации друзей.
	- `update_stg_by_date_job.py`  — Job обновления STG.

