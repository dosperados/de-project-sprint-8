# Проект 8-го спринта - Потоковая обработка данных

### Обшее описание задачи
В продукт планируют внедрить систему - добавление ресторана в "Избранное". Только тогда пользователю будут поступать уведомления о специальных акциях с ограниченным сроком действия.

### Основная цель:
- [x] 1. Читать данные из Kafka с помощью Spark Structured Streaming и Python в режиме реального времени.
- [x] 2. Отправлять тестовое сообщение в Kafka с помощью kcat - (не будет входного потока данных в Kafka от генератора данных.)
- [x] 3. Получать список подписчиков из базы данных Postgres.
- [x] 4. Джойнить данные из Kafka с данными из БД.
- [x] 5. Сохранять в памяти полученные данные, чтобы не собирать их заново после отправки в Postgres или Kafka.
- [x] 6. Отправлять выходное сообщение в Kafka с информацией об акции, пользователе со списком избранного и ресторане, а ещё вставлять записи в Postgres, чтобы впоследствии получить фидбэк от пользователя. Сервис push-уведомлений будет читать сообщения из Kafka и формировать готовые уведомления. 

## Запуск проекта
1. Загрузить отправку в поток Kafka информации по акциям ресторанов `/src/scripts/kafkacat_send_message01.sh`
	- Сам пример сообщения находится в папке `/src/data`
2. Создать таблицу для приема feeback сообщений в базе PostgreSQL запустив DDL скрипт `/src/scripts/DDL_create_table.sql`
3. Запустить основную программу обрабоки потока `/src/scripts/main.py`

