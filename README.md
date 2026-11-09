# Асинхронный процессинг платежей

Тестовый микросервис на Python 3.12. FastAPI принимает запрос и отвечает `202 Accepted`. Consumer обрабатывает платёж через эмулятор шлюза и отправляет результат на webhook. Стек — FastAPI, Pydantic v2, SQLAlchemy 2.0 async, PostgreSQL, Alembic, RabbitMQ и FastStream.

## Запуск

Нужен Docker с плагином Compose. Значения по умолчанию предназначены для локального запуска. Для собственных настроек скопируйте `.env.example` в `.env`. Используйте пароли из URL-безопасных символов, поскольку Compose подставляет их в строки подключения.

```sh
docker compose --profile demo up --build --wait
```

API доступен на `http://127.0.0.1:8000`, консоль RabbitMQ — на `http://127.0.0.1:15672`. Пользователь RabbitMQ — `payments`, пароль по умолчанию — `local-rabbitmq-password`. Одноразовый контейнер `migrate` применяет миграции до запуска приложения. Профиль `demo` добавляет принимающий webhook для примеров. Основной сервис можно запустить командой `docker compose up --build --wait`.

## API

Для каждого запроса передавайте `X-API-Key`. Для `POST` также нужен `Idempotency-Key` длиной от 1 до 128 печатных ASCII-символов без пробелов. Денежные суммы удобно передавать строкой с точкой и не более чем двумя знаками после неё.

```sh
curl -i http://127.0.0.1:8000/api/v1/payments \
  -H 'X-API-Key: local-demo-key-change-this' \
  -H 'Idempotency-Key: invoice-42' \
  -H 'Content-Type: application/json' \
  -d '{"amount":"120.50","currency":"RUB","description":"Счёт 42","metadata":{"invoice":42},"webhook_url":"http://webhook:8000/hook/ok"}'
```

Ответ `202` содержит `payment_id`, `status` и `created_at`. Повтор того же запроса с тем же ключом возвращает тот же идентификатор и текущий статус. Использование ключа с другим содержимым возвращает `409 Conflict`.

```sh
curl -i http://127.0.0.1:8000/api/v1/payments/<payment_id> \
  -H 'X-API-Key: local-demo-key-change-this'
```

Замените `<payment_id>` на UUID из ответа. Карточка показывает сумму, валюту, описание, метаданные, статус и время обработки, URL и состояние webhook. Для своего получателя добавьте его имя хоста в `WEBHOOK_ALLOWED_HOSTS` через запятую. Указывайте точные имена доверенных хостов. Редиректы не выполняются. Принимающий demo-контейнер использует адреса `/hook/ok`, `/hook/retry` (два ответа 503) и `/hook/fail` (всегда 503).

## Как проходит платёж

1. PostgreSQL сохраняет платёж и запись Outbox в одной транзакции. Уникальный ключ в базе защищает от дублей при одновременных запросах.
2. Фоновый издатель выбирает готовую запись Outbox через `FOR UPDATE SKIP LOCKED`, отправляет её в долговечную очередь `payments.new` и помечает опубликованной после подтверждения RabbitMQ.
3. Один consumer ждёт 2–5 секунд и фиксирует результат эмуляции шлюза. Примерно 90% платежей проходят, 10% получают статус `failed`. Оба результата отправляются получателю. Повтор webhook использует сохранённый результат платежа.
4. После сбоев уведомления consumer записывает повтор в Outbox с задержкой 1 и 2 секунды. Всего три попытки. При исчерпании попыток запись попадает в `payments.dlq`, а `webhook_state` меняется на `dead_lettered`. Некорректные сообщения RabbitMQ направляет в ту же очередь.

Публикация событий и доставка webhook работают с гарантией «как минимум один раз». Если процесс завершится после принятия сообщения брокером или получателем, но до записи результата в PostgreSQL, возможен повтор. Consumer игнорирует уже завершённые и устаревшие попытки. В webhook передаётся стабильный `event_id`, он же заголовок `Idempotency-Key` для дедупликации у получателя. Ошибка доставки webhook хранится отдельно от результата платежа.

## Проверки

В GitHub Actions поднимаются реальные PostgreSQL и RabbitMQ. Проверка применяет миграцию, посылает конкурентные запросы при выключенном consumer, запускает consumer и проверяет доставку, повторы, задержки и DLQ. Для ручного прогона на свежих томах выполните:

```sh
docker compose --profile demo up --build --wait postgres rabbitmq api webhook
docker compose exec -T api alembic check
docker compose exec -T api python -m unittest discover -v
docker compose exec -T api python -m tests.smoke seed
docker compose up -d consumer
docker compose exec -T api python -m tests.smoke verify
```

`docker compose --profile demo down` остановит контейнеры и сохранит данные.

Справка по использованным механизмам — [FastStream RabbitBroker](https://faststream.ag2.ai/latest/api/faststream/rabbit/RabbitBroker/), [подтверждения RabbitMQ](https://www.rabbitmq.com/docs/confirms), [PostgreSQL в SQLAlchemy](https://docs.sqlalchemy.org/en/20/dialects/postgresql.html).
