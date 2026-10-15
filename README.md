# TaskApi

Minimal API на .NET 9: задачи в PostgreSQL + событие `task.completed` в RabbitMQ при завершении.

## Структура

```
TaskApi/            приложение
TaskApi.Tests/      интеграционные тесты
docker-compose.yml  Postgres + RabbitMQ
```

## Запуск

Нужен .NET SDK 9. Поднять инфраструктуру (если нет своей):

```bash
docker compose up -d
```

```bash
dotnet run --project TaskApi
dotnet test
```

API на `http://localhost:5000`. Схема создаётся на старте через `EnsureCreated()` — миграций нет
(см. HACK в `Program.cs`).

Строка подключения в `appsettings.json` рассчитана на compose. Для своего инстанса проще
переопределить её переменной окружения, чем держать пароль в файле:

```powershell
$env:ConnectionStrings__Postgres = "Host=localhost;Port=5432;Database=taskdb;Username=postgres;Password=..."
```

Без RabbitMQ приложение стартует и работает: событие теряется с записью в лог (см. ниже).

## Эндпоинты

| Метод | Путь | Ответы |
|---|---|---|
| POST | `/tasks` | 201 / 400 |
| GET | `/tasks` | 200 |
| PUT | `/tasks/{id}/complete` | 200 / 404 / 409 |
| DELETE | `/tasks/{id}` | 204 / 404 |

```bash
curl -X POST localhost:5000/tasks -H 'Content-Type: application/json' \
     -d '{"title":"Buy milk","priority":"High"}'
```

## Решения по ключевым пунктам

**CompletedAt** — ставится только в обработчике `/complete`, при создании всегда `null`.

**Конкурентность** — оптимистичная блокировка на системном столбце `xmin`: Postgres меняет его при каждом
UPDATE, EF добавляет его в WHERE. Два одновременных `/complete` → второй получает
`DbUpdateConcurrencyException` → 409. Плюс быстрая проверка `IsCompleted` для последовательного повтора.
Отдельный столбец версии не нужен, но решение прибито к Postgres.

**Порядок операций** — `SaveChangesAsync` → затем `Publish`. Если БД упадёт, сообщение не уйдёт.
Обратная ситуация (запись в БД прошла, брокер лёг) допускается сознательно: событие теряется с логом.
Честное решение — transactional outbox, на него времени нет.

**Публикация** — `Publish()` только кладёт событие в `Channel<T>` и сразу возвращается, HTTP-поток не ждёт
ни коннекта, ни confirms. Отправляет фоновый воркер.

**Graceful shutdown** — `RabbitMqPublisher.StopAsync`: закрывает writer, ждёт слива очереди до 5 секунд
(настраивается `RabbitMq:ShutdownDrainSeconds`), затем закрывает канал и соединение. Воркер намеренно не
использует `stoppingToken`, иначе отмена оборвала бы недоотправленные сообщения.

## Что бы доделал

- `// HACK` в `RabbitMqService.cs` — нет backoff при переподключении к брокеру (Polly: retry + circuit breaker).
- `// HACK` в `TaskDbContext.cs` / тестах — интеграционный тест на InMemory, поэтому гонка на xmin не покрыта.
  Нужен Testcontainers с реальным Postgres и тест на два параллельных `/complete`.
- Там же не покрыт `DELETE`: `ExecuteDeleteAsync` на InMemory не работает.
- Миграции EF вместо `EnsureCreated()`.
- Outbox вместо fire-and-forget, если потеря события станет неприемлемой.
