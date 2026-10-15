using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Channels;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace TaskApi;

#region Contract

public record TaskCompletedEvent(
    Guid TaskId,
    string Title,
    DateTimeOffset CompletedAt,
    Priority Priority);

/// <summary>
/// Публикация событий. Не блокирует HTTP-поток: Publish только кладёт событие в очередь.
/// </summary>
public interface ITaskEventPublisher
{
    void Publish(TaskCompletedEvent @event);
}

public class RabbitMqOptions
{
    public string Uri { get; set; } = "amqp://guest:guest@localhost:5672/";
    public string Exchange { get; set; } = "task.events";
    public string RoutingKey { get; set; } = "task.completed";
    /// <summary>Сколько ждём отправки уже накопленных сообщений при остановке приложения.</summary>
    public int ShutdownDrainSeconds { get; set; } = 5;
}

#endregion

#region Publisher

/// <summary>
/// Fire-and-forget публикация через in-memory очередь + фоновый воркер.
/// Если брокер недоступен — задача всё равно завершается, сообщение уходит в лог (fail silently + log).
/// </summary>
public sealed class RabbitMqPublisher : ITaskEventPublisher, IHostedService
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        Converters = { new JsonStringEnumConverter() }
    };

    private readonly Channel<TaskCompletedEvent> _queue =
        Channel.CreateBounded<TaskCompletedEvent>(new BoundedChannelOptions(1000)
        {
            // Очередь переполнилась — лучше выкинуть событие, чем подвесить HTTP-запрос.
            FullMode = BoundedChannelFullMode.DropWrite
        });

    private readonly RabbitMqOptions _options;
    private readonly ILogger<RabbitMqPublisher> _logger;

    private Task? _worker;
    private IConnection? _connection;
    private IChannel? _channel;

    public RabbitMqPublisher(IOptions<RabbitMqOptions> options, ILogger<RabbitMqPublisher> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    public void Publish(TaskCompletedEvent @event)
    {
        if (!_queue.Writer.TryWrite(@event))
        {
            _logger.LogWarning("Очередь публикации переполнена, событие потеряно: {TaskId}", @event.TaskId);
        }
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        // Соединение поднимаем лениво в воркере: приложение должно стартовать даже без RabbitMQ.
        _worker = Task.Run(ProcessQueueAsync, CancellationToken.None);
        return Task.CompletedTask;
    }

    #region Graceful shutdown

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        // 1. Больше ничего не принимаем. 2. Даём воркеру дослать накопленное. 3. Закрываем канал и коннект.
        _queue.Writer.TryComplete();

        if (_worker is not null)
        {
            var drain = Task.Delay(TimeSpan.FromSeconds(_options.ShutdownDrainSeconds), CancellationToken.None);
            if (await Task.WhenAny(_worker, drain) == drain)
            {
                _logger.LogWarning("Не успели дослать все события за {Seconds}с, закрываемся", _options.ShutdownDrainSeconds);
            }
        }

        try
        {
            if (_channel is not null) await _channel.CloseAsync(CancellationToken.None);
            if (_connection is not null) await _connection.CloseAsync(CancellationToken.None);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Ошибка при закрытии RabbitMQ");
        }
        finally
        {
            if (_channel is not null) await _channel.DisposeAsync();
            if (_connection is not null) await _connection.DisposeAsync();
        }
    }

    #endregion

    private async Task ProcessQueueAsync()
    {
        // ReadAllAsync без токена: завершается сам, когда Writer.Complete() и очередь опустела.
        await foreach (var @event in _queue.Reader.ReadAllAsync(CancellationToken.None))
        {
            try
            {
                var channel = await EnsureChannelAsync();
                if (channel is null)
                {
                    LogDropped(@event, null);
                    continue;
                }

                var body = JsonSerializer.SerializeToUtf8Bytes(@event, JsonOptions);
                await channel.BasicPublishAsync(_options.Exchange, _options.RoutingKey, body, CancellationToken.None);

                _logger.LogInformation("Опубликовано {RoutingKey} для задачи {TaskId}", _options.RoutingKey, @event.TaskId);
            }
            catch (Exception ex)
            {
                // Брокер лёг — задача уже завершена в БД, откатывать нечего. Пишем в лог и живём дальше.
                LogDropped(@event, ex);
                await ResetConnectionAsync();
            }
        }
    }

    private void LogDropped(TaskCompletedEvent @event, Exception? ex) =>
        _logger.LogError(ex, "RabbitMQ недоступен, событие не отправлено: {Payload}",
            Encoding.UTF8.GetString(JsonSerializer.SerializeToUtf8Bytes(@event, JsonOptions)));

    private async Task<IChannel?> EnsureChannelAsync()
    {
        if (_channel is { IsOpen: true }) return _channel;

        var factory = new ConnectionFactory { Uri = new Uri(_options.Uri) };
        _connection = await factory.CreateConnectionAsync(CancellationToken.None);
        _channel = await _connection.CreateChannelAsync(cancellationToken: CancellationToken.None);
        await _channel.ExchangeDeclareAsync(_options.Exchange, ExchangeType.Topic, durable: true,
            cancellationToken: CancellationToken.None);

        return _channel;
        // HACK: нет backoff при переподключении — при лежащем брокере будем дёргать его на каждое событие.
        // Доделал бы через Polly: retry с экспоненциальной паузой + circuit breaker.
    }

    private async Task ResetConnectionAsync()
    {
        try
        {
            if (_channel is not null) await _channel.DisposeAsync();
            if (_connection is not null) await _connection.DisposeAsync();
        }
        catch { /* ignore */ }

        _channel = null;
        _connection = null;
    }
}

#endregion
