using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Channels;
using RabbitMQ.Client;
using TaskApi.Model;

namespace TaskApi;

public class RabbitMqService : BackgroundService
{
    public const string ExchangeName = "task.events";
    public const string RoutingKey = "task.completed";

    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter<Priority>(allowIntegerValues: false) }
    };

    // TODO: очередь в памяти теряется при аварийной остановке. Для надёжной доставки нужна таблица исходящих событий в БД.
    private readonly Channel<TaskCompletedEvent> _messages = Channel.CreateBounded<TaskCompletedEvent>(
        new BoundedChannelOptions(100) { SingleReader = true, FullMode = BoundedChannelFullMode.Wait });

    private readonly ILogger<RabbitMqService> _logger;
    private readonly ConnectionFactory _factory;
    private IConnection? _connection;
    private IChannel? _channel;

    public RabbitMqService(IConfiguration configuration, ILogger<RabbitMqService> logger)
    {
        _logger = logger;
        _factory = new ConnectionFactory
        {
            HostName = configuration["RabbitMq:HostName"] ?? "localhost",
            Port = configuration.GetValue("RabbitMq:Port", 5672),
            UserName = configuration["RabbitMq:UserName"] ?? "task_api",
            Password = configuration["RabbitMq:Password"] ?? "task_api",
            VirtualHost = "/",
            ClientProvidedName = "TaskApi",
            AutomaticRecoveryEnabled = false,
            RequestedConnectionTimeout = TimeSpan.FromSeconds(2),
            HandshakeContinuationTimeout = TimeSpan.FromSeconds(2),
            ContinuationTimeout = TimeSpan.FromSeconds(2)
        };
    }

    public void Publish(TaskCompletedEvent message)
    {
        if (!_messages.Writer.TryWrite(message))
            _logger.LogWarning("Очередь публикации заполнена или закрыта. Событие: {Event}", JsonSerializer.Serialize(message, JsonOptions));
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            await foreach (var message in _messages.Reader.ReadAllAsync(stoppingToken))
            {
                var body = JsonSerializer.SerializeToUtf8Bytes(message, JsonOptions);
                try
                {
                    await ConnectAsync(stoppingToken);

                    // HACK: без подтверждений брокера и повторной отправки. При сбое событие останется только в журнале.
                    await _channel!.BasicPublishAsync(ExchangeName, RoutingKey, false,
                        new BasicProperties { ContentType = "application/json", Persistent = true },
                        body, stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    _logger.LogWarning("Публикация прервана при остановке приложения. Событие: {Event}", JsonSerializer.Serialize(message, JsonOptions));
                    break;
                }
                catch (Exception exception)
                {
                    _logger.LogWarning(exception, "Не удалось отправить событие в RabbitMQ. Событие: {Event}", JsonSerializer.Serialize(message, JsonOptions));
                    await DisconnectAsync();
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
        finally
        {
            await DisconnectAsync();
            while (_messages.Reader.TryRead(out var pending))
                _logger.LogWarning("Событие не отправлено при остановке приложения: {Event}", JsonSerializer.Serialize(pending, JsonOptions));
        }
    }

    private async Task ConnectAsync(CancellationToken ct)
    {
        if (_channel is { IsOpen: true })
            return;

        await DisconnectAsync();
        ITcpClient? socket = null;
        _factory.SocketFactory = addressFamily => socket = ConnectionFactory.DefaultSocketFactory(addressFamily);
        // При отмене рукопожатия клиент может долго ждать закрытия. Разрываем зависший сокет при остановке.
        using var cancelConnect = ct.Register(() => socket?.Dispose());
        _connection = await _factory.CreateConnectionAsync(ct);
        _channel = await _connection.CreateChannelAsync(cancellationToken: ct);
        await _channel.ExchangeDeclareAsync(ExchangeName, ExchangeType.Topic,
            durable: true, autoDelete: false, cancellationToken: ct);
    }

    private async Task DisconnectAsync()
    {
        try
        {
            // Закрытие соединения закрывает и его каналы. По таймауту клиент принудительно закрывает сокет.
            if (_connection is not null)
                await _connection.CloseAsync(TimeSpan.FromSeconds(1));
        }
        catch (Exception exception)
        {
            _logger.LogWarning(exception, "Не удалось корректно закрыть подключение RabbitMQ.");
        }

        try
        {
            if (_channel is not null)
                await _channel.DisposeAsync();
        }
        catch (Exception exception)
        {
            _logger.LogWarning(exception, "Не удалось освободить канал RabbitMQ.");
        }

        try
        {
            if (_connection is not null)
                await _connection.DisposeAsync();
        }
        catch (Exception exception)
        {
            _logger.LogWarning(exception, "Не удалось освободить соединение RabbitMQ.");
        }
        finally
        {
            _channel = null;
            _connection = null;
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _messages.Writer.TryComplete();
        using var drainTimeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        // Оставляем время на отмену зависшего подключения и закрытие соединения в общем лимите пяти секунд.
        drainTimeout.CancelAfter(TimeSpan.FromSeconds(4));

        try
        {
            if (ExecuteTask is not null)
                await ExecuteTask.WaitAsync(drainTimeout.Token);
        }
        catch (OperationCanceledException)
        {
            _logger.LogWarning("Время ожидания отправки событий истекло. Останавливаем публикацию.");
        }

        await base.StopAsync(cancellationToken);
    }
}
