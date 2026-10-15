using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Testcontainers.PostgreSql;
using Testcontainers.RabbitMq;
using TaskApi.Model;
using Xunit;

namespace TaskApi.Tests;

public class CompleteTaskIntegrationTest
{
    [Fact]
    public async Task CompleteTask_SavesTaskAndPublishesEvent()
    {
        await using var postgres = new PostgreSqlBuilder("postgres:17-alpine").Build();
        await using var rabbit = new RabbitMqBuilder("rabbitmq:4-management-alpine")
            .WithUsername("task_api").WithPassword("task_api").Build();
        await Task.WhenAll(postgres.StartAsync(), rabbit.StartAsync());

        await using var factory = new TaskApiFactory(postgres.GetConnectionString(), rabbit);
        using var client = factory.CreateClient();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));

        var jsonOptions = new JsonSerializerOptions(JsonSerializerDefaults.Web);
        jsonOptions.Converters.Add(new JsonStringEnumConverter<Priority>());

        var connectionFactory = new ConnectionFactory { Uri = new Uri(rabbit.GetConnectionString()) };
        await using var connection = await connectionFactory.CreateConnectionAsync(timeout.Token);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: timeout.Token);
        await channel.ExchangeDeclareAsync(RabbitMqService.ExchangeName, ExchangeType.Topic,
            durable: true, autoDelete: false, cancellationToken: timeout.Token);
        var queue = await channel.QueueDeclareAsync(cancellationToken: timeout.Token);
        await channel.QueueBindAsync(queue.QueueName, RabbitMqService.ExchangeName,
            RabbitMqService.RoutingKey, cancellationToken: timeout.Token);

        var received = new TaskCompletionSource<TaskCompletedEvent>(TaskCreationOptions.RunContinuationsAsynchronously);
        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += (_, args) =>
        {
            try
            {
                var message = JsonSerializer.Deserialize<TaskCompletedEvent>(args.Body.Span, jsonOptions);
                if (message is null)
                    throw new InvalidOperationException("Получено пустое событие завершения задачи.");
                received.TrySetResult(message);
            }
            catch (Exception exception)
            {
                received.TrySetException(exception);
            }
            return Task.CompletedTask;
        };
        await channel.BasicConsumeAsync(queue.QueueName, autoAck: true, consumer, cancellationToken: timeout.Token);

        using var createdResponse = await client.PostAsJsonAsync("/tasks",
            new { title = "Проверить отправку события", priority = "High" }, timeout.Token);
        Assert.Equal(HttpStatusCode.Created, createdResponse.StatusCode);
        var created = await createdResponse.Content.ReadFromJsonAsync<TaskItem>(jsonOptions, timeout.Token);
        Assert.NotNull(created);
        Assert.False(created.IsCompleted);
        Assert.Null(created.CompletedAt);

        using var completedResponse = await client.PutAsync($"/tasks/{created.Id}/complete", null, timeout.Token);
        Assert.Equal(HttpStatusCode.OK, completedResponse.StatusCode);
        var completed = await completedResponse.Content.ReadFromJsonAsync<TaskItem>(jsonOptions, timeout.Token);
        Assert.NotNull(completed);
        Assert.True(completed.IsCompleted);
        Assert.NotNull(completed.CompletedAt);

        var message = await received.Task.WaitAsync(timeout.Token);
        Assert.Equal(created.Id, message.TaskId);
        Assert.Equal(created.Title, message.Title);
        Assert.Equal(completed.CompletedAt.Value, message.CompletedAt);
        Assert.Equal(Priority.High, message.Priority);

        await using var scope = factory.Services.CreateAsyncScope();
        var db = scope.ServiceProvider.GetRequiredService<TaskDbContext>();
        var saved = await db.Tasks.AsNoTracking().SingleAsync(x => x.Id == created.Id, timeout.Token);
        Assert.True(saved.IsCompleted);
        Assert.Equal(message.CompletedAt, saved.CompletedAt);

        using var repeatedResponse = await client.PutAsync($"/tasks/{created.Id}/complete", null, timeout.Token);
        Assert.Equal(HttpStatusCode.Conflict, repeatedResponse.StatusCode);
    }

    private sealed class TaskApiFactory(string connectionString, RabbitMqContainer rabbit) : WebApplicationFactory<Program>
    {
        protected override void ConfigureWebHost(IWebHostBuilder builder)
        {
            builder.UseContentRoot(Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "../../../../")));
            builder.ConfigureAppConfiguration((_, config) => config.AddInMemoryCollection(
                new Dictionary<string, string?>
                {
                    ["ConnectionStrings:DefaultConnection"] = connectionString,
                    ["RabbitMq:HostName"] = rabbit.Hostname,
                    ["RabbitMq:Port"] = rabbit.GetMappedPublicPort(5672).ToString(),
                    ["RabbitMq:UserName"] = "task_api",
                    ["RabbitMq:Password"] = "task_api"
                }));
        }
    }
}
