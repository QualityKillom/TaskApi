using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using TaskApi;
using Xunit;

namespace TaskApi.Tests;

#region Test doubles

/// <summary>Вместо реального RabbitMQ — проверяем, что событие ушло с правильным payload.</summary>
public sealed class FakeEventPublisher : ITaskEventPublisher
{
    public List<TaskCompletedEvent> Published { get; } = [];

    public void Publish(TaskCompletedEvent @event) => Published.Add(@event);
}

public class TaskApiFactory : WebApplicationFactory<Program>
{
    public FakeEventPublisher Publisher { get; } = new();

    // optionsLifetime у AddDbContext по умолчанию Scoped: с Guid.NewGuid() прямо в делегате
    // каждый запрос уходил бы в свою БД (POST создал, PUT уже не нашёл -> 404).
    private readonly string _databaseName = $"tasks-{Guid.NewGuid()}";

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.ConfigureServices(services =>
        {
            // HACK: InMemory вместо Postgres, чтобы тест не требовал поднятой БД.
            // Цена: не проверяется RowVersion/xmin — для этого нужен Testcontainers с реальным Postgres.
            // IDbContextOptionsConfiguration<T> тоже снимаем: в EF 9 он держит делегат UseNpgsql.
            services.RemoveAll<IDbContextOptionsConfiguration<TaskDbContext>>();
            services.RemoveAll<DbContextOptions<TaskDbContext>>();
            services.RemoveAll<DbContextOptions>();
            services.RemoveAll<TaskDbContext>();
            services.AddDbContext<TaskDbContext>(o => o.UseInMemoryDatabase(_databaseName));

            services.RemoveAll<ITaskEventPublisher>();
            services.RemoveAll<IHostedService>();
            services.AddSingleton<ITaskEventPublisher>(Publisher);
        });
    }
}

#endregion

public class CompleteTaskIntegrationTest : IClassFixture<TaskApiFactory>
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter() }
    };

    private readonly TaskApiFactory _factory;

    public CompleteTaskIntegrationTest(TaskApiFactory factory) => _factory = factory;

    [Fact]
    public async Task Create_then_complete_publishes_task_completed_event()
    {
        var client = _factory.CreateClient();

        // 1. Создаём задачу
        var createResponse = await client.PostAsJsonAsync("/tasks",
            new { title = "Buy milk", priority = "High" }, Json);

        Assert.Equal(HttpStatusCode.Created, createResponse.StatusCode);

        var created = await createResponse.Content.ReadFromJsonAsync<TaskResponse>(Json);
        Assert.NotNull(created);
        Assert.False(created!.IsCompleted);
        Assert.Null(created.CompletedAt); // CompletedAt не заполняется при создании

        // 2. Завершаем
        var completeResponse = await client.PutAsync($"/tasks/{created.Id}/complete", null);
        Assert.Equal(HttpStatusCode.OK, completeResponse.StatusCode);

        var completed = await completeResponse.Content.ReadFromJsonAsync<TaskResponse>(Json);
        Assert.True(completed!.IsCompleted);
        Assert.NotNull(completed.CompletedAt);

        // 3. Событие ушло в publisher
        var @event = Assert.Single(_factory.Publisher.Published, e => e.TaskId == created.Id);
        Assert.Equal("Buy milk", @event.Title);
        Assert.Equal(Priority.High, @event.Priority);
        Assert.Equal(completed.CompletedAt, @event.CompletedAt);

        // 4. Бонус: повторное завершение -> 409
        var secondComplete = await client.PutAsync($"/tasks/{created.Id}/complete", null);
        Assert.Equal(HttpStatusCode.Conflict, secondComplete.StatusCode);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData(null)]
    public async Task Create_with_invalid_title_returns_400(string? title)
    {
        var client = _factory.CreateClient();
        var response = await client.PostAsJsonAsync("/tasks", new { title }, Json);
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }
}
