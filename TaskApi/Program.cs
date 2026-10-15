using System.Text.Json.Serialization;
using Microsoft.EntityFrameworkCore;
using TaskApi;

var builder = WebApplication.CreateBuilder(args);

#region DI

builder.Services.AddDbContext<TaskDbContext>(o =>
    o.UseNpgsql(builder.Configuration.GetConnectionString("Postgres")));

builder.Services.Configure<RabbitMqOptions>(builder.Configuration.GetSection("RabbitMq"));
builder.Services.AddSingleton<RabbitMqPublisher>();
builder.Services.AddSingleton<ITaskEventPublisher>(sp => sp.GetRequiredService<RabbitMqPublisher>());
builder.Services.AddHostedService(sp => sp.GetRequiredService<RabbitMqPublisher>());

builder.Services.ConfigureHttpJsonOptions(o =>
    o.SerializerOptions.Converters.Add(new JsonStringEnumConverter()));

#endregion

var app = builder.Build();

#region Database bootstrap

// HACK: EnsureCreated вместо миграций — чтобы проект поднимался одной командой.
// В реальном проекте: dotnet ef migrations add Init + db.Database.Migrate().
using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<TaskDbContext>();
    await db.Database.EnsureCreatedAsync();
}

#endregion

#region POST /tasks

app.MapPost("/tasks", async (CreateTaskRequest request, TaskDbContext db, CancellationToken ct) =>
{
    if (!TryValidateTitle(request.Title, out var error))
    {
        return Results.ValidationProblem(new Dictionary<string, string[]>
        {
            ["title"] = [error]
        });
    }

    if (request.Priority is { } p && !Enum.IsDefined(p))
    {
        return Results.ValidationProblem(new Dictionary<string, string[]>
        {
            ["priority"] = ["Недопустимое значение приоритета."]
        });
    }

    var task = new TaskItem
    {
        Id = Guid.NewGuid(),
        Title = request.Title!.Trim(),
        IsCompleted = false,
        CreatedAt = DateTimeOffset.UtcNow,
        CompletedAt = null, // заполняется только при завершении
        Priority = request.Priority ?? Priority.Medium
    };

    db.Tasks.Add(task);
    await db.SaveChangesAsync(ct);

    return Results.Created($"/tasks/{task.Id}", TaskResponse.From(task));
});

static bool TryValidateTitle(string? title, out string error)
{
    if (string.IsNullOrWhiteSpace(title))
    {
        error = "Title обязателен и не может быть пустым.";
        return false;
    }

    if (title.Trim().Length > 200)
    {
        error = "Title не может быть длиннее 200 символов.";
        return false;
    }

    error = string.Empty;
    return true;
}

#endregion

#region GET /tasks

app.MapGet("/tasks", async (TaskDbContext db, CancellationToken ct) =>
{
    var tasks = await db.Tasks
        .AsNoTracking()
        .OrderByDescending(t => t.CreatedAt)
        .Select(t => TaskResponse.From(t))
        .ToListAsync(ct);

    return Results.Ok(tasks);
});

#endregion

#region PUT /tasks/{id}/complete

app.MapPut("/tasks/{id:guid}/complete", async (
    Guid id,
    TaskDbContext db,
    ITaskEventPublisher publisher,
    CancellationToken ct) =>
{
    var task = await db.Tasks.FirstOrDefaultAsync(t => t.Id == id, ct);
    if (task is null) return Results.NotFound();

    // Быстрый путь: задача уже завершена (последовательный повтор).
    if (task.IsCompleted)
        return Results.Conflict(new { message = "Задача уже завершена." });

    task.IsCompleted = true;
    task.CompletedAt = DateTimeOffset.UtcNow;

    try
    {
        // Сначала коммит в БД, только потом событие — иначе при падении БД уйдёт сообщение о том, чего не было.
        await db.SaveChangesAsync(ct);
    }
    catch (DbUpdateConcurrencyException)
    {
        // Гонка: кто-то обновил строку между нашим SELECT и UPDATE (xmin не совпал).
        return Results.Conflict(new { message = "Задача уже завершена (конкурентное обновление)." });
    }

    publisher.Publish(new TaskCompletedEvent(task.Id, task.Title, task.CompletedAt!.Value, task.Priority));

    return Results.Ok(TaskResponse.From(task));
});

#endregion

#region DELETE /tasks/{id}

app.MapDelete("/tasks/{id:guid}", async (Guid id, TaskDbContext db, CancellationToken ct) =>
{
    var deleted = await db.Tasks.Where(t => t.Id == id).ExecuteDeleteAsync(ct);
    return deleted == 0 ? Results.NotFound() : Results.NoContent();
});

#endregion

app.Run();

/// <summary>Нужен для WebApplicationFactory в интеграционном тесте.</summary>
public partial class Program;
