namespace TaskApi;

public enum Priority
{
    Low = 0,
    Medium = 1,
    High = 2
}

public class TaskItem
{
    public Guid Id { get; set; }

    public string Title { get; set; } = string.Empty;

    public bool IsCompleted { get; set; }

    public DateTimeOffset CreatedAt { get; set; }

    /// <summary>Заполняется ТОЛЬКО в момент завершения задачи (см. Program.cs, CompleteTask).</summary>
    public DateTimeOffset? CompletedAt { get; set; }

    public Priority Priority { get; set; } = Priority.Medium;
}

#region DTO / contracts

public record CreateTaskRequest(string? Title, Priority? Priority);

public record TaskResponse(
    Guid Id,
    string Title,
    bool IsCompleted,
    DateTimeOffset CreatedAt,
    DateTimeOffset? CompletedAt,
    Priority Priority)
{
    public static TaskResponse From(TaskItem t) =>
        new(t.Id, t.Title, t.IsCompleted, t.CreatedAt, t.CompletedAt, t.Priority);
}

#endregion
