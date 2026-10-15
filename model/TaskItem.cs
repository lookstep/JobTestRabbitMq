using System.Text.Json.Serialization;

namespace TaskApi.Model;

public class TaskItem
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public string Title { get; set; } = string.Empty;
    public bool IsCompleted { get; set; }
    public DateTimeOffset CreatedAt { get; set; } = DateTimeOffset.UtcNow;
    public DateTimeOffset? CompletedAt { get; set; }
    public Priority Priority { get; set; } = Priority.Medium;

    [JsonIgnore]
    public uint RowVersion { get; set; }

    public bool TryComplete(DateTimeOffset completedAt)
    {
        if (IsCompleted)
            return false;

        IsCompleted = true;
        CompletedAt = completedAt.ToUniversalTime();
        return true;
    }
}
