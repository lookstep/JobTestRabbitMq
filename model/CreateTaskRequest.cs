namespace TaskApi.Model;

public record CreateTaskRequest(string? Title, Priority? Priority = null);
