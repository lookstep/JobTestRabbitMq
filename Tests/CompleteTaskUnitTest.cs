using Xunit;
using TaskApi.Model;

namespace TaskApi.Tests;

public class CompleteTaskUnitTest
{
    [Fact]
    public void TryComplete_WhenAlreadyCompleted_DoesNotChangeCompletedAt()
    {
        var task = new TaskItem { Title = "Проверить завершение" };
        var completedAt = new DateTimeOffset(2026, 10, 15, 9, 0, 0, TimeSpan.Zero);

        Assert.Null(task.CompletedAt);
        Assert.True(task.TryComplete(completedAt));
        Assert.False(task.TryComplete(completedAt.AddMinutes(1)));

        Assert.True(task.IsCompleted);
        Assert.Equal(completedAt, task.CompletedAt);
    }
}
