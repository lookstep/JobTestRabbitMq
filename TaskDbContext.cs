using Microsoft.EntityFrameworkCore;
using TaskApi.Model;

namespace TaskApi;

public class TaskDbContext(DbContextOptions<TaskDbContext> options) : DbContext(options)
{
    public DbSet<TaskItem> Tasks => Set<TaskItem>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        var task = modelBuilder.Entity<TaskItem>();
        task.HasKey(x => x.Id);
        task.Property(x => x.Title).IsRequired().HasMaxLength(200);
        task.Property(x => x.Priority).HasConversion<string>().HasMaxLength(10);
        task.Property(x => x.RowVersion).IsRowVersion();
    }
}
