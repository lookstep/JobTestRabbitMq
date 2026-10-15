using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Diagnostics;
using Microsoft.EntityFrameworkCore;
using Microsoft.OpenApi;
using TaskApi;
using TaskApi.Model;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddDbContext<TaskDbContext>(options =>
    options.UseNpgsql(builder.Configuration.GetConnectionString("DefaultConnection")));
builder.Services.ConfigureHttpJsonOptions(options =>
    options.SerializerOptions.Converters.Add(new JsonStringEnumConverter<Priority>(allowIntegerValues: false)));
builder.Services.Configure<RouteHandlerOptions>(options => options.ThrowOnBadRequest = true);
builder.Services.Configure<HostOptions>(options => options.ShutdownTimeout = TimeSpan.FromSeconds(5));

builder.Services.AddSingleton<RabbitMqService>();
builder.Services.AddHostedService(services => services.GetRequiredService<RabbitMqService>());
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(options =>
    options.SwaggerDoc("v1", new OpenApiInfo { Title = "Task API", Version = "v1" }));

var app = builder.Build();

app.UseExceptionHandler(handler => handler.Run(async context =>
{
    var error = context.Features.Get<IExceptionHandlerFeature>()?.Error;
    context.Response.StatusCode = error is BadHttpRequestException badRequest
        ? badRequest.StatusCode
        : StatusCodes.Status500InternalServerError;

    var message = context.Response.StatusCode switch
    {
        400 => "Некорректный запрос. Проверьте JSON, идентификатор и приоритет задачи.",
        415 => "Неподдерживаемый формат запроса. Используйте application/json.",
        _ => "Не удалось обработать запрос."
    };
    await context.Response.WriteAsJsonAsync(new { error = message });
}));

app.UseStatusCodePages(async context =>
{
    var message = context.HttpContext.Response.StatusCode switch
    {
        404 => "Запрошенный адрес не найден.",
        405 => "Метод запроса не поддерживается.",
        _ => "Не удалось обработать запрос."
    };
    await context.HttpContext.Response.WriteAsJsonAsync(new { error = message });
});

app.UseSwagger();
app.UseSwaggerUI();

app.MapPost("/tasks", async (CreateTaskRequest request, TaskDbContext db, CancellationToken ct) =>
{
    if (string.IsNullOrWhiteSpace(request.Title) || request.Title.Length > 200)
        return Results.BadRequest(new { error = "Название задачи должно содержать от 1 до 200 символов и не состоять из пробелов." });

    if (request.Priority.HasValue && !Enum.IsDefined(request.Priority.Value))
        return Results.BadRequest(new { error = "Указан неизвестный приоритет задачи." });

    var task = new TaskItem
    {
        Title = request.Title.Trim(),
        Priority = request.Priority ?? Priority.Medium
    };

    db.Tasks.Add(task);
    await db.SaveChangesAsync(ct);
    return Results.Created($"/tasks/{task.Id}", task);
}).WithName("CreateTask").Produces<TaskItem>(201).Produces(400);

app.MapGet("/tasks", async (TaskDbContext db, CancellationToken ct) =>
    await db.Tasks.AsNoTracking().ToListAsync(ct))
    .WithName("GetTasks");

app.MapPut("/tasks/{id}/complete", async (Guid id, TaskDbContext db, RabbitMqService publisher, CancellationToken ct) =>
{
    var task = await db.Tasks.SingleOrDefaultAsync(x => x.Id == id, ct);
    if (task is null)
        return Results.NotFound(new { error = "Задача не найдена." });

    if (!task.TryComplete(DateTimeOffset.UtcNow))
        return Results.Conflict(new { error = "Задача уже завершена." });

    try
    {
        await db.SaveChangesAsync(ct);
    }
    catch (DbUpdateConcurrencyException)
    {
        return Results.Conflict(new { error = "Задача уже изменена другим запросом." });
    }

    publisher.Publish(new TaskCompletedEvent(task.Id, task.Title, task.CompletedAt!.Value, task.Priority));
    return Results.Ok(task);
}).WithName("CompleteTask").Produces<TaskItem>().Produces(400).Produces(404).Produces(409);

app.MapDelete("/tasks/{id}", async (Guid id, TaskDbContext db, CancellationToken ct) =>
{
    var task = await db.Tasks.SingleOrDefaultAsync(x => x.Id == id, ct);
    if (task is null)
        return Results.NotFound(new { error = "Задача не найдена." });

    db.Tasks.Remove(task);
    try
    {
        await db.SaveChangesAsync(ct);
    }
    catch (DbUpdateConcurrencyException)
    {
        return Results.Conflict(new { error = "Задача уже изменена другим запросом." });
    }

    return Results.NoContent();
}).WithName("DeleteTask").Produces(204).Produces(400).Produces(404).Produces(409);

// HACK: для локального запуска применяем миграции здесь. На сервере вынес бы в отдельный шаг развёртывания.
await using (var scope = app.Services.CreateAsyncScope())
{
    var db = scope.ServiceProvider.GetRequiredService<TaskDbContext>();
    await db.Database.MigrateAsync();
}

app.Run();

public partial class Program;
