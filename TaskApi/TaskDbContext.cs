using Microsoft.EntityFrameworkCore;

namespace TaskApi;

public class TaskDbContext(DbContextOptions<TaskDbContext> options) : DbContext(options)
{
    public DbSet<TaskItem> Tasks => Set<TaskItem>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        var task = modelBuilder.Entity<TaskItem>();

        task.HasKey(t => t.Id);
        task.Property(t => t.Title).IsRequired().HasMaxLength(200);
        task.Property(t => t.Priority).HasConversion<string>().HasMaxLength(16);

        // Оптимистичная блокировка. В Postgres не нужен отдельный столбец:
        // системный xmin меняется при каждом UPDATE строки, EF использует его как RowVersion.
        // Два параллельных /complete -> второй словит DbUpdateConcurrencyException -> 409.
        // UseXminAsConcurrencyToken() выпилили в Npgsql 9, uint + IsRowVersion() делает то же самое.
        if (Database.IsNpgsql())
        {
            task.Property<uint>("xmin")
                .HasColumnName("xmin")
                .IsRowVersion();
        }
        // HACK: guard на провайдер — чтобы интеграционный тест мог подняться на InMemory.
        // В проде это Npgsql всегда. Правильнее было бы гонять тест на Testcontainers c реальным Postgres.
    }
}
