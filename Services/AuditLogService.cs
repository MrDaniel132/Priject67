using System.Text.Json;
using Microsoft.AspNetCore.Http;
using MyFinanceApp.Models;
using MySqlConnector;

namespace MyFinanceApp.Services;

public sealed class AuditLogEntry
{
    public string Username { get; set; } = "";
    public string Action { get; set; } = "";
    public string EntityType { get; set; } = "";
    public string? EntityId { get; set; }
    public string? Details { get; set; }
}

public interface IAuditLogService
{
    Task EnsureAuditTableAsync(MySqlConnection connection);
    Task LogAsync(DbSettings settings, AuditLogEntry entry, HttpContext? httpContext = null);
    Task RememberDbSettingsAsync(HttpContext httpContext, DbSettings settings);
    Task FlushPendingLoginAsync(DbSettings settings, HttpContext httpContext);
    Task LogLogoutIfPossibleAsync(HttpContext httpContext, string username);
}

public sealed class AuditLogService : IAuditLogService
{
    private const string LastDbSettingsKey = "audit:last-db-settings";
    private const string PendingLoginKey = "audit:pending-login";

    public async Task EnsureAuditTableAsync(MySqlConnection connection)
    {
        const string sql = @"
            CREATE TABLE IF NOT EXISTS `audit_log` (
                `id` INT NOT NULL AUTO_INCREMENT,
                `username` VARCHAR(100) NOT NULL,
                `action` VARCHAR(100) NOT NULL,
                `entity_type` VARCHAR(100) NOT NULL,
                `entity_id` VARCHAR(255) NULL,
                `details` TEXT NULL,
                `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (`id`),
                INDEX `ix_audit_log_created_at` (`created_at`),
                INDEX `ix_audit_log_username` (`username`),
                INDEX `ix_audit_log_action` (`action`)
            );";

        await using var cmd = new MySqlCommand(sql, connection);
        await cmd.ExecuteNonQueryAsync();
    }

    public async Task LogAsync(DbSettings settings, AuditLogEntry entry, HttpContext? httpContext = null)
    {
        await using var connection = new MySqlConnection(settings.GetConnectionString());
        await connection.OpenAsync();
        await EnsureAuditTableAsync(connection);

        const string sql = @"
            INSERT INTO `audit_log` (`username`, `action`, `entity_type`, `entity_id`, `details`)
            VALUES (@username, @action, @entityType, @entityId, @details);";

        await using var cmd = new MySqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("@username", entry.Username);
        cmd.Parameters.AddWithValue("@action", entry.Action);
        cmd.Parameters.AddWithValue("@entityType", entry.EntityType);
        cmd.Parameters.AddWithValue("@entityId", string.IsNullOrWhiteSpace(entry.EntityId) ? DBNull.Value : entry.EntityId);
        cmd.Parameters.AddWithValue("@details", string.IsNullOrWhiteSpace(entry.Details) ? DBNull.Value : entry.Details);
        await cmd.ExecuteNonQueryAsync();

        if (httpContext is not null)
        {
            await RememberDbSettingsAsync(httpContext, settings);
        }
    }

    public Task RememberDbSettingsAsync(HttpContext httpContext, DbSettings settings)
    {
        httpContext.Session.SetString(LastDbSettingsKey, JsonSerializer.Serialize(settings));
        return Task.CompletedTask;
    }

    public async Task FlushPendingLoginAsync(DbSettings settings, HttpContext httpContext)
    {
        var pending = httpContext.Session.GetString(PendingLoginKey);
        if (!string.Equals(pending, "1", StringComparison.Ordinal))
        {
            await RememberDbSettingsAsync(httpContext, settings);
            return;
        }

        var username = httpContext.User.Identity?.Name ?? "unknown";
        var role = httpContext.User.GetUserRole();

        await LogAsync(settings, new AuditLogEntry
        {
            Username = username,
            Action = "login",
            EntityType = "auth",
            EntityId = username,
            Details = $"Вход в систему. Роль: {role}"
        }, httpContext);

        httpContext.Session.Remove(PendingLoginKey);
    }

    public async Task LogLogoutIfPossibleAsync(HttpContext httpContext, string username)
    {
        var rawSettings = httpContext.Session.GetString(LastDbSettingsKey);
        if (string.IsNullOrWhiteSpace(rawSettings))
            return;

        DbSettings? settings = null;
        try
        {
            settings = JsonSerializer.Deserialize<DbSettings>(rawSettings);
        }
        catch
        {
            settings = null;
        }

        if (settings is null)
            return;

        await LogAsync(settings, new AuditLogEntry
        {
            Username = username,
            Action = "logout",
            EntityType = "auth",
            EntityId = username,
            Details = "Выход из системы"
        }, httpContext);
    }

    public static void MarkLoginPending(HttpContext httpContext)
    {
        httpContext.Session.SetString(PendingLoginKey, "1");
    }
}
