using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using MyFinanceApp.Models;
using MyFinanceApp.Services;
using MySqlConnector;

namespace MyFinanceApp.Controllers;

[Authorize(Roles = "Администратор")]
public class AuditController : Controller
{
    private readonly IAuditLogService _auditLogService;

    public AuditController(IAuditLogService auditLogService)
    {
        _auditLogService = auditLogService;
    }

    public IActionResult Index() => View();

    [HttpPost]
    public async Task<IActionResult> GetLogs([FromBody] DbSettings settings)
    {
        var validationErrors = RequestValidationHelper.ValidateObjectGraph(settings);
        if (validationErrors.Count > 0)
            return Json(new { success = false, message = validationErrors[0], errors = validationErrors });

        try
        {
            await _auditLogService.FlushPendingLoginAsync(settings, HttpContext);

            using var connection = new MySqlConnection(settings.GetConnectionString());
            await connection.OpenAsync();
            await _auditLogService.EnsureAuditTableAsync(connection);

            const string sql = """
                SELECT id, username, action, entity_type, entity_id, details, created_at
                FROM audit_log
                ORDER BY created_at DESC, id DESC
                LIMIT 200;
                """;

            using var cmd = new MySqlCommand(sql, connection);
            var items = new List<object>();
            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                items.Add(new
                {
                    id = reader.GetInt32("id"),
                    username = reader.GetString("username"),
                    action = reader.GetString("action"),
                    entityType = reader.GetString("entity_type"),
                    entityId = reader["entity_id"]?.ToString() ?? "",
                    details = reader["details"]?.ToString() ?? "",
                    createdAt = Convert.ToDateTime(reader["created_at"]).ToString("dd.MM.yyyy HH:mm:ss")
                });
            }

            return Json(new { success = true, items });
        }
        catch (Exception ex)
        {
            return Json(new { success = false, message = ex.Message });
        }
    }
}
