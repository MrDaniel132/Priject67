using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Authorization;
using MySqlConnector;
using MyFinanceApp.Models;
using System.Data;
using System.Globalization;
using System.ComponentModel.DataAnnotations;
using MyFinanceApp.Services;

namespace MyFinanceApp.Controllers;

[Authorize]
public class DatabaseController : Controller
{
    private readonly IAuditLogService _auditLogService;

    public DatabaseController(IAuditLogService auditLogService)
    {
        _auditLogService = auditLogService;
    }

    [HttpPost]
    public async Task<IActionResult> GetTables([FromBody] DbSettings settings)
    {
        var validationErrors = RequestValidationHelper.ValidateObjectGraph(settings);
        if (validationErrors.Count > 0)
            return Json(new { success = false, message = validationErrors[0], errors = validationErrors });

        var tables = new List<string>();
        try
        {
            await _auditLogService.FlushPendingLoginAsync(settings, HttpContext);
            using var connection = new MySqlConnection(settings.GetConnectionString());
            await connection.OpenAsync();

            using var command = new MySqlCommand("SHOW TABLES", connection);
            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                tables.Add(reader.GetString(0));
            }

            if (tables.Count == 0)
            {
                return Json(new
                {
                    success = false,
                    message = $"В базе данных {settings.Database} не найдено таблиц. Проверь имя базы данных. Для этого проекта обычно используется myfinanceapp_db."
                });
            }

            return Json(new { success = true, tables });
        }
        catch (Exception ex)
        {
            return Json(new { success = false, message = ex.Message });
        }
    }

    [Authorize(Roles = "Администратор")]
    [HttpPost]
    public async Task<IActionResult> UpdateRecord(string tableName, [FromBody] UpdateRequest request)
    {
        var validationErrors = RequestValidationHelper.ValidateObjectGraph(request);
        if (validationErrors.Count > 0)
            return Json(new { success = false, message = validationErrors[0], errors = validationErrors });

        if (string.IsNullOrWhiteSpace(tableName))
            return Json(new { success = false, message = "Не выбрана таблица для редактирования." });

        try
        {
            using var connection = new MySqlConnection(request.Settings.GetConnectionString());
            await connection.OpenAsync();

            var columnMeta = await LoadColumnMetadata(connection, request.Settings.Database, tableName);
            var setClauses = new List<string>();
            using var command = new MySqlCommand { Connection = connection };

            string idColumn = request.Data.ContainsKey("SKU") ? "SKU" : "id";
            if (!request.Data.ContainsKey(idColumn) || string.IsNullOrWhiteSpace(request.Data[idColumn]))
                return Json(new { success = false, message = "Не удалось определить идентификатор записи для сохранения." });

            string idValue = request.Data[idColumn];

            foreach (var item in request.Data)
            {
                if (item.Key == idColumn || item.Key.StartsWith("__resolved", StringComparison.OrdinalIgnoreCase))
                    continue;

                var columnName = item.Key;
                var rawValue = item.Value?.Trim() ?? string.Empty;

                if (!columnMeta.TryGetValue(columnName, out var meta))
                {
                    // На всякий случай пропускаем неизвестные поля, чтобы не ломать сохранение.
                    continue;
                }

                // Системные временные поля лучше не трогать вручную из UI.
                if (string.Equals(columnName, "created_at", StringComparison.OrdinalIgnoreCase)
                    || string.Equals(columnName, "updated_at", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                var parameterName = $"@p_{setClauses.Count}";
                setClauses.Add($"`{columnName}` = {parameterName}");
                command.Parameters.AddWithValue(parameterName, ConvertValueForColumn(rawValue, meta));
            }

            if (setClauses.Count == 0)
                return Json(new { success = false, message = "Нет изменяемых полей для сохранения." });

            command.CommandText = $"UPDATE `{tableName}` SET {string.Join(", ", setClauses)} WHERE `{idColumn}` = @primaryId";
            command.Parameters.AddWithValue("@primaryId", idValue);

            await command.ExecuteNonQueryAsync();

            await _auditLogService.LogAsync(request.Settings, new AuditLogEntry
            {
                Username = User.Identity?.Name ?? "unknown",
                Action = "update_record",
                EntityType = tableName,
                EntityId = idValue,
                Details = $"Изменены поля: {string.Join(", ", setClauses.Select(x => x.Split("=")[0].Trim()))}"
            }, HttpContext);

            return Json(new { success = true });
        }
        catch (Exception ex)
        {
            return Json(new { success = false, message = ex.Message });
        }
    }

    public class UpdateRequest
    {
        [Required(ErrorMessage = "Не переданы настройки подключения к базе данных.")]
        public DbSettings Settings { get; set; } = new();

        [MinLength(1, ErrorMessage = "Нет данных для сохранения.")]
        public Dictionary<string, string> Data { get; set; } = new();
    }

    [HttpPost]
    public async Task<IActionResult> GetTableData(string tableName, [FromBody] DbSettings settings)
    {
        var validationErrors = RequestValidationHelper.ValidateObjectGraph(settings);
        if (validationErrors.Count > 0)
            return Json(new { success = false, message = validationErrors[0], errors = validationErrors });

        if (string.IsNullOrWhiteSpace(tableName))
            return Json(new { success = false, message = "Не выбрана таблица для загрузки." });

        try
        {
            await _auditLogService.FlushPendingLoginAsync(settings, HttpContext);
            await _auditLogService.RememberDbSettingsAsync(HttpContext, settings);
            using var connection = new MySqlConnection(settings.GetConnectionString());
            await connection.OpenAsync();

            var sql = $"SELECT * FROM `{tableName}` LIMIT 100";
            using var adapter = new MySqlDataAdapter(sql, connection);
            var dataTable = new DataTable();
            adapter.Fill(dataTable);

            var columns = dataTable.Columns.Cast<DataColumn>().Select(c => c.ColumnName).ToList();
            string? quantityColumn = ResolveQuantityColumn(dataTable);
            string? priceColumn = ResolvePriceColumn(dataTable, quantityColumn);
            string? partNumberColumn = ResolveTextColumn(dataTable, "part_number", "sku", "article", "арт", "артикул", "code", "код");
            string? partNameColumn = ResolveTextColumn(dataTable, "part_name", "name", "название", "наименование", "товар");
            string? brandColumn = ResolveTextColumn(dataTable, "brand", "manufacturer", "бренд", "марка", "производитель");
            string? carBrandColumn = ResolveTextColumn(dataTable, "car_brand", "make", "vehicle_brand", "марка_авто", "маркаавто");
            string? carModelColumn = ResolveTextColumn(dataTable, "car_model", "model", "vehicle_model", "модель", "автомодель");
            string? categoryNameColumn = ResolveTextColumn(dataTable, "category", "category_name", "group_name", "категория");
            string? supplierNameColumn = ResolveTextColumn(dataTable, "supplier", "supplier_name", "vendor", "поставщик");
            string? categoryIdColumn = ResolveTextColumn(dataTable, "category_id");
            string? supplierIdColumn = ResolveTextColumn(dataTable, "supplier_id");

            var rows = dataTable.AsEnumerable()
                .Select(r =>
                {
                    var dict = columns.ToDictionary(c => c, c => r[c]?.ToString() ?? "");
                    dict["__resolvedPrice"] = priceColumn is null ? "" : r[priceColumn]?.ToString() ?? "";
                    dict["__resolvedQuantity"] = quantityColumn is null ? "" : r[quantityColumn]?.ToString() ?? "";
                    dict["__resolvedPartNumber"] = partNumberColumn is null ? "" : r[partNumberColumn]?.ToString() ?? "";
                    dict["__resolvedPartName"] = partNameColumn is null ? "" : r[partNameColumn]?.ToString() ?? "";
                    dict["__resolvedBrand"] = brandColumn is null ? "" : r[brandColumn]?.ToString() ?? "";
                    dict["__resolvedCarBrand"] = carBrandColumn is null ? "" : r[carBrandColumn]?.ToString() ?? "";
                    dict["__resolvedCarModel"] = carModelColumn is null ? "" : r[carModelColumn]?.ToString() ?? "";
                    dict["__resolvedCategory"] = categoryNameColumn is null ? "" : r[categoryNameColumn]?.ToString() ?? "";
                    dict["__resolvedSupplier"] = supplierNameColumn is null ? "" : r[supplierNameColumn]?.ToString() ?? "";
                    dict["__resolvedCategoryId"] = categoryIdColumn is null ? "" : r[categoryIdColumn]?.ToString() ?? "";
                    dict["__resolvedSupplierId"] = supplierIdColumn is null ? "" : r[supplierIdColumn]?.ToString() ?? "";
                    return dict;
                })
                .ToList();

            int total = rows.Count;
            int inStock = 0;
            int outOfStock = 0;
            decimal totalValue = 0;

            foreach (var row in rows)
            {
                var quantity = ParseDecimal(row.GetValueOrDefault("__resolvedQuantity"));
                var price = ParseDecimal(row.GetValueOrDefault("__resolvedPrice"));
                if (quantity > 0) inStock++; else outOfStock++;
                totalValue += price * quantity;
            }

            return Json(new
            {
                success = true,
                columns,
                rows,
                resolved = new
                {
                    priceColumn,
                    quantityColumn,
                    partNumberColumn,
                    partNameColumn,
                    brandColumn,
                    carBrandColumn,
                    carModelColumn,
                    categoryNameColumn,
                    supplierNameColumn,
                    categoryIdColumn,
                    supplierIdColumn
                },
                stats = new
                {
                    total,
                    inStock,
                    outOfStock,
                    totalValue = totalValue.ToString("N0", CultureInfo.InvariantCulture)
                }
            });
        }
        catch (Exception ex)
        {
            return Json(new { success = false, message = ex.Message });
        }
    }

    private sealed class ColumnMetadata
    {
        public string Name { get; set; } = string.Empty;
        public string DataType { get; set; } = string.Empty;
        public bool IsNullable { get; set; }
    }

    private static async Task<Dictionary<string, ColumnMetadata>> LoadColumnMetadata(MySqlConnection connection, string databaseName, string tableName)
    {
        const string sql = @"SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = @db AND TABLE_NAME = @table";

        using var command = new MySqlCommand(sql, connection);
        command.Parameters.AddWithValue("@db", databaseName);
        command.Parameters.AddWithValue("@table", tableName);
        using var reader = await command.ExecuteReaderAsync();

        var result = new Dictionary<string, ColumnMetadata>(StringComparer.OrdinalIgnoreCase);
        while (await reader.ReadAsync())
        {
            var meta = new ColumnMetadata
            {
                Name = reader.GetString("COLUMN_NAME"),
                DataType = reader.GetString("DATA_TYPE"),
                IsNullable = string.Equals(reader.GetString("IS_NULLABLE"), "YES", StringComparison.OrdinalIgnoreCase)
            };
            result[meta.Name] = meta;
        }

        return result;
    }

    private static object ConvertValueForColumn(string rawValue, ColumnMetadata meta)
    {
        if (string.IsNullOrWhiteSpace(rawValue))
        {
            if (meta.IsNullable)
                return DBNull.Value;

            return meta.DataType.ToLowerInvariant() switch
            {
                "decimal" or "numeric" or "float" or "double" or "real" => 0,
                "tinyint" or "smallint" or "mediumint" or "int" or "integer" or "bigint" => 0,
                _ => string.Empty
            };
        }

        var normalized = rawValue.Trim();
        var dataType = meta.DataType.ToLowerInvariant();

        switch (dataType)
        {
            case "decimal":
            case "numeric":
            case "float":
            case "double":
            case "real":
                normalized = normalized.Replace(" ", string.Empty).Replace(',', '.');
                if (decimal.TryParse(normalized, NumberStyles.Any, CultureInfo.InvariantCulture, out var decimalValue))
                    return decimalValue;
                break;

            case "tinyint":
            case "smallint":
            case "mediumint":
            case "int":
            case "integer":
            case "bigint":
                normalized = normalized.Replace(" ", string.Empty);
                if (long.TryParse(normalized, NumberStyles.Integer, CultureInfo.InvariantCulture, out var intValue))
                    return intValue;
                break;

            case "date":
                if (TryParseDateTime(normalized, out var dateOnlyValue))
                    return dateOnlyValue.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                break;

            case "datetime":
            case "timestamp":
                if (TryParseDateTime(normalized, out var dateTimeValue))
                    return dateTimeValue.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
                break;
        }

        return normalized;
    }

    private static bool TryParseDateTime(string value, out DateTime result)
    {
        var formats = new[]
        {
            "dd.MM.yyyy HH:mm:ss",
            "dd.MM.yyyy HH:mm",
            "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd HH:mm",
            "yyyy-MM-ddTHH:mm:ss",
            "yyyy-MM-ddTHH:mm",
            "dd.MM.yyyy",
            "yyyy-MM-dd"
        };

        return DateTime.TryParseExact(value, formats, CultureInfo.InvariantCulture, DateTimeStyles.None, out result)
            || DateTime.TryParse(value, new CultureInfo("ru-RU"), DateTimeStyles.None, out result)
            || DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.None, out result);
    }

    private static string? ResolveQuantityColumn(DataTable table)
    {
        var exact = FindExactColumn(table, "quantity", "qty", "stock", "остаток", "количество", "кол_во", "кол-во");
        if (exact is not null) return exact;

        var candidates = table.Columns.Cast<DataColumn>()
            .Where(c => IsNumericType(c.DataType))
            .Where(c => !IsIdentifierColumn(c.ColumnName))
            .Select(c => new { c.ColumnName, Score = ScoreQuantityColumnName(c.ColumnName) })
            .OrderByDescending(x => x.Score)
            .ThenBy(x => x.ColumnName.Length)
            .ToList();

        return candidates.FirstOrDefault(x => x.Score > 0)?.ColumnName;
    }

    private static string? ResolvePriceColumn(DataTable table, string? quantityColumn)
    {
        var exact = FindExactColumn(table, "sale_price", "retail_price", "selling_price", "unit_price", "price", "цена", "стоимость", "cost", "sum");
        if (exact is not null) return exact;

        var candidates = table.Columns.Cast<DataColumn>()
            .Where(c => IsNumericType(c.DataType))
            .Where(c => !IsIdentifierColumn(c.ColumnName))
            .Where(c => !string.Equals(c.ColumnName, quantityColumn, StringComparison.OrdinalIgnoreCase))
            .Select(c => new
            {
                c.ColumnName,
                NameScore = ScorePriceColumnName(c.ColumnName),
                PositiveCount = table.AsEnumerable().Count(r => ParseDecimal(r[c]) > 0),
                SampleAverage = table.AsEnumerable().Select(r => ParseDecimal(r[c])).Where(v => v > 0).DefaultIfEmpty().Average()
            })
            .OrderByDescending(x => x.NameScore)
            .ThenByDescending(x => x.PositiveCount)
            .ThenByDescending(x => x.SampleAverage)
            .ToList();

        var best = candidates.FirstOrDefault();
        if (best is null) return null;

        if (best.NameScore > 0 || best.PositiveCount > 0)
            return best.ColumnName;

        return null;
    }

    private static string? ResolveTextColumn(DataTable table, params string[] patterns)
    {
        var exact = FindExactColumn(table, patterns);
        if (exact is not null) return exact;

        return table.Columns.Cast<DataColumn>()
            .Select(c => c.ColumnName)
            .FirstOrDefault(name => patterns.Any(p => Normalize(name).Contains(Normalize(p), StringComparison.OrdinalIgnoreCase)));
    }

    private static string? FindExactColumn(DataTable table, params string[] names)
    {
        return table.Columns.Cast<DataColumn>()
            .Select(c => c.ColumnName)
            .FirstOrDefault(name => names.Any(n => string.Equals(name, n, StringComparison.OrdinalIgnoreCase)));
    }

    private static string Normalize(string value)
    {
        return value.Trim().Replace(" ", "").Replace("_", "").Replace("-", "").ToLowerInvariant();
    }

    private static bool IsIdentifierColumn(string columnName)
    {
        var name = Normalize(columnName);
        return name is "id" or "sku" or "partnumber" or "article" or "код" or "артикул";
    }

    private static int ScoreQuantityColumnName(string columnName)
    {
        var name = Normalize(columnName);
        if (name is "quantity" or "qty" or "stock" or "остаток" or "количество" or "колво") return 100;
        if (name.Contains("stock") || name.Contains("quantity") || name.Contains("qty")) return 80;
        if (name.Contains("остат") || name.Contains("колич") || name.Contains("колво")) return 70;
        return 0;
    }

    private static int ScorePriceColumnName(string columnName)
    {
        var name = Normalize(columnName);
        if (name is "saleprice" or "retailprice" or "sellingprice" or "unitprice" or "price" or "цена" or "стоимость") return 100;
        if (name.Contains("saleprice") || name.Contains("retail") || name.Contains("selling") || name.Contains("unitprice")) return 90;
        if (name.Contains("price") || name.Contains("цена") || name.Contains("стоим") || name.Contains("sum") || name.Contains("amount")) return 75;
        if (name.Contains("cost")) return 50;
        return 0;
    }

    private static bool IsNumericType(Type type)
    {
        type = Nullable.GetUnderlyingType(type) ?? type;
        return type == typeof(byte) || type == typeof(sbyte)
            || type == typeof(short) || type == typeof(ushort)
            || type == typeof(int) || type == typeof(uint)
            || type == typeof(long) || type == typeof(ulong)
            || type == typeof(float) || type == typeof(double)
            || type == typeof(decimal);
    }

    private static decimal ParseDecimal(object? value)
    {
        if (value is null || value == DBNull.Value) return 0;
        if (value is decimal d) return d;
        if (value is int i) return i;
        if (value is long l) return l;
        if (value is double db) return Convert.ToDecimal(db, CultureInfo.InvariantCulture);
        if (value is float f) return Convert.ToDecimal(f, CultureInfo.InvariantCulture);

        var text = value.ToString();
        if (string.IsNullOrWhiteSpace(text)) return 0;

        text = text.Trim().Replace(" ", "").Replace(",", ".");
        return decimal.TryParse(text, NumberStyles.Any, CultureInfo.InvariantCulture, out var parsed) ? parsed : 0;
    }
}
