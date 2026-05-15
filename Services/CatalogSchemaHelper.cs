using MySqlConnector;
using System.Globalization;

namespace MyFinanceApp.Services;

public static class CatalogSchemaHelper
{
    public static async Task EnsureReferenceTablesAndColumnsAsync(
        MySqlConnection connection,
        string inventoryTableName,
        string suppliersTableName = "suppliers",
        string categoriesTableName = "categories")
    {
        string createSuppliersSql = $@"
            CREATE TABLE IF NOT EXISTS `{suppliersTableName}` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                `name` VARCHAR(150) NOT NULL,
                `phone` VARCHAR(50) NULL,
                `email` VARCHAR(150) NULL,
                `contact_person` VARCHAR(150) NULL,
                `address` VARCHAR(255) NULL,
                `note` VARCHAR(255) NULL,
                `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY `ux_{suppliersTableName}_name` (`name`)
            );";

        string createCategoriesSql = $@"
            CREATE TABLE IF NOT EXISTS `{categoriesTableName}` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                `name` VARCHAR(150) NOT NULL,
                `description` VARCHAR(255) NULL,
                `is_active` TINYINT(1) NOT NULL DEFAULT 1,
                `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY `ux_{categoriesTableName}_name` (`name`)
            );";

        using (var cmd = new MySqlCommand(createSuppliersSql, connection))
            await cmd.ExecuteNonQueryAsync();
        using (var cmd = new MySqlCommand(createCategoriesSql, connection))
            await cmd.ExecuteNonQueryAsync();

        var columns = await GetColumnsAsync(connection, inventoryTableName);
        if (!columns.Contains("supplier_id"))
        {
            using var cmd = new MySqlCommand($"ALTER TABLE `{inventoryTableName}` ADD COLUMN `supplier_id` INT NULL;", connection);
            await cmd.ExecuteNonQueryAsync();
            columns.Add("supplier_id");
        }

        if (!columns.Contains("category_id"))
        {
            using var cmd = new MySqlCommand($"ALTER TABLE `{inventoryTableName}` ADD COLUMN `category_id` INT NULL;", connection);
            await cmd.ExecuteNonQueryAsync();
            columns.Add("category_id");
        }

        if (!columns.Contains("min_quantity"))
        {
            using var cmd = new MySqlCommand($"ALTER TABLE `{inventoryTableName}` ADD COLUMN `min_quantity` INT NOT NULL DEFAULT 5;", connection);
            await cmd.ExecuteNonQueryAsync();
        }
    }

    public static async Task<HashSet<string>> GetColumnsAsync(MySqlConnection connection, string tableName)
    {
        const string sql = @"
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = @tableName;";

        using var cmd = new MySqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("@tableName", tableName);

        var columns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            if (!reader.IsDBNull(0))
            {
                var value = reader.GetString(0);
                if (!string.IsNullOrWhiteSpace(value))
                    columns.Add(value);
            }
        }

        return columns;
    }

    public static string? ResolveColumn(IEnumerable<string> columns, params string[] candidates)
    {
        var list = columns.ToList();
        foreach (var candidate in candidates)
        {
            var exact = list.FirstOrDefault(c => string.Equals(c, candidate, StringComparison.OrdinalIgnoreCase));
            if (exact != null) return exact;
        }

        foreach (var candidate in candidates)
        {
            var normalizedCandidate = Normalize(candidate);
            var fuzzy = list.FirstOrDefault(c => Normalize(c).Contains(normalizedCandidate, StringComparison.OrdinalIgnoreCase));
            if (fuzzy != null) return fuzzy;
        }

        return null;
    }

    public static string Normalize(string value)
    {
        return value.Trim().Replace(" ", "").Replace("_", "").Replace("-", "").ToLowerInvariant();
    }

    public static decimal ParseDecimal(object? value)
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

    public static int ParseInt(object? value)
    {
        if (value is null || value == DBNull.Value) return 0;
        if (value is int i) return i;
        if (value is long l) return Convert.ToInt32(l, CultureInfo.InvariantCulture);
        var text = value.ToString();
        return int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) ? parsed : 0;
    }

    public static async Task<List<ReferenceItem>> GetSuppliersAsync(MySqlConnection connection, string suppliersTableName = "suppliers")
    {
        string sql = $@"SELECT id, name, phone, email, contact_person, address, note FROM `{suppliersTableName}` ORDER BY name;";
        using var cmd = new MySqlCommand(sql, connection);
        var list = new List<ReferenceItem>();
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            list.Add(new ReferenceItem
            {
                Id = reader.IsDBNull(reader.GetOrdinal("id")) ? 0 : reader.GetInt32(reader.GetOrdinal("id")),
                Name = reader.IsDBNull(reader.GetOrdinal("name")) ? string.Empty : reader.GetString(reader.GetOrdinal("name")),
                Phone = reader.IsDBNull(reader.GetOrdinal("phone")) ? string.Empty : reader.GetString(reader.GetOrdinal("phone")),
                Email = reader.IsDBNull(reader.GetOrdinal("email")) ? string.Empty : reader.GetString(reader.GetOrdinal("email")),
                ContactPerson = reader.IsDBNull(reader.GetOrdinal("contact_person")) ? string.Empty : reader.GetString(reader.GetOrdinal("contact_person")),
                Address = reader.IsDBNull(reader.GetOrdinal("address")) ? string.Empty : reader.GetString(reader.GetOrdinal("address")),
                Note = reader.IsDBNull(reader.GetOrdinal("note")) ? string.Empty : reader.GetString(reader.GetOrdinal("note"))
            });
        }
        return list;
    }

    public static async Task<List<CategoryItem>> GetCategoriesAsync(MySqlConnection connection, string categoriesTableName = "categories")
    {
        string sql = $@"SELECT id, name, description, is_active FROM `{categoriesTableName}` ORDER BY name;";
        using var cmd = new MySqlCommand(sql, connection);
        var list = new List<CategoryItem>();
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            list.Add(new CategoryItem
            {
                Id = reader.IsDBNull(reader.GetOrdinal("id")) ? 0 : reader.GetInt32(reader.GetOrdinal("id")),
                Name = reader.IsDBNull(reader.GetOrdinal("name")) ? string.Empty : reader.GetString(reader.GetOrdinal("name")),
                Description = reader.IsDBNull(reader.GetOrdinal("description")) ? string.Empty : reader.GetString(reader.GetOrdinal("description")),
                IsActive = ParseInt(reader[reader.GetOrdinal("is_active")]) != 0
            });
        }
        return list;
    }

    public sealed class ReferenceItem
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string Phone { get; set; } = string.Empty;
        public string Email { get; set; } = string.Empty;
        public string ContactPerson { get; set; } = string.Empty;
        public string Address { get; set; } = string.Empty;
        public string Note { get; set; } = string.Empty;
    }

    public sealed class CategoryItem
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string Description { get; set; } = string.Empty;
        public bool IsActive { get; set; }
    }
}
