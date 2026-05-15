using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Authorization;
using MyFinanceApp.Models;
using MyFinanceApp.Services;
using MySqlConnector;
using System.Globalization;

namespace MyFinanceApp.Controllers;

[Authorize(Roles = "Администратор,Кладовщик")]
public class InventoryController : Controller
{
    private readonly IAuditLogService _auditLogService;

    public InventoryController(IAuditLogService auditLogService)
    {
        _auditLogService = auditLogService;
    }





    [HttpPost]
    public async Task<IActionResult> GetProductCard([FromBody] ProductCardRequest request)
    {
        var validationErrors = RequestValidationHelper.ValidateObjectGraph(request);
        if (validationErrors.Count > 0)
            return Json(new { success = false, message = validationErrors[0], errors = validationErrors });

        if (request.Settings == null)
            return Json(new { success = false, message = "Не переданы настройки подключения к базе данных." });

        try
        {
            await _auditLogService.RememberDbSettingsAsync(HttpContext, request.Settings);
            using var connection = new MySqlConnection(request.Settings.GetConnectionString());
            await connection.OpenAsync();

            var metadata = await GetInventoryTableMetadataAsync(connection, request.InventoryTableName);
            await EnsureInventorySupportTablesAsync(connection, "purchases", "purchase_items", "inventory_movements", "sales_returns", "sales_return_items");

            var product = await GetInventoryRecordAsync(connection, request.InventoryTableName, metadata, request.PartId, request.PartNumber);
            if (product == null)
                return Json(new { success = false, message = "Товар не найден." });

            var fields = await GetInventoryFieldsAsync(connection, request.InventoryTableName, metadata, product);
            var movements = await GetProductMovementsAsync(connection, "inventory_movements", product, request.MovementsLimit > 0 ? request.MovementsLimit : 20);
            var totals = await GetProductTotalsAsync(connection, product);

            int minQuantity = product.MinQuantity > 0 ? product.MinQuantity : 5;
            bool isLowStock = product.Quantity <= minQuantity;
            decimal stockValue = product.Quantity * product.SalePrice;

            return Json(new
            {
                success = true,
                product = new
                {
                    product.PartId,
                    product.PartNumber,
                    product.PartName,
                    product.Brand,
                    product.CarBrand,
                    product.CarModel,
                    quantity = product.Quantity,
                    minQuantity,
                    salePrice = product.SalePrice,
                    purchasePrice = product.PurchasePrice,
                    isLowStock,
                    stockValue
                },
                fields,
                movements,
                totals
            });
        }
        catch (Exception ex)
        {
            return Json(new { success = false, message = ex.Message });
        }
    }

    [HttpPost]
    public async Task<IActionResult> GetPurchases([FromBody] PurchaseHistoryRequest request)
    {
        var validationErrors = RequestValidationHelper.ValidateObjectGraph(request);
        if (validationErrors.Count > 0)
            return Json(new { success = false, message = validationErrors[0], errors = validationErrors });

        if (request.Settings == null)
            return Json(new { success = false, message = "Не переданы настройки подключения к базе данных." });

        try
        {
            using var connection = new MySqlConnection(request.Settings.GetConnectionString());
            await connection.OpenAsync();
            await EnsureInventorySupportTablesAsync(connection, request.PurchasesTableName, request.PurchaseItemsTableName, "inventory_movements", "sales_returns", "sales_return_items");

            string filter = request.Search?.Trim() ?? string.Empty;
            int limit = request.Limit is > 0 and <= 200 ? request.Limit.Value : 50;

            string sql = $@"
                SELECT p.id,
                       p.purchase_date,
                       p.supplier_name,
                       p.supplier_phone,
                       p.document_number,
                       p.note,
                       p.total_amount,
                       p.status,
                       COALESCE(SUM(pi.quantity), 0) AS total_quantity,
                       COUNT(pi.id) AS positions
                FROM `{request.PurchasesTableName}` p
                LEFT JOIN `{request.PurchaseItemsTableName}` pi ON pi.purchase_id = p.id
                WHERE (@search = '' OR p.supplier_name LIKE CONCAT('%', @search, '%')
                    OR p.supplier_phone LIKE CONCAT('%', @search, '%')
                    OR p.document_number LIKE CONCAT('%', @search, '%')
                    OR CAST(p.id AS CHAR) LIKE CONCAT('%', @search, '%'))
                GROUP BY p.id, p.purchase_date, p.supplier_name, p.supplier_phone, p.document_number, p.note, p.total_amount, p.status
                ORDER BY p.purchase_date DESC, p.id DESC
                LIMIT @limit;";

            using var cmd = new MySqlCommand(sql, connection);
            cmd.Parameters.AddWithValue("@search", filter);
            cmd.Parameters.AddWithValue("@limit", limit);

            var purchases = new List<object>();
            decimal totalAmount = 0;
            int totalPositions = 0;
            int totalItems = 0;

            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                int positions = SafeGetInt(reader, "positions");
                int totalQuantity = SafeGetInt(reader, "total_quantity");
                decimal amount = SafeGetDecimal(reader, "total_amount");
                totalAmount += amount;
                totalPositions += positions;
                totalItems += totalQuantity;

                purchases.Add(new
                {
                    id = SafeGetInt(reader, "id"),
                    purchaseDate = SafeGetDateTime(reader, "purchase_date")?.ToString("dd.MM.yyyy HH:mm", CultureInfo.InvariantCulture) ?? "—",
                    supplierName = SafeGetString(reader, "supplier_name"),
                    supplierPhone = SafeGetString(reader, "supplier_phone"),
                    documentNumber = SafeGetString(reader, "document_number"),
                    note = SafeGetString(reader, "note"),
                    status = SafeGetString(reader, "status"),
                    totalAmount = amount,
                    totalQuantity,
                    positions
                });
            }

            return Json(new
            {
                success = true,
                purchases,
                summary = new
                {
                    count = purchases.Count,
                    totalAmount,
                    totalPositions,
                    totalItems
                }
            });
        }
        catch (Exception ex)
        {
            return Json(new { success = false, message = ex.Message });
        }
    }

    [HttpPost]
    public async Task<IActionResult> CreatePurchase([FromBody] PurchaseCreateRequest request)
    {
        var validationErrors = RequestValidationHelper.ValidateObjectGraph(request);
        if (validationErrors.Count > 0)
            return Json(new { success = false, message = validationErrors[0], errors = validationErrors });

        if (request.Settings == null)
            return Json(new { success = false, message = "Не переданы настройки подключения к базе данных." });

        if (string.IsNullOrWhiteSpace(request.InventoryTableName))
            return Json(new { success = false, message = "Не выбрана таблица товаров." });

        if (string.IsNullOrWhiteSpace(request.SupplierName))
            return Json(new { success = false, message = "Укажи поставщика." });

        if (request.Items == null || request.Items.Count == 0)
            return Json(new { success = false, message = "Добавь хотя бы одну позицию в закупку." });

        if (request.Items.Any(i => i.Quantity <= 0 || i.PurchasePrice < 0))
            return Json(new { success = false, message = "Количество и цена закупки должны быть корректными." });

        try
        {
            await _auditLogService.FlushPendingLoginAsync(request.Settings, HttpContext);
            using var connection = new MySqlConnection(request.Settings.GetConnectionString());
            await connection.OpenAsync();
            await EnsureInventorySupportTablesAsync(connection, request.PurchasesTableName, request.PurchaseItemsTableName, request.MovementsTableName, "sales_returns", "sales_return_items");

            var metadata = await GetInventoryTableMetadataAsync(connection, request.InventoryTableName);
            var inventoryRecords = new List<InventoryRecord>();

            foreach (var item in request.Items)
            {
                var record = await GetInventoryRecordAsync(connection, request.InventoryTableName, metadata, item.PartId, item.PartNumber);
                if (record == null)
                    return Json(new { success = false, message = $"Товар '{item.PartName}' не найден в таблице {request.InventoryTableName}." });

                inventoryRecords.Add(record);
            }

            decimal totalAmount = request.Items.Sum(i => i.PurchasePrice * i.Quantity);
            int purchaseId = await InsertPurchaseAsync(connection, request, totalAmount);

            for (int index = 0; index < request.Items.Count; index++)
            {
                var item = request.Items[index];
                var record = inventoryRecords[index];
                decimal lineTotal = item.PurchasePrice * item.Quantity;

                await InsertPurchaseItemAsync(connection, request, purchaseId, item, record, lineTotal);
                await UpdateInventoryAfterPurchaseAsync(connection, request.InventoryTableName, metadata, record, item.Quantity, item.PurchasePrice);
                await InsertInventoryMovementAsync(connection, request.MovementsTableName, "purchase", record, item.Quantity, item.PurchasePrice, "purchase", purchaseId, $"Поступление от поставщика {request.SupplierName}");
            }

            await _auditLogService.LogAsync(request.Settings, new AuditLogEntry
            {
                Username = User.Identity?.Name ?? "unknown",
                Action = "create_purchase",
                EntityType = request.PurchasesTableName,
                EntityId = purchaseId.ToString(CultureInfo.InvariantCulture),
                Details = $"Оформлена закупка на сумму {totalAmount.ToString(CultureInfo.InvariantCulture)}; позиций: {request.Items.Count}; поставщик: {request.SupplierName}"
            }, HttpContext);

            return Json(new { success = true, purchaseId, message = "Закупка успешно сохранена." });
        }
        catch (Exception ex)
        {
            return Json(new { success = false, message = ex.Message });
        }
    }

    [HttpPost]
    public async Task<IActionResult> CreateReturn([FromBody] ReturnCreateRequest request)
    {
        var validationErrors = RequestValidationHelper.ValidateObjectGraph(request);
        if (validationErrors.Count > 0)
            return Json(new { success = false, message = validationErrors[0], errors = validationErrors });

        if (request.Settings == null)
            return Json(new { success = false, message = "Не переданы настройки подключения к базе данных." });

        if (request.SaleId <= 0)
            return Json(new { success = false, message = "Не выбран заказ для возврата." });

        if (request.Items == null || request.Items.Count == 0)
            return Json(new { success = false, message = "Не выбраны позиции для возврата." });

        if (request.Items.Any(i => i.Quantity <= 0))
            return Json(new { success = false, message = "Количество к возврату должно быть больше нуля." });

        try
        {
            await _auditLogService.FlushPendingLoginAsync(request.Settings, HttpContext);
            using var connection = new MySqlConnection(request.Settings.GetConnectionString());
            await connection.OpenAsync();
            await EnsureInventorySupportTablesAsync(connection, "purchases", "purchase_items", request.MovementsTableName, request.SalesReturnsTableName, request.SalesReturnItemsTableName);
            await EnsureSalesTablesExistAsync(connection, request.SalesTableName, request.SaleItemsTableName);

            var metadata = await GetInventoryTableMetadataAsync(connection, request.InventoryTableName);
            var saleItemsToReturn = new List<SaleItemForReturn>();

            foreach (var item in request.Items)
            {
                var saleItem = await GetSaleItemForReturnAsync(connection, request, item.SaleItemId);
                if (saleItem == null)
                    return Json(new { success = false, message = "Одна из позиций заказа не найдена." });

                int available = saleItem.Quantity - saleItem.ReturnedQuantity;
                if (item.Quantity > available)
                    return Json(new { success = false, message = $"Для товара '{saleItem.PartName}' доступно к возврату только {available} шт." });

                saleItem.ReturnQuantity = item.Quantity;
                saleItemsToReturn.Add(saleItem);
            }

            decimal totalAmount = saleItemsToReturn.Sum(i => i.SalePrice * i.ReturnQuantity);
            int returnId = await InsertReturnAsync(connection, request, totalAmount);

            foreach (var saleItem in saleItemsToReturn)
            {
                await InsertReturnItemAsync(connection, request, returnId, saleItem);

                var inventoryRecord = await GetInventoryRecordAsync(connection, request.InventoryTableName, metadata,
                    saleItem.PartId?.ToString(CultureInfo.InvariantCulture), saleItem.PartNumber);

                if (inventoryRecord != null)
                {
                    await UpdateInventoryAfterReturnAsync(connection, request.InventoryTableName, metadata, inventoryRecord, saleItem.ReturnQuantity);
                    await InsertInventoryMovementAsync(connection, request.MovementsTableName, "return", inventoryRecord, saleItem.ReturnQuantity, saleItem.SalePrice, "sales_return", returnId, $"Возврат по заказу #{request.SaleId}");
                }
            }

            await _auditLogService.LogAsync(request.Settings, new AuditLogEntry
            {
                Username = User.Identity?.Name ?? "unknown",
                Action = "create_return",
                EntityType = request.SalesReturnsTableName,
                EntityId = returnId.ToString(CultureInfo.InvariantCulture),
                Details = $"Оформлен возврат по продаже #{request.SaleId}; позиций: {request.Items.Count}; причина: {request.Reason}"
            }, HttpContext);

            return Json(new { success = true, returnId, message = "Возврат успешно оформлен." });
        }
        catch (Exception ex)
        {
            return Json(new { success = false, message = ex.Message });
        }
    }

    private static async Task<int> InsertPurchaseAsync(MySqlConnection connection, PurchaseCreateRequest request, decimal totalAmount)
    {
        string sql = $@"
            INSERT INTO `{request.PurchasesTableName}`
            (purchase_date, supplier_name, supplier_phone, document_number, note, total_amount, status)
            VALUES (@purchase_date, @supplier_name, @supplier_phone, @document_number, @note, @total_amount, @status);";

        using var cmd = new MySqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("@purchase_date", DateTime.Now);
        cmd.Parameters.AddWithValue("@supplier_name", request.SupplierName);
        cmd.Parameters.AddWithValue("@supplier_phone", (object?)request.SupplierPhone ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@document_number", (object?)request.DocumentNumber ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@note", (object?)request.Note ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@total_amount", totalAmount);
        cmd.Parameters.AddWithValue("@status", request.Status);
        await cmd.ExecuteNonQueryAsync();
        return Convert.ToInt32(cmd.LastInsertedId, CultureInfo.InvariantCulture);
    }

    private static async Task InsertPurchaseItemAsync(MySqlConnection connection, PurchaseCreateRequest request, int purchaseId, PurchaseItemRequest item, InventoryRecord record, decimal lineTotal)
    {
        string sql = $@"
            INSERT INTO `{request.PurchaseItemsTableName}`
            (purchase_id, part_id, part_number, part_name, quantity, purchase_price, line_total)
            VALUES (@purchase_id, @part_id, @part_number, @part_name, @quantity, @purchase_price, @line_total);";

        using var cmd = new MySqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("@purchase_id", purchaseId);
        cmd.Parameters.AddWithValue("@part_id", record.PartId.HasValue ? record.PartId.Value : DBNull.Value);
        cmd.Parameters.AddWithValue("@part_number", record.PartNumber);
        cmd.Parameters.AddWithValue("@part_name", item.PartName);
        cmd.Parameters.AddWithValue("@quantity", item.Quantity);
        cmd.Parameters.AddWithValue("@purchase_price", item.PurchasePrice);
        cmd.Parameters.AddWithValue("@line_total", lineTotal);
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<int> InsertReturnAsync(MySqlConnection connection, ReturnCreateRequest request, decimal totalAmount)
    {
        string sql = $@"
            INSERT INTO `{request.SalesReturnsTableName}`
            (sale_id, return_date, customer_name, note, total_amount, status)
            VALUES (@sale_id, @return_date, @customer_name, @note, @total_amount, @status);";

        using var cmd = new MySqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("@sale_id", request.SaleId);
        cmd.Parameters.AddWithValue("@return_date", DateTime.Now);
        cmd.Parameters.AddWithValue("@customer_name", (object?)request.CustomerName ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@note", (object?)request.Note ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@total_amount", totalAmount);
        cmd.Parameters.AddWithValue("@status", "completed");
        await cmd.ExecuteNonQueryAsync();
        return Convert.ToInt32(cmd.LastInsertedId, CultureInfo.InvariantCulture);
    }

    private static async Task InsertReturnItemAsync(MySqlConnection connection, ReturnCreateRequest request, int returnId, SaleItemForReturn saleItem)
    {
        string sql = $@"
            INSERT INTO `{request.SalesReturnItemsTableName}`
            (return_id, sale_item_id, part_id, part_number, part_name, quantity, sale_price, line_total)
            VALUES (@return_id, @sale_item_id, @part_id, @part_number, @part_name, @quantity, @sale_price, @line_total);";

        using var cmd = new MySqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("@return_id", returnId);
        cmd.Parameters.AddWithValue("@sale_item_id", saleItem.Id);
        cmd.Parameters.AddWithValue("@part_id", saleItem.PartId.HasValue ? saleItem.PartId.Value : DBNull.Value);
        cmd.Parameters.AddWithValue("@part_number", saleItem.PartNumber);
        cmd.Parameters.AddWithValue("@part_name", saleItem.PartName);
        cmd.Parameters.AddWithValue("@quantity", saleItem.ReturnQuantity);
        cmd.Parameters.AddWithValue("@sale_price", saleItem.SalePrice);
        cmd.Parameters.AddWithValue("@line_total", saleItem.SalePrice * saleItem.ReturnQuantity);
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<SaleItemForReturn?> GetSaleItemForReturnAsync(MySqlConnection connection, ReturnCreateRequest request, int saleItemId)
    {
        string sql = $@"
            SELECT si.id,
                   si.sale_id,
                   si.part_id,
                   si.part_number,
                   si.part_name,
                   si.quantity,
                   si.sale_price,
                   COALESCE(r.returned_quantity, 0) AS returned_quantity
            FROM `{request.SaleItemsTableName}` si
            LEFT JOIN (
                SELECT sale_item_id, SUM(quantity) AS returned_quantity
                FROM `{request.SalesReturnItemsTableName}`
                GROUP BY sale_item_id
            ) r ON r.sale_item_id = si.id
            WHERE si.id = @sale_item_id AND si.sale_id = @sale_id
            LIMIT 1;";

        using var cmd = new MySqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("@sale_item_id", saleItemId);
        cmd.Parameters.AddWithValue("@sale_id", request.SaleId);
        using var reader = await cmd.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
            return null;

        return new SaleItemForReturn
        {
            Id = SafeGetInt(reader, "id"),
            SaleId = SafeGetInt(reader, "sale_id"),
            PartId = SafeGetNullableInt(reader, "part_id"),
            PartNumber = SafeGetString(reader, "part_number"),
            PartName = SafeGetString(reader, "part_name"),
            Quantity = SafeGetInt(reader, "quantity"),
            SalePrice = SafeGetDecimal(reader, "sale_price"),
            ReturnedQuantity = SafeGetInt(reader, "returned_quantity")
        };
    }

    private static async Task<Dictionary<string, string>> GetInventoryFieldsAsync(MySqlConnection connection, string tableName, InventoryTableMetadata metadata, InventoryRecord product)
    {
        string whereClause = product.PartId.HasValue && !string.IsNullOrWhiteSpace(metadata.IdColumn)
            ? $"`{metadata.IdColumn}` = @identifier"
            : $"`{metadata.PartNumberColumn}` = @identifier";

        string sql = $"SELECT * FROM `{tableName}` WHERE {whereClause} LIMIT 1;";
        using var cmd = new MySqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("@identifier", product.PartId.HasValue
            ? product.PartId.Value
            : product.PartNumber);

        using var reader = await cmd.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
            return new Dictionary<string, string>();

        var fields = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < reader.FieldCount; i++)
        {
            string name = reader.GetName(i);
            string value = reader.IsDBNull(i) ? string.Empty : Convert.ToString(reader.GetValue(i), CultureInfo.InvariantCulture) ?? string.Empty;
            fields[name] = value;
        }

        return fields;
    }

    private static async Task<List<object>> GetProductMovementsAsync(MySqlConnection connection, string movementsTableName, InventoryRecord product, int limit)
    {
        string sql = $@"
            SELECT movement_date, movement_type, quantity_change, unit_price, reference_type, reference_id, comment
            FROM `{movementsTableName}`
            WHERE ((@part_id IS NOT NULL AND part_id = @part_id) OR (@part_number <> '' AND part_number = @part_number))
            ORDER BY movement_date DESC, id DESC
            LIMIT @limit;";

        using var cmd = new MySqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("@part_id", product.PartId.HasValue ? product.PartId.Value : DBNull.Value);
        cmd.Parameters.AddWithValue("@part_number", product.PartNumber ?? string.Empty);
        cmd.Parameters.AddWithValue("@limit", limit);

        var movements = new List<object>();
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            movements.Add(new
            {
                movementDate = SafeGetDateTime(reader, "movement_date")?.ToString("dd.MM.yyyy HH:mm", CultureInfo.InvariantCulture) ?? "—",
                movementType = SafeGetString(reader, "movement_type"),
                quantityChange = SafeGetInt(reader, "quantity_change"),
                unitPrice = SafeGetDecimal(reader, "unit_price"),
                referenceType = SafeGetString(reader, "reference_type"),
                referenceId = SafeGetNullableInt(reader, "reference_id"),
                comment = SafeGetString(reader, "comment")
            });
        }

        return movements;
    }

    private static async Task<object> GetProductTotalsAsync(MySqlConnection connection, InventoryRecord product)
    {
        decimal totalSold = 0;
        int soldUnits = 0;
        decimal totalPurchased = 0;
        int purchasedUnits = 0;

        if (await TableExistsAsync(connection, "sale_items"))
        {
            string saleSql = @"
                SELECT COALESCE(SUM(quantity), 0) AS sold_units, COALESCE(SUM(line_total), 0) AS sold_amount
                FROM `sale_items`
                WHERE ((@part_id IS NOT NULL AND part_id = @part_id) OR (@part_number <> '' AND part_number = @part_number));";
            using var saleCmd = new MySqlCommand(saleSql, connection);
            saleCmd.Parameters.AddWithValue("@part_id", product.PartId.HasValue ? product.PartId.Value : DBNull.Value);
            saleCmd.Parameters.AddWithValue("@part_number", product.PartNumber ?? string.Empty);
            using var saleReader = await saleCmd.ExecuteReaderAsync();
            if (await saleReader.ReadAsync())
            {
                soldUnits = SafeGetInt(saleReader, "sold_units");
                totalSold = SafeGetDecimal(saleReader, "sold_amount");
            }
        }

        if (await TableExistsAsync(connection, "purchase_items"))
        {
            string purchaseSql = @"
                SELECT COALESCE(SUM(quantity), 0) AS purchased_units, COALESCE(SUM(line_total), 0) AS purchased_amount
                FROM `purchase_items`
                WHERE ((@part_id IS NOT NULL AND part_id = @part_id) OR (@part_number <> '' AND part_number = @part_number));";
            using var purchaseCmd = new MySqlCommand(purchaseSql, connection);
            purchaseCmd.Parameters.AddWithValue("@part_id", product.PartId.HasValue ? product.PartId.Value : DBNull.Value);
            purchaseCmd.Parameters.AddWithValue("@part_number", product.PartNumber ?? string.Empty);
            using var purchaseReader = await purchaseCmd.ExecuteReaderAsync();
            if (await purchaseReader.ReadAsync())
            {
                purchasedUnits = SafeGetInt(purchaseReader, "purchased_units");
                totalPurchased = SafeGetDecimal(purchaseReader, "purchased_amount");
            }
        }

        return new
        {
            soldUnits,
            totalSold,
            purchasedUnits,
            totalPurchased
        };
    }

    private static async Task<bool> TableExistsAsync(MySqlConnection connection, string tableName)
    {
        const string sql = @"
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = @tableName;";

        using var cmd = new MySqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("@tableName", tableName);
        var result = await cmd.ExecuteScalarAsync();
        return Convert.ToInt32(result, CultureInfo.InvariantCulture) > 0;
    }

    private static async Task UpdateInventoryAfterPurchaseAsync(MySqlConnection connection, string tableName, InventoryTableMetadata metadata, InventoryRecord record, int quantity, decimal purchasePrice)
    {
        var setParts = new List<string>
        {
            $"`{metadata.QuantityColumn}` = `{metadata.QuantityColumn}` + @qty"
        };

        if (!string.IsNullOrWhiteSpace(metadata.PurchasePriceColumn))
            setParts.Add($"`{metadata.PurchasePriceColumn}` = @purchase_price");

        string whereClause = record.PartId.HasValue && !string.IsNullOrWhiteSpace(metadata.IdColumn)
            ? $"`{metadata.IdColumn}` = @identifier"
            : $"`{metadata.PartNumberColumn}` = @identifier";

        string sql = $"UPDATE `{tableName}` SET {string.Join(", ", setParts)} WHERE {whereClause};";
        using var cmd = new MySqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("@qty", quantity);
        cmd.Parameters.AddWithValue("@purchase_price", purchasePrice);
        cmd.Parameters.AddWithValue("@identifier", record.PartId.HasValue ? record.PartId.Value : record.PartNumber);
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task UpdateInventoryAfterReturnAsync(MySqlConnection connection, string tableName, InventoryTableMetadata metadata, InventoryRecord record, int quantity)
    {
        string whereClause = record.PartId.HasValue && !string.IsNullOrWhiteSpace(metadata.IdColumn)
            ? $"`{metadata.IdColumn}` = @identifier"
            : $"`{metadata.PartNumberColumn}` = @identifier";

        string sql = $"UPDATE `{tableName}` SET `{metadata.QuantityColumn}` = `{metadata.QuantityColumn}` + @qty WHERE {whereClause};";
        using var cmd = new MySqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("@qty", quantity);
        cmd.Parameters.AddWithValue("@identifier", record.PartId.HasValue ? record.PartId.Value : record.PartNumber);
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task InsertInventoryMovementAsync(MySqlConnection connection, string tableName, string movementType, InventoryRecord record, int quantityChange, decimal unitPrice, string referenceType, int referenceId, string? comment)
    {
        string sql = $@"
            INSERT INTO `{tableName}`
            (movement_date, movement_type, part_id, part_number, part_name, quantity_change, unit_price, reference_type, reference_id, comment)
            VALUES (@movement_date, @movement_type, @part_id, @part_number, @part_name, @quantity_change, @unit_price, @reference_type, @reference_id, @comment);";

        using var cmd = new MySqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("@movement_date", DateTime.Now);
        cmd.Parameters.AddWithValue("@movement_type", movementType);
        cmd.Parameters.AddWithValue("@part_id", record.PartId.HasValue ? record.PartId.Value : DBNull.Value);
        cmd.Parameters.AddWithValue("@part_number", record.PartNumber);
        cmd.Parameters.AddWithValue("@part_name", record.PartName);
        cmd.Parameters.AddWithValue("@quantity_change", quantityChange);
        cmd.Parameters.AddWithValue("@unit_price", unitPrice);
        cmd.Parameters.AddWithValue("@reference_type", referenceType);
        cmd.Parameters.AddWithValue("@reference_id", referenceId);
        cmd.Parameters.AddWithValue("@comment", (object?)comment ?? DBNull.Value);
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task EnsureSalesTablesExistAsync(MySqlConnection connection, string salesTableName, string saleItemsTableName)
    {
        string saleSql = $@"
            CREATE TABLE IF NOT EXISTS `{salesTableName}` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                `sale_date` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                `customer_name` VARCHAR(150) NOT NULL,
                `customer_phone` VARCHAR(50) NULL,
                `customer_email` VARCHAR(150) NULL,
                `note` VARCHAR(255) NULL,
                `total_amount` DECIMAL(10,2) NOT NULL DEFAULT 0,
                `payment_method` VARCHAR(50) NOT NULL DEFAULT 'cash',
                `status` VARCHAR(50) NOT NULL DEFAULT 'completed'
            );";

        string saleItemsSql = $@"
            CREATE TABLE IF NOT EXISTS `{saleItemsTableName}` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                `sale_id` INT NOT NULL,
                `part_id` INT NULL,
                `part_number` VARCHAR(100) NULL,
                `part_name` VARCHAR(150) NOT NULL,
                `quantity` INT NOT NULL,
                `sale_price` DECIMAL(10,2) NOT NULL,
                `purchase_price` DECIMAL(10,2) NOT NULL DEFAULT 0,
                `line_total` DECIMAL(10,2) NOT NULL,
                INDEX (`sale_id`)
            );";

        using var cmd1 = new MySqlCommand(saleSql, connection);
        await cmd1.ExecuteNonQueryAsync();
        using var cmd2 = new MySqlCommand(saleItemsSql, connection);
        await cmd2.ExecuteNonQueryAsync();
    }

    private static async Task EnsureInventorySupportTablesAsync(MySqlConnection connection, string purchasesTableName, string purchaseItemsTableName, string movementsTableName, string salesReturnsTableName, string salesReturnItemsTableName)
    {
        string purchasesSql = $@"
            CREATE TABLE IF NOT EXISTS `{purchasesTableName}` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                `purchase_date` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                `supplier_name` VARCHAR(150) NOT NULL,
                `supplier_phone` VARCHAR(50) NULL,
                `document_number` VARCHAR(100) NULL,
                `note` VARCHAR(255) NULL,
                `total_amount` DECIMAL(10,2) NOT NULL DEFAULT 0,
                `status` VARCHAR(50) NOT NULL DEFAULT 'received'
            );";

        string purchaseItemsSql = $@"
            CREATE TABLE IF NOT EXISTS `{purchaseItemsTableName}` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                `purchase_id` INT NOT NULL,
                `part_id` INT NULL,
                `part_number` VARCHAR(100) NULL,
                `part_name` VARCHAR(150) NOT NULL,
                `quantity` INT NOT NULL,
                `purchase_price` DECIMAL(10,2) NOT NULL,
                `line_total` DECIMAL(10,2) NOT NULL,
                INDEX (`purchase_id`)
            );";

        string movementsSql = $@"
            CREATE TABLE IF NOT EXISTS `{movementsTableName}` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                `movement_date` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                `movement_type` VARCHAR(50) NOT NULL,
                `part_id` INT NULL,
                `part_number` VARCHAR(100) NULL,
                `part_name` VARCHAR(150) NULL,
                `quantity_change` INT NOT NULL,
                `unit_price` DECIMAL(10,2) NOT NULL DEFAULT 0,
                `reference_type` VARCHAR(50) NULL,
                `reference_id` INT NULL,
                `comment` VARCHAR(255) NULL,
                INDEX (`part_id`),
                INDEX (`part_number`)
            );";

        string returnsSql = $@"
            CREATE TABLE IF NOT EXISTS `{salesReturnsTableName}` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                `sale_id` INT NOT NULL,
                `return_date` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                `customer_name` VARCHAR(150) NULL,
                `note` VARCHAR(255) NULL,
                `total_amount` DECIMAL(10,2) NOT NULL DEFAULT 0,
                `status` VARCHAR(50) NOT NULL DEFAULT 'completed',
                INDEX (`sale_id`)
            );";

        string returnItemsSql = $@"
            CREATE TABLE IF NOT EXISTS `{salesReturnItemsTableName}` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                `return_id` INT NOT NULL,
                `sale_item_id` INT NOT NULL,
                `part_id` INT NULL,
                `part_number` VARCHAR(100) NULL,
                `part_name` VARCHAR(150) NOT NULL,
                `quantity` INT NOT NULL,
                `sale_price` DECIMAL(10,2) NOT NULL,
                `line_total` DECIMAL(10,2) NOT NULL,
                INDEX (`return_id`),
                INDEX (`sale_item_id`)
            );";

        foreach (var sql in new[] { purchasesSql, purchaseItemsSql, movementsSql, returnsSql, returnItemsSql })
        {
            using var cmd = new MySqlCommand(sql, connection);
            await cmd.ExecuteNonQueryAsync();
        }
    }

    private static async Task<InventoryTableMetadata> GetInventoryTableMetadataAsync(MySqlConnection connection, string tableName)
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
                var columnName = reader.GetString(0);
                if (!string.IsNullOrWhiteSpace(columnName))
                    columns.Add(columnName);
            }
        }

        if (columns.Count == 0)
            throw new Exception($"Таблица '{tableName}' не найдена или не содержит колонок.");

        string? quantityColumn = ResolveColumn(columns, "quantity", "qty", "stock", "остаток", "количество", "кол_во", "кол-во");
        if (string.IsNullOrWhiteSpace(quantityColumn))
            throw new Exception($"В таблице '{tableName}' не удалось найти колонку остатка.");

        return new InventoryTableMetadata
        {
            TableName = tableName,
            Columns = columns,
            IdColumn = ResolveColumn(columns, "id"),
            PartNumberColumn = ResolveColumn(columns, "part_number", "sku", "article", "артикул", "код"),
            PartNameColumn = ResolveColumn(columns, "part_name", "name", "название", "наименование", "товар"),
            BrandColumn = ResolveColumn(columns, "brand", "manufacturer", "бренд", "марка", "производитель"),
            CarBrandColumn = ResolveColumn(columns, "car_brand", "make", "марка_авто", "авто_марка"),
            CarModelColumn = ResolveColumn(columns, "car_model", "model", "модель", "авто_модель"),
            QuantityColumn = quantityColumn,
            MinQuantityColumn = ResolveColumn(columns, "min_quantity", "min_stock", "reorder_level", "минимальный_остаток", "minqty"),
            SalePriceColumn = ResolveColumn(columns, "sale_price", "retail_price", "selling_price", "unit_price", "price", "цена"),
            PurchasePriceColumn = ResolveColumn(columns, "purchase_price", "cost_price", "buy_price", "cost", "закупочная_цена")
        };
    }

    private static string? ResolveColumn(HashSet<string> columns, params string[] candidates)
    {
        foreach (var candidate in candidates)
        {
            var exact = columns.FirstOrDefault(c => string.Equals(c, candidate, StringComparison.OrdinalIgnoreCase));
            if (exact != null) return exact;
        }

        foreach (var candidate in candidates)
        {
            string normalizedCandidate = Normalize(candidate);
            var fuzzy = columns.FirstOrDefault(c => Normalize(c).Contains(normalizedCandidate, StringComparison.OrdinalIgnoreCase));
            if (fuzzy != null) return fuzzy;
        }

        return null;
    }

    private static async Task<InventoryRecord?> GetInventoryRecordAsync(MySqlConnection connection, string tableName, InventoryTableMetadata metadata, string? partId, string? partNumber)
    {
        string? whereColumn;
        object identifier;

        if (!string.IsNullOrWhiteSpace(partId) && !string.IsNullOrWhiteSpace(metadata.IdColumn) && int.TryParse(partId, NumberStyles.Integer, CultureInfo.InvariantCulture, out int parsedId))
        {
            whereColumn = metadata.IdColumn;
            identifier = parsedId;
        }
        else if (!string.IsNullOrWhiteSpace(partNumber) && !string.IsNullOrWhiteSpace(metadata.PartNumberColumn))
        {
            whereColumn = metadata.PartNumberColumn;
            identifier = partNumber;
        }
        else
        {
            throw new Exception($"Не удалось определить ключ товара в таблице '{tableName}'. Нужен id или артикул.");
        }

        string idSelect = !string.IsNullOrWhiteSpace(metadata.IdColumn) ? $"`{metadata.IdColumn}`" : "NULL";
        string partNumberSelect = !string.IsNullOrWhiteSpace(metadata.PartNumberColumn) ? $"`{metadata.PartNumberColumn}`" : "NULL";
        string partNameSelect = !string.IsNullOrWhiteSpace(metadata.PartNameColumn) ? $"`{metadata.PartNameColumn}`" : "NULL";
        string brandSelect = !string.IsNullOrWhiteSpace(metadata.BrandColumn) ? $"`{metadata.BrandColumn}`" : "NULL";
        string carBrandSelect = !string.IsNullOrWhiteSpace(metadata.CarBrandColumn) ? $"`{metadata.CarBrandColumn}`" : "NULL";
        string carModelSelect = !string.IsNullOrWhiteSpace(metadata.CarModelColumn) ? $"`{metadata.CarModelColumn}`" : "NULL";
        string salePriceSelect = !string.IsNullOrWhiteSpace(metadata.SalePriceColumn) ? $"`{metadata.SalePriceColumn}`" : "0";
        string purchasePriceSelect = !string.IsNullOrWhiteSpace(metadata.PurchasePriceColumn) ? $"`{metadata.PurchasePriceColumn}`" : salePriceSelect;
        string minQuantitySelect = !string.IsNullOrWhiteSpace(metadata.MinQuantityColumn) ? $"`{metadata.MinQuantityColumn}`" : "0";

        string sql = $@"
            SELECT {idSelect} AS part_id,
                   {partNumberSelect} AS part_number,
                   {partNameSelect} AS part_name,
                   {brandSelect} AS brand,
                   {carBrandSelect} AS car_brand,
                   {carModelSelect} AS car_model,
                   `{metadata.QuantityColumn}` AS quantity,
                   {minQuantitySelect} AS min_quantity,
                   {salePriceSelect} AS sale_price,
                   {purchasePriceSelect} AS purchase_price
            FROM `{tableName}`
            WHERE `{whereColumn}` = @identifier
            LIMIT 1;";

        using var cmd = new MySqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("@identifier", identifier);
        using var reader = await cmd.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
            return null;

        return new InventoryRecord
        {
            PartId = SafeGetNullableInt(reader, "part_id"),
            PartNumber = SafeGetString(reader, "part_number"),
            PartName = SafeGetString(reader, "part_name"),
            Brand = SafeGetString(reader, "brand"),
            CarBrand = SafeGetString(reader, "car_brand"),
            CarModel = SafeGetString(reader, "car_model"),
            Quantity = SafeGetInt(reader, "quantity"),
            MinQuantity = SafeGetInt(reader, "min_quantity"),
            SalePrice = SafeGetDecimal(reader, "sale_price"),
            PurchasePrice = SafeGetDecimal(reader, "purchase_price")
        };
    }

    private static string Normalize(string value)
    {
        return value.Trim().Replace(" ", "").Replace("_", "").Replace("-", "").ToLowerInvariant();
    }

    private static int SafeGetInt(MySqlDataReader reader, string columnName)
    {
        int ordinal = reader.GetOrdinal(columnName);
        return reader.IsDBNull(ordinal) ? 0 : Convert.ToInt32(reader.GetValue(ordinal), CultureInfo.InvariantCulture);
    }

    private static int? SafeGetNullableInt(MySqlDataReader reader, string columnName)
    {
        int ordinal = reader.GetOrdinal(columnName);
        return reader.IsDBNull(ordinal) ? null : Convert.ToInt32(reader.GetValue(ordinal), CultureInfo.InvariantCulture);
    }

    private static decimal SafeGetDecimal(MySqlDataReader reader, string columnName)
    {
        int ordinal = reader.GetOrdinal(columnName);
        if (reader.IsDBNull(ordinal)) return 0;
        var value = reader.GetValue(ordinal);
        if (value is decimal d) return d;
        return Convert.ToDecimal(value, CultureInfo.InvariantCulture);
    }

    private static string SafeGetString(MySqlDataReader reader, string columnName)
    {
        int ordinal = reader.GetOrdinal(columnName);
        return reader.IsDBNull(ordinal) ? string.Empty : reader.GetValue(ordinal)?.ToString() ?? string.Empty;
    }

    private static DateTime? SafeGetDateTime(MySqlDataReader reader, string columnName)
    {
        int ordinal = reader.GetOrdinal(columnName);
        return reader.IsDBNull(ordinal) ? null : Convert.ToDateTime(reader.GetValue(ordinal), CultureInfo.InvariantCulture);
    }

    private sealed class InventoryTableMetadata
    {
        public string TableName { get; set; } = string.Empty;
        public HashSet<string> Columns { get; set; } = new(StringComparer.OrdinalIgnoreCase);
        public string? IdColumn { get; set; }
        public string? PartNumberColumn { get; set; }
        public string? PartNameColumn { get; set; }
        public string? BrandColumn { get; set; }
        public string? CarBrandColumn { get; set; }
        public string? CarModelColumn { get; set; }
        public string QuantityColumn { get; set; } = "quantity";
        public string? MinQuantityColumn { get; set; }
        public string? SalePriceColumn { get; set; }
        public string? PurchasePriceColumn { get; set; }
    }

    private sealed class InventoryRecord
    {
        public int? PartId { get; set; }
        public string PartNumber { get; set; } = string.Empty;
        public string PartName { get; set; } = string.Empty;
        public string Brand { get; set; } = string.Empty;
        public string CarBrand { get; set; } = string.Empty;
        public string CarModel { get; set; } = string.Empty;
        public int Quantity { get; set; }
        public int MinQuantity { get; set; }
        public decimal SalePrice { get; set; }
        public decimal PurchasePrice { get; set; }
    }

    private sealed class SaleItemForReturn
    {
        public int Id { get; set; }
        public int SaleId { get; set; }
        public int? PartId { get; set; }
        public string PartNumber { get; set; } = string.Empty;
        public string PartName { get; set; } = string.Empty;
        public int Quantity { get; set; }
        public int ReturnedQuantity { get; set; }
        public int ReturnQuantity { get; set; }
        public decimal SalePrice { get; set; }
    }
}
