namespace MyWebApp.Models;

public class PartViewModel
{
    public string SKU { get; set; } = ""; // Артикул
    public string Name { get; set; } = "";
    public string Manufacturer { get; set; } = "";
    public string CarModel { get; set; } = "";
    public string Category { get; set; } = "";
    public decimal Price { get; set; }
    public int Stock { get; set; }
    public bool IsInStock => Stock > 0;

    // Поле для хранения путей к фото
    public string? Images { get; set; }

    // Вспомогательное свойство, которое превращает строку в массив для слайдера
    public string[] ImageList => string.IsNullOrEmpty(Images)
        ? new string[] { "/img/no-photo.png" }
        : Images.Split(',');
}