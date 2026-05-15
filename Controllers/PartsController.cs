using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Authorization;
using MyWebApp.Models;

namespace MyFinanceApp.Controllers;

[Authorize]
public class PartsController : Controller
{
    public IActionResult Index()
    {
        var parts = new List<PartViewModel>
        {
            new PartViewModel { SKU = "8200084401", Name = "Термостат", Manufacturer = "Renault", CarModel = "Renault", Category = "Двигатель", Price = 2100, Stock = 0 },
            new PartViewModel { SKU = "1613239480", Name = "Ремень ГРМ", Manufacturer = "Peugeot", CarModel = "Peugeot", Category = "Двигатель", Price = 3200, Stock = 7 }
        };

        return View(parts);
    }
}
