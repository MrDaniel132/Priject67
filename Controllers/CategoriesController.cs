using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Authorization;

namespace MyFinanceApp.Controllers;

[Authorize(Roles = "Администратор,Кассир,Менеджер")]
public class CategoriesController : Controller
{
    public IActionResult Index() => View();
}
