using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Authorization;

namespace MyFinanceApp.Controllers;

[Authorize(Roles = "Администратор,Менеджер,Кладовщик")]
public class SuppliersController : Controller
{
    public IActionResult Index() => View();
}
