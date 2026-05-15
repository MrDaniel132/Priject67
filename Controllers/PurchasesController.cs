using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Authorization;

namespace MyFinanceApp.Controllers;

[Authorize(Roles = "Администратор,Кладовщик,Менеджер")]
public class PurchasesController : Controller
{
    public IActionResult Index() => View();
}
