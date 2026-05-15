using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using MyFinanceApp.Models;
using MyFinanceApp.Services;

namespace MyFinanceApp.Controllers;

[Authorize(Roles = "Администратор")]
public class EmployeesController : Controller
{
    [HttpGet]
    public IActionResult Index()
    {
        ViewBag.Users = AppUserStore.GetAll();
        return View(new EmployeeCreateViewModel());
    }

    [HttpPost]
    public IActionResult Delete(string username)
    {
        if (string.IsNullOrWhiteSpace(username))
        {
            TempData["Error"] = "Логин сотрудника не передан";
            return RedirectToAction(nameof(Index));
        }

        var deleted = AppUserStore.DeleteUser(username);

        if (!deleted)
        {
            TempData["Error"] = "Не удалось удалить сотрудника. Главного admin удалять нельзя";
            return RedirectToAction(nameof(Index));
        }

        TempData["Success"] = "Сотрудник успешно удален";
        return RedirectToAction(nameof(Index));
    }

    [HttpPost]
    public IActionResult Index(EmployeeCreateViewModel model)
    {
        ViewBag.Users = AppUserStore.GetAll();

        if (!ModelState.IsValid)
            return View(model);

        var added = AppUserStore.AddUser(new AppUser(
            model.Username.Trim(),
            model.Password.Trim(),
            model.Role,
            model.FullName.Trim()
        ));

        if (!added)
        {
            ModelState.AddModelError(nameof(model.Username), "Пользователь с таким логином уже существует");
            return View(model);
        }

        TempData["Success"] = "Сотрудник успешно добавлен";
        return RedirectToAction(nameof(Index));
    }
}