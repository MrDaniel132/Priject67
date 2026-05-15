using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Authorization;
using System.Security.Claims;
using MyFinanceApp.Models;
using MyFinanceApp.Services;

namespace MyFinanceApp.Controllers;

public class AccountController : Controller
{
    private readonly IAuditLogService _auditLogService;

    public AccountController(IAuditLogService auditLogService)
    {
        _auditLogService = auditLogService;
    }

    [AllowAnonymous]
    [HttpGet]
    public IActionResult Login()
    {
        ViewBag.TestUsers = AppUserStore.GetAll();
        return View(new LoginViewModel());
    }

    [AllowAnonymous]
    [HttpPost]
    public async Task<IActionResult> Login(LoginViewModel model)
    {
        ViewBag.TestUsers = AppUserStore.GetAll();

        if (!ModelState.IsValid)
            return View(model);

        var user = AppUserStore.Validate(model.Username, model.Password);
        if (user is null)
        {
            ModelState.AddModelError(string.Empty, "Неверный логин или пароль (проверь раскладку)");
            return View(model);
        }

        var claims = new List<Claim>
        {
            new(ClaimTypes.Name, user.Username),
            new(ClaimTypes.Role, user.Role),
            new("display_name", user.DisplayName)
        };

        var claimsIdentity = new ClaimsIdentity(claims, CookieAuthenticationDefaults.AuthenticationScheme);
        await HttpContext.SignInAsync(
            CookieAuthenticationDefaults.AuthenticationScheme,
            new ClaimsPrincipal(claimsIdentity));

        AuditLogService.MarkLoginPending(HttpContext);
        return RedirectToAction("Index", "Parts");
    }

    [Authorize]
    public async Task<IActionResult> Logout()
    {
        var username = User.Identity?.Name ?? "unknown";
        await _auditLogService.LogLogoutIfPossibleAsync(HttpContext, username);
        await HttpContext.SignOutAsync(CookieAuthenticationDefaults.AuthenticationScheme);
        return RedirectToAction("Login");
    }

    [AllowAnonymous]
    [HttpGet]
    public IActionResult AccessDenied() => View();
}
