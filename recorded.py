import asyncio
import re
from playwright.async_api import Playwright, async_playwright, expect


async def run(playwright: Playwright) -> None:
    browser = await playwright.chromium.launch(headless=False)
    context = await browser.new_context()
    await page.goto("https://aspapp.misa.vn/App/Account/Join")
    await page1.get_by_role("link", name="Báo cáo", exact=True).click()
    await page1.locator("div").filter(has_text=re.compile(r"^Báo cáo tài chính$")).first.click()
    await page1.get_by_role("link", name="B01-DN: Báo cáo tình hình tài").click()
    await page1.get_by_role("button", name="Chọn tham số").click()
    await page1.get_by_role("textbox", name="DD/MM/YYYY").first.click()
    await page1.get_by_role("textbox", name="DD/MM/YYYY").nth(1).click()
    await page1.get_by_role("button", name="Xem báo cáo").click()
    async with page1.expect_download() as download_info:
        async with page1.expect_popup() as page2_info:
            await page1.locator(".flex-center.print-button > .con-ms-tooltip > .tooltip-content > div > .ms-component").click()
        page2 = await page2_info.value
    download = await download_info.value
    await page2.close()

    # ---------------------
    await context.close()
    await browser.close()


async def main() -> None:
    async with async_playwright() as playwright:
        await run(playwright)


asyncio.run(main())
