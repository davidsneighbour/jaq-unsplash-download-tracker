#!/usr/bin/env node

import { chromium } from "playwright";

const output = process.argv[2] ?? "./unsplash-storage-state.json";

const browser = await chromium.launch({ headless: false });
const context = await browser.newContext();
const page = await context.newPage();

await page.goto("https://unsplash.com/login", { waitUntil: "domcontentloaded" });

console.log("Log in manually, then press Enter here in the terminal.");

process.stdin.resume();
process.stdin.once("data", async () => {
    await context.storageState({ path: output });
    await browser.close();
    console.log(`Saved storage state to ${output}`);
    process.exit(0);
});