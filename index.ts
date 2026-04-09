#!/usr/bin/env node

import { createHash } from "node:crypto";
import { createWriteStream, existsSync } from "node:fs";
import { mkdir, readFile, rename, rm, stat, writeFile } from "node:fs/promises";
import { basename, join, resolve } from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { chromium, type Browser, type BrowserContext, type Page } from "playwright";

type JsonValue =
    | string
    | number
    | boolean
    | null
    | JsonObject
    | JsonArray;

interface JsonObject {
    [key: string]: JsonValue;
}

type JsonArray = JsonValue[];

interface CliOptions {
    outputDir: string;
    downloadsDir: string;
    metadataDir: string;
    manifestFile: string;
    statusFile: string;
    headTimeoutMs: number;
    downloadTimeoutMs: number;
    historyUrl: string;
    verbose: boolean;
    refreshAll: boolean;
    maxScrollRounds: number;
    scrollPauseMs: number;
}

interface UnsplashUrls {
    raw?: string;
    full?: string;
    regular?: string;
    small?: string;
    thumb?: string;
}

interface UnsplashLinks {
    self?: string;
    html?: string;
    download?: string;
    download_location?: string;
}

interface UnsplashUser {
    id: string;
    username?: string;
    name?: string;
}

interface UnsplashPhoto {
    id: string;
    slug?: string;
    created_at?: string;
    updated_at?: string;
    width?: number;
    height?: number;
    color?: string | null;
    blur_hash?: string | null;
    description?: string | null;
    alt_description?: string | null;
    urls?: UnsplashUrls;
    links?: UnsplashLinks;
    user?: UnsplashUser;
    [key: string]: JsonValue | undefined;
}

interface StoredPhotoEntry {
    id: string;
    photoJsonFile: string;
    imageFile: string | null;
    photoUpdatedAt: string | null;
    photoJsonSha256: string;
    imageSha256: string | null;
    imageSizeBytes: number | null;
    imageContentType: string | null;
    imageContentLength: number | null;
    imageEtag: string | null;
    imageLastModified: string | null;
    photoHtmlUrl: string | null;
    lastSyncedAt: string;
}

interface RateLimitState {
    /**
     * Unsplash documents a default demo limit of 50 requests/hour.
     * Production-approved apps are increased beyond that.
     * We use 50 as the conservative fallback when headers are missing.
     */
    limit: number | null;
    remaining: number | null;
    resetEpochSeconds: number | null;
    lastUpdatedAt: string | null;
}

interface StatusFile {
    blockedUntil: string | null;
    reason: string | null;
    note: string | null;
    limit: number | null;
    remaining: number | null;
    resetEpochSeconds: number | null;
    lastUpdatedAt: string;
}

interface Manifest {
    version: 1;
    generatedAt: string;
    historyUrl: string;
    rateLimit: RateLimitState;
    entries: Record<string, StoredPhotoEntry>;
}

interface HeadInfo {
    contentType: string | null;
    contentLength: number | null;
    etag: string | null;
    lastModified: string | null;
    finalUrl: string;
}

interface DownloadResult {
    sha256: string;
    sizeBytes: number;
    contentType: string | null;
    contentLength: number | null;
    etag: string | null;
    lastModified: string | null;
    finalUrl: string;
}

interface BrowserSession {
    context: BrowserContext;
    browser: Browser | null;
}

interface RetryOptions {
    retries?: number;
    baseDelayMs?: number;
    verbose?: boolean;
    contextLabel?: string;
    allowHttpErrorRetry?: boolean;
}

const DEFAULT_OPTIONS: CliOptions = {
    outputDir: "./unsplash-archive",
    downloadsDir: "images",
    metadataDir: "photos",
    manifestFile: "manifest.json",
    statusFile: "status.json",
    headTimeoutMs: 30_000,
    downloadTimeoutMs: 120_000,
    historyUrl: "https://unsplash.com/downloads",
    verbose: false,
    refreshAll: false,
    maxScrollRounds: 100,
    scrollPauseMs: 1_000,
};

const GLOBAL_DELAY_MS = 2000;

/**
 * Conservative documented fallback for demo mode.
 * Unsplash documents 50 requests/hour for demo applications.
 */
const UNSPLASH_DEFAULT_DEMO_RATE_LIMIT_PER_HOUR = 50;

let currentRateLimitState: RateLimitState = {
    limit: UNSPLASH_DEFAULT_DEMO_RATE_LIMIT_PER_HOUR,
    remaining: null,
    resetEpochSeconds: null,
    lastUpdatedAt: null,
};

function printHelp(): void {
    const command = basename(process.argv[1] ?? "index.ts");
    console.log(`
${command}

Synchronise your Unsplash download history into a local archive.

Options:
  --output-dir <path>         Base output directory
  --verbose                   Enable verbose logging
  --refresh-all               Force re-fetch of cached photo JSON
  --head-timeout-ms <ms>      Timeout for HEAD requests
  --download-timeout-ms <ms>  Timeout for downloads
  --history-url <url>         Download history page URL
  --max-scroll-rounds <n>     Maximum infinite-scroll rounds
  --scroll-pause-ms <ms>      Pause between scroll rounds
  --help                      Show this help

Environment:
  UNSPLASH_ACCESS_KEY
  UNSPLASH_AUTHORIZATION_TOKEN
  UNSPLASH_STORAGE_STATE      Optional; defaults to ./unsplash-storage-state.json if present
  UNSPLASH_USER_DATA_DIR      Optional alternative to storage state
`.trim());
}

function logInfo(message: string): void {
    console.log(`[info] ${message}`);
}

function logWarn(message: string): void {
    console.warn(`[warn] ${message}`);
}

function logError(message: string): void {
    console.error(`[error] ${message}`);
}

function logVerbose(enabled: boolean, message: string): void {
    if (enabled) {
        console.log(`[debug] ${message}`);
    }
}

function parsePositiveInteger(value: string, flagName: string): number {
    const parsed = Number.parseInt(value, 10);
    if (!Number.isFinite(parsed) || parsed <= 0) {
        throw new Error(`Invalid numeric value for ${flagName}: ${value}`);
    }

    return parsed;
}

function parseCliArgs(argv: string[]): CliOptions {
    const options: CliOptions = { ...DEFAULT_OPTIONS };

    for (let index = 0; index < argv.length; index += 1) {
        const arg = argv[index];

        switch (arg) {
            case "--help":
                printHelp();
                process.exit(0);
            case "--verbose":
                options.verbose = true;
                break;
            case "--refresh-all":
                options.refreshAll = true;
                break;
            case "--output-dir": {
                const value = argv[index + 1];
                if (!value) {
                    throw new Error("Missing value for --output-dir");
                }
                options.outputDir = value;
                index += 1;
                break;
            }
            case "--head-timeout-ms": {
                const value = argv[index + 1];
                if (!value) {
                    throw new Error("Missing value for --head-timeout-ms");
                }
                options.headTimeoutMs = parsePositiveInteger(value, "--head-timeout-ms");
                index += 1;
                break;
            }
            case "--download-timeout-ms": {
                const value = argv[index + 1];
                if (!value) {
                    throw new Error("Missing value for --download-timeout-ms");
                }
                options.downloadTimeoutMs = parsePositiveInteger(value, "--download-timeout-ms");
                index += 1;
                break;
            }
            case "--history-url": {
                const value = argv[index + 1];
                if (!value) {
                    throw new Error("Missing value for --history-url");
                }
                options.historyUrl = value;
                index += 1;
                break;
            }
            case "--max-scroll-rounds": {
                const value = argv[index + 1];
                if (!value) {
                    throw new Error("Missing value for --max-scroll-rounds");
                }
                options.maxScrollRounds = parsePositiveInteger(value, "--max-scroll-rounds");
                index += 1;
                break;
            }
            case "--scroll-pause-ms": {
                const value = argv[index + 1];
                if (!value) {
                    throw new Error("Missing value for --scroll-pause-ms");
                }
                options.scrollPauseMs = parsePositiveInteger(value, "--scroll-pause-ms");
                index += 1;
                break;
            }
            default:
                throw new Error(`Unknown argument: ${arg}`);
        }
    }

    return options;
}

function getRequiredEnv(name: string): string {
    const value = process.env[name];
    if (!value) {
        throw new Error(`Missing required environment variable: ${name}`);
    }

    return value;
}

function getStorageStatePath(): string | null {
    const fromEnv = process.env.UNSPLASH_STORAGE_STATE;
    if (fromEnv && fromEnv.trim().length > 0) {
        return resolve(fromEnv);
    }

    const fallback = resolve("./unsplash-storage-state.json");
    return existsSync(fallback) ? fallback : null;
}

function buildApiHeaders(): HeadersInit {
    const accessKey = getRequiredEnv("UNSPLASH_ACCESS_KEY");
    const bearerToken = getRequiredEnv("UNSPLASH_AUTHORIZATION_TOKEN");

    return {
        Authorization: `Bearer ${bearerToken}`,
        "Accept-Version": "v1",
        Accept: "application/json",
        "User-Agent": "jaq-unsplash-download-tracker",
        "X-Unsplash-Access-Key": accessKey,
    };
}

async function ensureDirectory(pathname: string): Promise<void> {
    await mkdir(pathname, { recursive: true });
}

async function readJsonFile<T>(filePath: string, fallback: T): Promise<T> {
    if (!existsSync(filePath)) {
        return fallback;
    }

    const content = await readFile(filePath, "utf8");
    return JSON.parse(content) as T;
}

async function writeJsonFile(filePath: string, value: unknown): Promise<void> {
    const text = JSON.stringify(value, null, 2);
    await writeFile(filePath, `${text}\n`, "utf8");
}

function createEmptyManifest(historyUrl: string): Manifest {
    return {
        version: 1,
        generatedAt: new Date().toISOString(),
        historyUrl,
        rateLimit: {
            limit: UNSPLASH_DEFAULT_DEMO_RATE_LIMIT_PER_HOUR,
            remaining: null,
            resetEpochSeconds: null,
            lastUpdatedAt: null,
        },
        entries: {},
    };
}

function createEmptyStatus(): StatusFile {
    return {
        blockedUntil: null,
        reason: null,
        note: null,
        limit: UNSPLASH_DEFAULT_DEMO_RATE_LIMIT_PER_HOUR,
        remaining: null,
        resetEpochSeconds: null,
        lastUpdatedAt: new Date().toISOString(),
    };
}

function computeSha256FromBuffer(buffer: Buffer): string {
    return createHash("sha256").update(buffer).digest("hex");
}

async function computeFileSha256(filePath: string): Promise<string> {
    const content = await readFile(filePath);
    return computeSha256FromBuffer(content);
}

function safeFileStem(input: string): string {
    return input
        .trim()
        .toLowerCase()
        .replaceAll(/[^a-z0-9._-]+/g, "-")
        .replaceAll(/-+/g, "-")
        .replaceAll(/^-|-$/g, "");
}

function inferExtensionFromContentType(contentType: string | null): string {
    const mime = contentType?.split(";")[0]?.trim().toLowerCase();

    switch (mime) {
        case "image/jpeg":
            return ".jpg";
        case "image/png":
            return ".png";
        case "image/webp":
            return ".webp";
        case "image/avif":
            return ".avif";
        case "image/gif":
            return ".gif";
        default:
            return ".jpg";
    }
}

function buildImageBaseName(photo: UnsplashPhoto): string {
    const username = photo.user?.username ? safeFileStem(photo.user.username) : "unknown-user";
    const slug = photo.slug ? safeFileStem(photo.slug) : photo.id;
    return `${photo.id}--${username}--${slug}`;
}

function sleep(ms: number): Promise<void> {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}

function getDefaultHourlyLimitFromUnsplashDocs(): number {
    return UNSPLASH_DEFAULT_DEMO_RATE_LIMIT_PER_HOUR;
}

function updateRateLimitStateFromHeaders(headers: Headers, verbose: boolean): void {
    const limitHeader = headers.get("x-ratelimit-limit");
    const remainingHeader = headers.get("x-ratelimit-remaining");
    const resetHeader = headers.get("x-ratelimit-reset");

    const parsedLimit = limitHeader !== null ? Number.parseInt(limitHeader, 10) : Number.NaN;
    const parsedRemaining = remainingHeader !== null ? Number.parseInt(remainingHeader, 10) : Number.NaN;
    const parsedReset = resetHeader !== null ? Number.parseInt(resetHeader, 10) : Number.NaN;

    currentRateLimitState = {
        limit: Number.isFinite(parsedLimit) ? parsedLimit : currentRateLimitState.limit,
        remaining: Number.isFinite(parsedRemaining) ? parsedRemaining : currentRateLimitState.remaining,
        resetEpochSeconds: Number.isFinite(parsedReset)
            ? parsedReset
            : currentRateLimitState.resetEpochSeconds,
        lastUpdatedAt: new Date().toISOString(),
    };

    if (verbose) {
        logVerbose(
            verbose,
            `Rate limit state: limit=${String(currentRateLimitState.limit)} remaining=${String(currentRateLimitState.remaining)} reset=${String(currentRateLimitState.resetEpochSeconds)}`,
        );
    }
}

async function waitForRateLimitResetIfNeeded(verbose: boolean): Promise<void> {
    if (
        currentRateLimitState.remaining !== null &&
        currentRateLimitState.remaining <= 0 &&
        currentRateLimitState.resetEpochSeconds !== null
    ) {
        const resetMs = currentRateLimitState.resetEpochSeconds * 1000;
        const nowMs = Date.now();

        if (resetMs > nowMs) {
            const delayMs = resetMs - nowMs + 2000;
            logWarn(`Rate limit exhausted. Waiting ${delayMs}ms until reset.`);
            logVerbose(verbose, `Sleeping until ${new Date(resetMs).toISOString()}`);
            await sleep(delayMs);
        }
    }
}

function formatMinutesFromNow(targetIso: string): number {
    const diffMs = new Date(targetIso).getTime() - Date.now();
    return Math.max(0, Math.ceil(diffMs / 1000 / 60));
}

function calculateBlockedUntilIso(resetEpochSeconds: number | null, limit: number | null): string {
    if (resetEpochSeconds !== null && Number.isFinite(resetEpochSeconds)) {
        return new Date((resetEpochSeconds * 1000) + 2000).toISOString();
    }

    /**
     * Fallback:
     * when Unsplash does not provide reset timing, assume a full hourly cooldown.
     * This matches the documented demo-mode rate limit model.
     */
    const fallbackLimit = limit ?? getDefaultHourlyLimitFromUnsplashDocs();
    const fallbackMs = fallbackLimit > 0 ? 60 * 60 * 1000 : 60 * 60 * 1000;
    return new Date(Date.now() + fallbackMs).toISOString();
}

async function loadStatusFile(statusPath: string): Promise<StatusFile> {
    return readJsonFile<StatusFile>(statusPath, createEmptyStatus());
}

async function writeStatusFile(statusPath: string, status: StatusFile): Promise<void> {
    await writeJsonFile(statusPath, status);
}

async function clearStatusFile(statusPath: string): Promise<void> {
    await writeStatusFile(statusPath, createEmptyStatus());
}

async function assertNotBlocked(statusPath: string): Promise<void> {
    const status = await loadStatusFile(statusPath);

    if (!status.blockedUntil) {
        return;
    }

    const blockedUntilMs = new Date(status.blockedUntil).getTime();

    if (Number.isNaN(blockedUntilMs)) {
        return;
    }

    if (blockedUntilMs > Date.now()) {
        const minutes = formatMinutesFromNow(status.blockedUntil);
        throw new Error(
            `Unsplash API cooldown active. Try again after ${minutes} minute(s) at ${status.blockedUntil}. Reason: ${status.reason ?? "unknown"}.`,
        );
    }
}

async function setRateLimitedStatus(
    statusPath: string,
    reason: string,
    verbose: boolean,
): Promise<void> {
    const limit = currentRateLimitState.limit ?? getDefaultHourlyLimitFromUnsplashDocs();
    const remaining = currentRateLimitState.remaining ?? 0;
    const resetEpochSeconds = currentRateLimitState.resetEpochSeconds ?? null;
    const blockedUntil = calculateBlockedUntilIso(resetEpochSeconds, limit);

    const note =
        limit === UNSPLASH_DEFAULT_DEMO_RATE_LIMIT_PER_HOUR
            ? "Unsplash demo applications are documented at 50 API requests per hour. This script uses that as a conservative fallback when headers are missing."
            : `Unsplash rate limit detected from headers: ${String(limit)} API requests per hour.`;

    const status: StatusFile = {
        blockedUntil,
        reason,
        note,
        limit,
        remaining,
        resetEpochSeconds,
        lastUpdatedAt: new Date().toISOString(),
    };

    await writeStatusFile(statusPath, status);

    logWarn(
        `Rate-limited. Cooldown written to status file. Try again after ${formatMinutesFromNow(blockedUntil)} minute(s) at ${blockedUntil}.`,
    );

    logVerbose(verbose, `Status file content: ${JSON.stringify(status)}`);
}

async function fetchWithRetry(
    input: RequestInfo | URL,
    init: RequestInit,
    options: RetryOptions = {},
): Promise<Response> {
    const retries = options.retries ?? 5;
    const baseDelayMs = options.baseDelayMs ?? GLOBAL_DELAY_MS;
    const verbose = options.verbose ?? false;
    const contextLabel = options.contextLabel ?? "request";
    const allowHttpErrorRetry = options.allowHttpErrorRetry ?? false;

    let lastError: unknown = null;

    for (let attempt = 0; attempt <= retries; attempt += 1) {
        await waitForRateLimitResetIfNeeded(verbose);

        try {
            const response = await fetch(input, init);
            updateRateLimitStateFromHeaders(response.headers, verbose);

            const responseText =
                response.ok ? null : await response.clone().text().catch(() => "");

            const isExplicitRateLimit403 =
                response.status === 403 &&
                typeof responseText === "string" &&
                responseText.toLowerCase().includes("rate limit exceeded");

            if (response.status === 429 || isExplicitRateLimit403) {
                const retryAfterHeader = response.headers.get("retry-after");
                const retryAfterSeconds =
                    retryAfterHeader !== null ? Number.parseInt(retryAfterHeader, 10) : Number.NaN;

                const retryDelayMs = Number.isFinite(retryAfterSeconds)
                    ? retryAfterSeconds * 1000
                    : baseDelayMs * 2 ** attempt;

                logWarn(
                    `[${contextLabel}] Rate-limit response received (${response.status}). Waiting ${retryDelayMs}ms before retry.`,
                );

                await sleep(retryDelayMs);
                lastError = new Error(
                    `Rate limit response for ${contextLabel}: HTTP ${response.status} ${response.statusText} ${responseText ?? ""}`.trim(),
                );
                continue;
            }

            if (!response.ok) {
                if (allowHttpErrorRetry && response.status >= 500 && attempt < retries) {
                    const retryDelayMs = baseDelayMs * 2 ** attempt;
                    logWarn(`[${contextLabel}] HTTP ${response.status}. Retrying in ${retryDelayMs}ms.`);
                    await sleep(retryDelayMs);
                    continue;
                }

                throw new Error(
                    `HTTP ${response.status} ${response.statusText} for ${contextLabel}: ${responseText ?? ""}`,
                );
            }

            return response;
        } catch (error: unknown) {
            lastError = error;

            if (attempt >= retries) {
                break;
            }

            const retryDelayMs = baseDelayMs * 2 ** attempt;
            logWarn(`[${contextLabel}] Request failed. Retrying in ${retryDelayMs}ms.`);
            logVerbose(
                verbose,
                `[${contextLabel}] Failure detail: ${error instanceof Error ? error.message : String(error)}`,
            );
            await sleep(retryDelayMs);
        }
    }

    if (lastError instanceof Error) {
        throw lastError;
    }

    throw new Error(`[${contextLabel}] Request failed after retries.`);
}

async function apiGetJson<T extends JsonValue>(
    url: string,
    verbose: boolean,
    contextLabel: string,
): Promise<T> {
    const response = await fetchWithRetry(
        url,
        {
            method: "GET",
            headers: buildApiHeaders(),
        },
        {
            verbose,
            contextLabel,
            allowHttpErrorRetry: true,
        },
    );

    return (await response.json()) as T;
}

async function fetchPhoto(photoId: string, verbose: boolean): Promise<UnsplashPhoto> {
    const url = `https://api.unsplash.com/photos/${encodeURIComponent(photoId)}`;
    const photo = await apiGetJson<UnsplashPhoto>(url, verbose, `photo:${photoId}`);

    if (!photo.id) {
        throw new Error(`Photo response for ${photoId} did not contain an id`);
    }

    return photo;
}

async function fetchTrackedDownloadUrl(photoId: string, verbose: boolean): Promise<string> {
    const url = `https://api.unsplash.com/photos/${encodeURIComponent(photoId)}/download`;
    const result = await apiGetJson<{ url?: string }>(url, verbose, `download-url:${photoId}`);

    if (!result.url) {
        throw new Error(`Download URL missing for photo ${photoId}`);
    }

    return result.url;
}

async function headUrl(
    url: string,
    timeoutMs: number,
    verbose: boolean,
    contextLabel: string,
): Promise<HeadInfo> {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);

    try {
        const response = await fetchWithRetry(
            url,
            {
                method: "HEAD",
                redirect: "follow",
                signal: controller.signal,
            },
            {
                verbose,
                contextLabel,
                allowHttpErrorRetry: true,
            },
        );

        const contentLengthHeader = response.headers.get("content-length");
        const contentLength =
            contentLengthHeader !== null ? Number.parseInt(contentLengthHeader, 10) : null;

        return {
            contentType: response.headers.get("content-type"),
            contentLength: Number.isFinite(contentLength) ? contentLength : null,
            etag: response.headers.get("etag"),
            lastModified: response.headers.get("last-modified"),
            finalUrl: response.url,
        };
    } finally {
        clearTimeout(timer);
    }
}

async function downloadFile(
    url: string,
    destinationPath: string,
    timeoutMs: number,
    verbose: boolean,
    contextLabel: string,
): Promise<DownloadResult> {
    const tempPath = `${destinationPath}.part`;
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);

    try {
        const response = await fetchWithRetry(
            url,
            {
                method: "GET",
                redirect: "follow",
                signal: controller.signal,
            },
            {
                verbose,
                contextLabel,
                allowHttpErrorRetry: true,
            },
        );

        if (!response.body) {
            throw new Error(`GET ${url} returned no response body`);
        }

        const nodeReadable = Readable.fromWeb(response.body as globalThis.ReadableStream);
        const output = createWriteStream(tempPath);

        await pipeline(nodeReadable, output);

        const fileStats = await stat(tempPath);
        const sha256 = await computeFileSha256(tempPath);

        await rename(tempPath, destinationPath);

        const contentLengthHeader = response.headers.get("content-length");
        const contentLength =
            contentLengthHeader !== null ? Number.parseInt(contentLengthHeader, 10) : null;

        return {
            sha256,
            sizeBytes: fileStats.size,
            contentType: response.headers.get("content-type"),
            contentLength: Number.isFinite(contentLength) ? contentLength : null,
            etag: response.headers.get("etag"),
            lastModified: response.headers.get("last-modified"),
            finalUrl: response.url,
        };
    } catch (error: unknown) {
        await rm(tempPath, { force: true });
        throw error;
    } finally {
        clearTimeout(timer);
    }
}

async function createBrowserSession(verbose: boolean): Promise<BrowserSession> {
    const storageStatePath = getStorageStatePath();
    const userDataDir = process.env.UNSPLASH_USER_DATA_DIR?.trim() || null;

    if (storageStatePath) {
        logInfo(`Using storage state: ${storageStatePath}`);
        const browser = await chromium.launch({ headless: true });
        const context = await browser.newContext({ storageState: storageStatePath });
        return { context, browser };
    }

    if (userDataDir) {
        logInfo(`Using browser profile: ${resolve(userDataDir)}`);
        const context = await chromium.launchPersistentContext(resolve(userDataDir), {
            headless: true,
        });
        return { context, browser: null };
    }

    logVerbose(verbose, "No storage state file found at ./unsplash-storage-state.json");
    throw new Error(
        "No browser authentication available. Set UNSPLASH_STORAGE_STATE, or place unsplash-storage-state.json in the current directory, or set UNSPLASH_USER_DATA_DIR.",
    );
}

async function assertUnsplashLogin(page: Page): Promise<void> {
    await page.goto("https://unsplash.com/downloads", {
        waitUntil: "domcontentloaded",
        timeout: 120_000,
    });

    await page.waitForLoadState("networkidle", { timeout: 30_000 }).catch(() => {
        /* ignore */
    });

    const currentUrl = page.url();
    const title = await page.title();

    if (currentUrl.includes("/login")) {
        throw new Error("Browser session is not authenticated. Unsplash redirected to /login.");
    }

    logInfo(`Loaded download page: ${currentUrl}`);
    logInfo(`Page title: ${title}`);
}

function extractPhotoIdFromHref(href: string): string | null {
    const patterns = [
        /\/photos\/([A-Za-z0-9_-]+)(?:[/?#]|$)/,
        /\/photos\/[A-Za-z0-9_-]+-([A-Za-z0-9_-]+)(?:[/?#]|$)/,
    ];

    for (const pattern of patterns) {
        const match = href.match(pattern);
        const photoId = match?.[1];
        if (photoId) {
            return photoId;
        }
    }

    return null;
}

async function collectDownloadedPhotoIds(
    page: Page,
    historyUrl: string,
    maxScrollRounds: number,
    scrollPauseMs: number,
    verbose: boolean,
): Promise<string[]> {
    await page.goto(historyUrl, {
        waitUntil: "domcontentloaded",
        timeout: 120_000,
    });

    await page.waitForLoadState("networkidle", { timeout: 30_000 }).catch(() => {
        logVerbose(verbose, "networkidle timed out; continuing");
    });

    for (let round = 0; round < maxScrollRounds; round += 1) {
        const previousHeight = await page.evaluate(() => document.body.scrollHeight);

        await page.evaluate(() => {
            window.scrollTo(0, document.body.scrollHeight);
        });

        await page.waitForTimeout(scrollPauseMs);

        const nextHeight = await page.evaluate(() => document.body.scrollHeight);

        logVerbose(verbose, `Scroll round ${round + 1}: ${previousHeight} -> ${nextHeight}`);

        if (nextHeight <= previousHeight) {
            break;
        }
    }

    const hrefs = await page.locator("a[href]").evaluateAll((elements) => {
        return elements
            .map((element) => element.getAttribute("href"))
            .filter((href): href is string => typeof href === "string");
    });

    const ids = new Set<string>();

    for (const href of hrefs) {
        const photoId = extractPhotoIdFromHref(href);
        if (photoId) {
            ids.add(photoId);
        }
    }

    return [...ids];
}

function shouldSkipDownload(
    previousEntry: StoredPhotoEntry | undefined,
    photo: UnsplashPhoto,
    headInfo: HeadInfo,
    imagePath: string,
    refreshAll: boolean,
): boolean {
    if (refreshAll) {
        return false;
    }

    if (!previousEntry) {
        return false;
    }

    if (!existsSync(imagePath)) {
        return false;
    }

    const sameUpdatedAt = previousEntry.photoUpdatedAt === (photo.updated_at ?? null);
    const sameEtag =
        previousEntry.imageEtag !== null &&
        headInfo.etag !== null &&
        previousEntry.imageEtag === headInfo.etag;

    const sameLastModified =
        previousEntry.imageLastModified !== null &&
        headInfo.lastModified !== null &&
        previousEntry.imageLastModified === headInfo.lastModified;

    const sameContentLength =
        previousEntry.imageContentLength !== null &&
        headInfo.contentLength !== null &&
        previousEntry.imageContentLength === headInfo.contentLength;

    return sameUpdatedAt && (sameEtag || sameLastModified || sameContentLength);
}

async function loadPhotoFromCacheOrApi(
    photoId: string,
    photoJsonPath: string,
    refreshAll: boolean,
    verbose: boolean,
): Promise<UnsplashPhoto> {
    if (!refreshAll && existsSync(photoJsonPath)) {
        const cachedPhoto = await readJsonFile<UnsplashPhoto | null>(photoJsonPath, null);

        if (cachedPhoto && typeof cachedPhoto.id === "string" && cachedPhoto.id.length > 0) {
            logVerbose(verbose, `Using cached photo JSON for ${photoId}`);
            return cachedPhoto;
        }

        logWarn(`Cached photo JSON for ${photoId} is invalid. Re-fetching from API.`);
    }

    logVerbose(verbose, `Fetching photo JSON from API for ${photoId}`);
    return fetchPhoto(photoId, verbose);
}

function isRateLimitMessage(message: string): boolean {
    const lower = message.toLowerCase();
    return (
        lower.includes("rate limit exceeded") ||
        lower.includes("http 429") ||
        lower.includes("rate limit response") ||
        lower.includes("cooldown active")
    );
}

async function main(): Promise<void> {
    const options = parseCliArgs(process.argv.slice(2));

    logInfo("Starting Unsplash sync");
    logInfo(`Output directory: ${resolve(options.outputDir)}`);
    logInfo(`Global delay between items: ${GLOBAL_DELAY_MS}ms`);
    logInfo(
        `Conservative fallback rate limit: ${UNSPLASH_DEFAULT_DEMO_RATE_LIMIT_PER_HOUR} API requests/hour`,
    );

    const baseOutputDir = resolve(options.outputDir);
    const downloadsDir = join(baseOutputDir, options.downloadsDir);
    const metadataDir = join(baseOutputDir, options.metadataDir);
    const manifestPath = join(baseOutputDir, options.manifestFile);
    const statusPath = join(baseOutputDir, options.statusFile);

    await ensureDirectory(baseOutputDir);
    await ensureDirectory(downloadsDir);
    await ensureDirectory(metadataDir);

    await assertNotBlocked(statusPath);
    await clearStatusFile(statusPath);

    logInfo("Validating environment variables");
    getRequiredEnv("UNSPLASH_ACCESS_KEY");
    getRequiredEnv("UNSPLASH_AUTHORIZATION_TOKEN");

    const manifest = await readJsonFile<Manifest>(
        manifestPath,
        createEmptyManifest(options.historyUrl),
    );

    currentRateLimitState = manifest.rateLimit ?? currentRateLimitState;

    const session = await createBrowserSession(options.verbose);

    try {
        const page = await session.context.newPage();

        await assertUnsplashLogin(page);

        const photoIds = await collectDownloadedPhotoIds(
            page,
            options.historyUrl,
            options.maxScrollRounds,
            options.scrollPauseMs,
            options.verbose,
        );

        logInfo(`Found ${photoIds.length} photo ids`);

        if (photoIds.length === 0) {
            logWarn("No photo ids found on the downloads page");
        }

        for (const [index, photoId] of photoIds.entries()) {
            await sleep(GLOBAL_DELAY_MS);

            logInfo(`[${index + 1}/${photoIds.length}] Processing ${photoId}`);

            try {
                const photoJsonPath = join(metadataDir, `${photoId}.json`);
                const photo = await loadPhotoFromCacheOrApi(
                    photoId,
                    photoJsonPath,
                    options.refreshAll,
                    options.verbose,
                );

                if (photo.id !== photoId) {
                    logWarn(`Photo id mismatch: requested ${photoId}, got ${photo.id}`);
                }

                await writeJsonFile(photoJsonPath, photo);
                const photoJsonSha256 = await computeFileSha256(photoJsonPath);

                const trackedDownloadUrl = await fetchTrackedDownloadUrl(photo.id, options.verbose);
                const headInfo = await headUrl(
                    trackedDownloadUrl,
                    options.headTimeoutMs,
                    options.verbose,
                    `head:${photo.id}`,
                );

                const extension = inferExtensionFromContentType(headInfo.contentType);
                const imageBaseName = buildImageBaseName(photo);
                const imagePath = join(downloadsDir, `${imageBaseName}${extension}`);

                const previousEntry = manifest.entries[photo.id];

                if (
                    shouldSkipDownload(
                        previousEntry,
                        photo,
                        headInfo,
                        imagePath,
                        options.refreshAll,
                    )
                ) {
                    logInfo(`Skipping unchanged image: ${basename(imagePath)}`);

                    manifest.entries[photo.id] = {
                        id: photo.id,
                        photoJsonFile: photoJsonPath,
                        imageFile: imagePath,
                        photoUpdatedAt: photo.updated_at ?? null,
                        photoJsonSha256,
                        imageSha256:
                            previousEntry?.imageSha256 ??
                            (existsSync(imagePath) ? await computeFileSha256(imagePath) : null),
                        imageSizeBytes: previousEntry?.imageSizeBytes ?? null,
                        imageContentType: headInfo.contentType,
                        imageContentLength: headInfo.contentLength,
                        imageEtag: headInfo.etag,
                        imageLastModified: headInfo.lastModified,
                        photoHtmlUrl: photo.links?.html ?? null,
                        lastSyncedAt: new Date().toISOString(),
                    };

                    manifest.generatedAt = new Date().toISOString();
                    manifest.rateLimit = currentRateLimitState;
                    await writeJsonFile(manifestPath, manifest);
                    continue;
                }

                const downloadResult = await downloadFile(
                    trackedDownloadUrl,
                    imagePath,
                    options.downloadTimeoutMs,
                    options.verbose,
                    `download:${photo.id}`,
                );

                logInfo(`Downloaded ${basename(imagePath)}`);

                manifest.entries[photo.id] = {
                    id: photo.id,
                    photoJsonFile: photoJsonPath,
                    imageFile: imagePath,
                    photoUpdatedAt: photo.updated_at ?? null,
                    photoJsonSha256,
                    imageSha256: downloadResult.sha256,
                    imageSizeBytes: downloadResult.sizeBytes,
                    imageContentType: downloadResult.contentType,
                    imageContentLength: downloadResult.contentLength,
                    imageEtag: downloadResult.etag,
                    imageLastModified: downloadResult.lastModified,
                    photoHtmlUrl: photo.links?.html ?? null,
                    lastSyncedAt: new Date().toISOString(),
                };

                manifest.generatedAt = new Date().toISOString();
                manifest.rateLimit = currentRateLimitState;
                await writeJsonFile(manifestPath, manifest);
            } catch (error: unknown) {
                const message = error instanceof Error ? error.stack ?? error.message : String(error);
                logError(`Failed for ${photoId}: ${message}`);

                if (isRateLimitMessage(message)) {
                    await setRateLimitedStatus(
                        statusPath,
                        "Unsplash API rate limit reached",
                        options.verbose,
                    );

                    manifest.generatedAt = new Date().toISOString();
                    manifest.rateLimit = currentRateLimitState;
                    await writeJsonFile(manifestPath, manifest);

                    logWarn("Stopping now to avoid making the rate-limited situation worse.");
                    return;
                }

                manifest.generatedAt = new Date().toISOString();
                manifest.rateLimit = currentRateLimitState;
                await writeJsonFile(manifestPath, manifest);
            }
        }

        manifest.generatedAt = new Date().toISOString();
        manifest.rateLimit = currentRateLimitState;
        await writeJsonFile(manifestPath, manifest);

        await clearStatusFile(statusPath);

        logInfo(`Done. Manifest: ${manifestPath}`);
    } finally {
        await session.context.close();

        if (session.browser) {
            await session.browser.close();
        }
    }
}

void main().catch((error: unknown) => {
    const message = error instanceof Error ? error.stack ?? error.message : String(error);
    logError(message);
    process.exitCode = 1;
});