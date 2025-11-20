// Randstad.com jobs scraper: Cheerio for LIST pages, Playwright for DETAIL pages
import { Actor, log } from 'apify';
import { CheerioCrawler, PlaywrightCrawler, Dataset } from 'crawlee';

const BASE_URL = 'https://www.randstad.com';

const toAbs = (href, base = BASE_URL) => {
    try {
        return new URL(href, base).href;
    } catch {
        return null;
    }
};

const clean = (text) => {
    if (text == null) return null;
    const t = String(text).replace(/\s+/g, ' ').trim();
    return t || null;
};

// Decide if a URL path looks like a job DETAIL page.
// Example detail: /jobs/accounting-analyst_alges_46025627/
// Example NON-detail: /jobs/s-accounting-auditing/
const isJobDetailPath = (path) => {
    try {
        if (!path) return false;
        const segments = path.split('/').filter(Boolean);
        if (segments[0] !== 'jobs') return false;

        const lastSeg = segments[segments.length - 1];
        // Detail pages always end in something like ..._123456 or ..._town_123456
        return /.+_\d+$/.test(lastSeg);
    } catch {
        return false;
    }
};

// Build next pagination URL for randstad.com LIST pages.
// E.g. /jobs/s-accounting-auditing/ -> /jobs/s-accounting-auditing/page-2/
//      /jobs/s-accounting-auditing/page-2/ -> /jobs/s-accounting-auditing/page-3/
const buildNextPageUrl = (urlStr) => {
    const u = new URL(urlStr);
    const path = u.pathname;
    const m = path.match(/\/page-(\d+)\/?$/);
    let nextPath;

    if (m) {
        const current = parseInt(m[1], 10) || 1;
        nextPath = path.replace(/\/page-\d+\/?$/, `/page-${current + 1}/`);
    } else {
        const basePath = path.endsWith('/') ? path : `${path}/`;
        nextPath = `${basePath}page-2/`;
    }

    u.pathname = nextPath;
    return u.toString();
};

Actor.main(async () => {
    const input = (await Actor.getInput()) || {};

    const {
        // Works with any Randstad.com jobs listing, e.g.:
        //  - https://www.randstad.com/jobs/
        //  - https://www.randstad.com/jobs/s-accounting-auditing/
        //  - https://www.randstad.com/jobs/l-english/
        startUrl = 'https://www.randstad.com/jobs/',
        results_wanted = 50,
        maxPages = 10,
        collectDetails = true,
        proxyConfiguration,
    } = input;

    const target = Number.isFinite(+results_wanted)
        ? Math.max(1, +results_wanted)
        : 50;

    const maxPagesNum = Number.isFinite(+maxPages)
        ? Math.max(1, +maxPages)
        : 10;

    const proxyConf = await Actor.createProxyConfiguration(proxyConfiguration);

    const detailUrls = new Set();
    let detailSaved = 0;

    log.info(
        `Randstad.com hybrid scraper starting → startUrl=${startUrl}, target=${target}, maxPages=${maxPagesNum}, collectDetails=${collectDetails}`,
    );

    // -------------------------------------------------------------------------
    // PHASE 1: LIST PAGES (Cheerio – fast, no JS)
    // -------------------------------------------------------------------------
    const cheerioCrawler = new CheerioCrawler({
        proxyConfiguration: proxyConf,
        maxConcurrency: 10,
        requestHandlerTimeoutSecs: 40,
        maxRequestRetries: 2,

        async requestHandler({ request, $, enqueueLinks, log: crawlerLog }) {
            if (!$) {
                crawlerLog.warning(`No Cheerio handle for ${request.url}`);
                return;
            }

            const url = request.url;
            const u = new URL(url);
            const pageMatch = u.pathname.match(/\/page-(\d+)\/?$/);
            const pageNo = pageMatch ? parseInt(pageMatch[1], 10) || 1 : 1;

            const links = new Set();

            // Collect ONLY job-detail-like URLs, NOT category/search pages.
            $('a[href^="/jobs/"], a[href*="://www.randstad.com/jobs/"]').each(
                (_, el) => {
                    const href = $(el).attr('href');
                    if (!href) return;

                    const abs = toAbs(href, BASE_URL);
                    if (!abs) return;

                    let path;
                    try {
                        path = new URL(abs).pathname;
                    } catch {
                        return;
                    }

                    if (!isJobDetailPath(path)) return;

                    links.add(abs);
                },
            );

            crawlerLog.info(
                `LIST page ${pageNo}: found ${links.size} job DETAIL links (collected so far=${detailUrls.size}, target=${target})`,
            );

            for (const jobUrl of links) {
                if (detailUrls.size >= target) break;
                if (!detailUrls.has(jobUrl)) detailUrls.add(jobUrl);
            }

            crawlerLog.info(
                `Total unique DETAIL URLs after page ${pageNo}: ${detailUrls.size}`,
            );

            if (detailUrls.size >= target) {
                crawlerLog.info(
                    `Reached requested number of jobs (${target}), stopping LIST pagination.`,
                );
                return;
            }

            if (pageNo >= maxPagesNum) {
                crawlerLog.info(
                    `Reached maxPages=${maxPagesNum}, not enqueueing more LIST pages.`,
                );
                return;
            }

            const nextUrl = buildNextPageUrl(url);
            crawlerLog.info(`Enqueueing next LIST page: ${nextUrl}`);
            await enqueueLinks({ urls: [nextUrl] });
        },

        failedRequestHandler: async ({ request, error }) => {
            log.error(`LIST failed ${request.url}: ${error.message}`);
        },
    });

    // -------------------------------------------------------------------------
    // PHASE 2: DETAIL PAGES (Playwright – rich data, still optimized)
    // -------------------------------------------------------------------------
    const playwrightCrawler = new PlaywrightCrawler({
        proxyConfiguration: proxyConf,
        maxConcurrency: 8,
        maxRequestRetries: 1,
        requestHandlerTimeoutSecs: 60,

        launchContext: {
            useIncognitoPages: true,
            launchOptions: {
                headless: true,
                args: [
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-background-networking',
                    '--disable-background-timer-throttling',
                    '--disable-breakpad',
                    '--disable-client-side-phishing-detection',
                    '--disable-default-apps',
                    '--disable-extensions',
                    '--disable-hang-monitor',
                    '--disable-ipc-flooding-protection',
                    '--disable-popup-blocking',
                    '--disable-prompt-on-repost',
                    '--disable-renderer-backgrounding',
                    '--disable-sync',
                    '--disable-translate',
                    '--metrics-recording-only',
                    '--no-first-run',
                    '--lang=en-US',
                ],
            },
        },

        browserPoolOptions: {
            useFingerprints: true,
            fingerprintOptions: {
                locales: ['en-US'],
                browsers: ['chromium'],
            },
            retireBrowserAfterPageCount: 50,
            maxOpenPagesPerBrowser: 2,
        },

        preNavigationHooks: [
            async ({ page }, gotoOptions) => {
                // Block heavy resources for speed & stealth
                if (!page._randstadRouteSetup) {
                    await page.route('**/*', (route) => {
                        const type = route.request().resourceType();
                        if (
                            ['image', 'stylesheet', 'font', 'media'].includes(
                                type,
                            )
                        ) {
                            route.abort();
                        } else {
                            route.continue();
                        }
                    });
                    // custom flag so we don't re-register on every nav
                    page._randstadRouteSetup = true;
                }

                gotoOptions.waitUntil = 'domcontentloaded';
            },
        ],

        async requestHandler({ request, page, log: crawlerLog }) {
            if (detailSaved >= target) return;

            crawlerLog.info(`DETAIL page: ${request.url}`);

            await page
                .waitForSelector('h1', { timeout: 10000 })
                .catch(() => null);

            const data = await page.evaluate(() => {
                const clean = (t) =>
                    (t || '').replace(/\s+/g, ' ').trim() || null;

                const result = {};

                const h1 = document.querySelector('h1');
                result.title = clean(h1?.innerText);

                // ---- SUMMARY section (location, contract, salary) ----
                const headings = Array.from(
                    document.querySelectorAll('h2, h3'),
                );

                let location = null;
                let contractType = null;
                let salary = null;

                const summaryHeading = headings.find((h) =>
                    (h.innerText || '')
                        .toLowerCase()
                        .includes('summary'),
                );

                if (summaryHeading) {
                    let ul = summaryHeading.nextElementSibling;
                    if (!ul || ul.tagName.toLowerCase() !== 'ul') {
                        ul =
                            summaryHeading.parentElement &&
                            summaryHeading.parentElement.querySelector('ul');
                    }

                    if (ul) {
                        const items = Array.from(
                            ul.querySelectorAll('li'),
                        ).map((li) => clean(li.innerText));

                        if (items.length) {
                            // Usually “city, region” is the first bullet
                            location = items[0] || null;
                        }

                        const contractItem = items.find((t) =>
                            /(temporary|contract|permanent|interim|internship|temp to perm)/i.test(
                                t || '',
                            ),
                        );
                        if (contractItem) contractType = contractItem;

                        const salaryItem = items.find((t) =>
                            /€|\$|£|per hour|per year|per month|per annum/i.test(
                                t || '',
                            ),
                        );
                        if (salaryItem) salary = salaryItem;
                    }
                }

                // ---- JOB DETAILS description ----
                let descriptionText = null;

                const jdHeading = headings.find((h) =>
                    (h.innerText || '')
                        .toLowerCase()
                        .includes('job details'),
                );

                if (jdHeading) {
                    const parts = [];
                    let el = jdHeading.nextElementSibling;

                    while (el && !/^H[23]$/i.test(el.tagName)) {
                        const txt = clean(el.innerText || '');
                        if (
                            txt &&
                            txt.length > 40 &&
                            !/cookies|privacy|javascript to run this app/i.test(
                                txt,
                            )
                        ) {
                            parts.push(txt);
                        }
                        el = el.nextElementSibling;
                    }

                    if (parts.length) descriptionText = parts.join('\n\n');
                }

                // ---- Posted / closes (keep raw string) ----
                const bodyText = document.body.innerText || '';
                let posted = null;
                let closes = null;

                const postedMatch = bodyText.match(/posted\s+[^\n]+/i);
                if (postedMatch) posted = clean(postedMatch[0]);

                const closesMatch = bodyText.match(/closes\s+[^\n]+/i);
                if (closesMatch) closes = clean(closesMatch[0]);

                result.location = location;
                result.contractType = contractType;
                result.salary = salary;
                result.descriptionText = descriptionText;
                result.posted = posted;
                result.closes = closes;

                return result;
            });

            const item = {
                title: clean(data.title),
                location: clean(data.location),
                contract_type: clean(data.contractType),
                salary: clean(data.salary),
                description_text: clean(data.descriptionText),
                date_info: {
                    posted: clean(data.posted),
                    closes: clean(data.closes),
                },
                url: request.url,
                _source: 'randstad.com',
            };

            if (!item.title) {
                crawlerLog.warning(
                    `Skipping DETAIL without title: ${request.url}`,
                );
                return;
            }

            await Dataset.pushData(item);
            detailSaved++;

            crawlerLog.info(
                `Saved job #${detailSaved}/${target}: ${item.title} (${item.location || 'location N/A'})`,
            );
        },

        failedRequestHandler: async ({ request, error }) => {
            log.error(`DETAIL failed ${request.url}: ${error.message}`);
        },
    });

    // -------------------------------------------------------------------------
    // RUN THE HYBRID FLOW
    // -------------------------------------------------------------------------
    log.info('Phase 1: LIST (CheerioCrawler)');
    await cheerioCrawler.run([{ url: startUrl }]);

    log.info(
        `LIST phase finished. Collected ${detailUrls.size} DETAIL URLs (requested ${target}).`,
    );

    if (collectDetails && detailUrls.size > 0) {
        log.info('Phase 2: DETAIL (PlaywrightCrawler)');
        await playwrightCrawler.run(
            Array.from(detailUrls).map((u) => ({ url: u })),
        );
    } else if (collectDetails) {
        log.warning(
            'DETAIL phase skipped because no detail URLs were collected.',
        );
    }

    log.info('=== Randstad.com scraping DONE ===');
    log.info(`Total detail jobs saved: ${detailSaved}`);
});
