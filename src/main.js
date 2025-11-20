// Randstad.com jobs scraper: Cheerio LIST + Playwright DETAIL (clean description & location)
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

// Detect if path is a job detail URL, not a category page
// e.g. /jobs/medical-device-sales-executive-central-region_selangor_45976286/
const isJobDetailPath = (path) => {
    try {
        if (!path) return false;
        const segments = path.split('/').filter(Boolean);
        if (segments[0] !== 'jobs') return false;
        const lastSeg = segments[segments.length - 1];
        return /.+_\d+$/.test(lastSeg); // must end with _digits
    } catch {
        return false;
    }
};

// Pagination helper: /jobs/.../       -> /jobs/.../page-2/
//                    /jobs/.../page-2 -> /jobs/.../page-3/
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
        // Any Randstad.com list URL works, e.g.:
        // "https://www.randstad.com/jobs/", "https://www.randstad.com/jobs/l-english/",
        // "https://www.randstad.com/jobs/s-advertising-marketing-public-relations/"
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
    // PHASE 1: LIST PAGES (Cheerio – collect only real job detail URLs)
    // -------------------------------------------------------------------------
    const cheerioCrawler = new CheerioCrawler({
        proxyConfiguration: proxyConf,
        maxConcurrency: 10,          // already fast & safe
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
                `LIST page ${pageNo}: found ${links.size} job-detail links (total=${detailUrls.size}/${target})`,
            );

            for (const jobUrl of links) {
                if (detailUrls.size >= target) break;
                detailUrls.add(jobUrl);
            }

            crawlerLog.debug(
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
            crawlerLog.debug(`Enqueueing next LIST page: ${nextUrl}`);
            await enqueueLinks({ urls: [nextUrl] });
        },

        failedRequestHandler: async ({ request, error }) => {
            log.error(`LIST failed ${request.url}: ${error.message}`);
        },
    });

    // -------------------------------------------------------------------------
    // PHASE 2: DETAIL PAGES (Playwright – clean location + description_html/text)
    // -------------------------------------------------------------------------
    const playwrightCrawler = new PlaywrightCrawler({
        proxyConfiguration: proxyConf,
        maxConcurrency: 12,              // bumped from 8 → faster, still safe
        maxRequestRetries: 1,
        requestHandlerTimeoutSecs: 40,   // was 60 → fail faster on stuck pages

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
            retireBrowserAfterPageCount: 100, // reuse browsers more
            maxOpenPagesPerBrowser: 3,        // slight parallelism bump
        },

        preNavigationHooks: [
            async ({ page }, gotoOptions) => {
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
                    page._randstadRouteSetup = true;
                }

                gotoOptions.waitUntil = 'domcontentloaded';
            },
        ],

        async requestHandler({ request, page, log: crawlerLog }) {
            if (detailSaved >= target) return;

            crawlerLog.debug(`DETAIL page: ${request.url}`);

            await page
                .waitForSelector('h1', { timeout: 10000 })
                .catch(() => null);

            const data = await page.evaluate(() => {
                const cleanLine = (t) =>
                    (t || '').replace(/\s+/g, ' ').trim();

                function escapeHtml(str) {
                    return String(str)
                        .replace(/&/g, '&amp;')
                        .replace(/</g, '&lt;')
                        .replace(/>/g, '&gt;')
                        .replace(/"/g, '&quot;')
                        .replace(/'/g, '&#39;');
                }

                const body = document.body.innerText || '';
                const lower = body.toLowerCase();

                const result = {};

                // ---------- TITLE ----------
                const h1 = document.querySelector('h1');
                result.title = cleanLine(h1?.innerText || '');

                // ---------- SUMMARY: location + salary + contract_type ----------
                let location = null;
                let salary = null;
                let contractType = null;

                const jdIdx = lower.indexOf('job details');
                if (jdIdx >= 0) {
                    const summaryIdx = lower.indexOf('summary', jdIdx);
                    if (summaryIdx >= 0) {
                        const afterSummaryIdx =
                            summaryIdx + 'summary'.length;
                        const jobCatIdx = lower.indexOf(
                            'job category',
                            afterSummaryIdx,
                        );
                        let summaryEndIdx =
                            jobCatIdx > afterSummaryIdx
                                ? jobCatIdx
                                : afterSummaryIdx + 400;

                        if (summaryEndIdx > body.length) {
                            summaryEndIdx = body.length;
                        }

                        const summaryChunkRaw = body.slice(
                            afterSummaryIdx,
                            summaryEndIdx,
                        );

                        const lines = summaryChunkRaw
                            .split('\n')
                            .map((l) => cleanLine(l))
                            .filter(Boolean);

                        if (lines.length) {
                            location = lines[0];
                        }

                        const salaryLine = lines.find((l) =>
                            /€|\$|£|RM|per hour|per month|per year|per annum/i.test(
                                l,
                            ),
                        );
                        if (salaryLine) salary = salaryLine;

                        const contractLine = lines.find((l) =>
                            /(permanent|temporary|contract|interim|internship|apprenticeship|full[- ]time|part[- ]time)/i.test(
                                l,
                            ),
                        );
                        if (contractLine) contractType = contractLine;
                    }
                }

                result.location = location;
                result.salary = salary;
                result.contractType = contractType;

                // ---------- DESCRIPTION ----------
                let descriptionText = null;
                let descriptionHtml = null;

                const aboutIdx = lower.indexOf('about the company');
                if (aboutIdx >= 0) {
                    const stopCandidates = [];

                    const showMoreIdx = lower.indexOf('show more', aboutIdx);
                    if (showMoreIdx > aboutIdx) stopCandidates.push(showMoreIdx);

                    const appProcIdx = lower.indexOf(
                        'the application process',
                        aboutIdx,
                    );
                    if (appProcIdx > aboutIdx) stopCandidates.push(appProcIdx);

                    const shareIdx = lower.indexOf('share this job', aboutIdx);
                    if (shareIdx > aboutIdx) stopCandidates.push(shareIdx);

                    const relatedIdx = lower.indexOf('related jobs', aboutIdx);
                    if (relatedIdx > aboutIdx) stopCandidates.push(relatedIdx);

                    let endIdx;
                    if (stopCandidates.length) {
                        endIdx = Math.min(...stopCandidates);
                    } else {
                        endIdx = Math.min(aboutIdx + 4000, body.length);
                    }

                    const descRaw = body.slice(aboutIdx, endIdx);
                    const lines = descRaw
                        .split('\n')
                        .map((l) => cleanLine(l))
                        .filter(Boolean);

                    if (lines.length) {
                        descriptionText = lines.join('\n\n');
                        descriptionHtml = lines
                            .map((l) => `<p>${escapeHtml(l)}</p>`)
                            .join('');
                    }
                }

                if (descriptionText && descriptionText.length < 40) {
                    descriptionText = null;
                    descriptionHtml = null;
                }

                result.descriptionText = descriptionText;
                result.descriptionHtml = descriptionHtml;

                // ---------- POSTED / CLOSES ----------
                let posted = null;
                let closes = null;

                const postedMatch = body.match(
                    /posted\s+([0-9]{1,2}\s+\w+\s+[0-9]{4}|today|yesterday)/i,
                );
                if (postedMatch) posted = cleanLine(postedMatch[0]);

                const closesMatch = body.match(
                    /closes\s+([0-9]{1,2}\s+\w+\s+[0-9]{4})/i,
                );
                if (closesMatch) closes = cleanLine(closesMatch[0]);

                result.posted = posted;
                result.closes = closes;

                return result;
            });

            const item = {
                title: clean(data.title),
                location: clean(data.location),
                contract_type: clean(data.contractType),
                salary: clean(data.salary),
                description_text: data.descriptionText
                    ? data.descriptionText.trim()
                    : null,
                description_html: data.descriptionHtml || null,
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

            // Only log progress occasionally instead of every job
            if (detailSaved % 10 === 0 || detailSaved === target) {
                log.info(
                    `Progress: saved ${detailSaved}/${target} jobs. Last: ${item.title}`,
                );
            } else {
                crawlerLog.debug(
                    `Saved job #${detailSaved}/${target}: ${item.title}`,
                );
            }
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
