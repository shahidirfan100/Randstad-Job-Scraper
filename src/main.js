// src/main.js
import { Actor, log } from 'apify';
import { CheerioCrawler, PlaywrightCrawler, Dataset } from 'crawlee';

const BASE_URL = 'https://www.randstad.com';

const normalizeUrl = (href) => {
    try {
        return new URL(href, BASE_URL).href;
    } catch {
        return null;
    }
};

const clean = (text) => {
    if (!text) return null;
    const t = String(text).replace(/\s+/g, ' ').trim();
    return t || null;
};

Actor.main(async () => {
    const input = (await Actor.getInput()) || {};

    const {
        // Default: global jobs in English
        startUrl = 'https://www.randstad.com/jobs/l-english/',
        results_wanted = 50,
        maxPages = 20,
        collectDetails = true,
        proxyConfiguration,
    } = input;

    const target = Number.isFinite(+results_wanted)
        ? Math.max(1, +results_wanted)
        : 50;
    const maxPagesNum = Number.isFinite(+maxPages)
        ? Math.max(1, +maxPages)
        : 20;

    const proxyConf = await Actor.createProxyConfiguration(proxyConfiguration);

    const detailUrls = new Set();
    let listLinksCollected = 0;
    let detailSaved = 0;

    log.info(
        `Randstad.com scraper starting ⇒ startUrl=${startUrl}, target=${target}, maxPages=${maxPagesNum}, collectDetails=${collectDetails}`,
    );

    // ---------------------------------------------------------------------
    // PHASE 1: LIST PAGES (CheerioCrawler)
    // ---------------------------------------------------------------------
    const cheerioCrawler = new CheerioCrawler({
        proxyConfiguration: proxyConf,
        maxConcurrency: 10,
        requestHandlerTimeoutSecs: 45,
        maxRequestRetries: 2,

        async requestHandler({ request, $, enqueueLinks, log: crawlerLog }) {
            if (!$) {
                crawlerLog.warning(`No Cheerio handle for ${request.url}`);
                return;
            }

            const url = request.url;
            const u = new URL(url);
            const pageParam = u.searchParams.get('page');
            const pageNo = pageParam ? parseInt(pageParam, 10) : 1;

            const links = new Set();

            // Grab all job-detail-like links: /jobs/<slug>...
            $('a[href^="/jobs/"]').each((_, el) => {
                const href = $(el).attr('href');
                if (!href) return;

                // Skip generic nav links like “all jobs”, which are typically /jobs/ only
                if (href === '/jobs' || href === '/jobs/') return;

                const abs = normalizeUrl(href);
                if (!abs) return;

                // Make sure it's actually under /jobs/
                if (!abs.includes('/jobs/')) return;

                // Avoid obvious duplicates
                links.add(abs);
            });

            crawlerLog.info(
                `LIST page ${pageNo}: found ${links.size} /jobs/ links (so far detailUrls=${detailUrls.size}, target=${target})`,
            );

            for (const jobUrl of links) {
                if (detailUrls.size >= target) break;
                if (!detailUrls.has(jobUrl)) {
                    detailUrls.add(jobUrl);
                    listLinksCollected++;
                }
            }

            crawlerLog.info(
                `Accumulated ${detailUrls.size} unique DETAIL URLs so far.`,
            );

            if (detailUrls.size >= target) {
                crawlerLog.info(
                    `Reached requested number of jobs (${target}), stopping pagination.`,
                );
                return;
            }

            if (pageNo >= maxPagesNum) {
                crawlerLog.info(
                    `Reached maxPages=${maxPagesNum}, not enqueueing next LIST page.`,
                );
                return;
            }

            // --- Pagination strategy for randstad.com ---
            // URLs look like: /jobs/l-english/ or /jobs/l-english/?page=2
            const nextUrlObj = new URL(url);
            const currentPage = pageParam ? parseInt(pageParam, 10) : 1;
            nextUrlObj.searchParams.set('page', String(currentPage + 1));
            const nextUrl = nextUrlObj.href;

            crawlerLog.info(`Enqueueing next LIST page: ${nextUrl}`);
            await enqueueLinks({ urls: [nextUrl] });
        },

        failedRequestHandler: async ({ request, error }) => {
            log.error(`LIST request failed ${request.url}: ${error.message}`);
        },
    });

    // ---------------------------------------------------------------------
    // PHASE 2: DETAIL PAGES (PlaywrightCrawler)
    // ---------------------------------------------------------------------
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
                // Block heavy resources to speed up & reduce fingerprinting surface
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
                    // custom flag to avoid re-registering
                    page._randstadRouteSetup = true;
                }

                gotoOptions.waitUntil = 'domcontentloaded';
            },
        ],

        async requestHandler({ request, page, log: crawlerLog }) {
            if (detailSaved >= target) return;

            crawlerLog.info(`DETAIL page: ${request.url}`);

            // Job pages usually show <h1> quickly; still, don't crash if not
            await page
                .waitForSelector('h1', { timeout: 10000 })
                .catch(() => null);

            const data = await page.evaluate(() => {
                const clean = (t) =>
                    (t || '').replace(/\s+/g, ' ').trim() || null;

                // Title
                const title = clean(
                    document.querySelector('h1')?.innerText || '',
                );

                // SUMMARY section (location, salary, type)
                let location = null;
                let salary = null;
                let contractType = null;

                const headings = Array.from(
                    document.querySelectorAll('h2, h3'),
                );
                const summaryHeading = headings.find((h) =>
                    (h.innerText || '')
                        .toLowerCase()
                        .includes('summary'),
                );

                if (summaryHeading) {
                    // Typically the summary bullets are the first <ul> after "summary"
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

                        if (items.length > 0) {
                            location = items[0] || null;
                        }

                        const salaryItem = items.find((t) =>
                            /€|\$|£|per hour|per month|per year|per annum/i.test(
                                t || '',
                            ),
                        );
                        if (salaryItem) salary = salaryItem;

                        const contractItem = items.find((t) =>
                            /(temporary|contract|permanent|interim|internship|apprenticeship|student|temp to perm)/i.test(
                                t || '',
                            ),
                        );
                        if (contractItem) contractType = contractItem;
                    }
                }

                // JOB DETAILS description
                let descriptionText = null;

                const detailsHeading = headings.find((h) =>
                    (h.innerText || '')
                        .toLowerCase()
                        .includes('job details'),
                );

                if (detailsHeading) {
                    const parts = [];
                    let el = detailsHeading.nextElementSibling;

                    while (el && !/^H[23]$/i.test(el.tagName)) {
                        const txt = clean(el.innerText || '');
                        if (
                            txt &&
                            txt.length > 40 && // avoid tiny crumbs
                            !/cookies|privacy|javascript to run this app/i.test(
                                txt,
                            )
                        ) {
                            parts.push(txt);
                        }
                        el = el.nextElementSibling;
                    }

                    if (parts.length) {
                        descriptionText = parts.join('\n\n');
                    }
                }

                // Date posted, close date from the "posted today / closes ..." area
                let posted = null;
                let closes = null;

                const mainText = document.body.innerText || '';

                const postedMatch = mainText.match(
                    /posted\s+([0-9]{1,2}\s+\w+\s+[0-9]{4}|today|yesterday)/i,
                );
                if (postedMatch) posted = clean(postedMatch[0]);

                const closesMatch = mainText.match(
                    /closes\s+([0-9]{1,2}\s+\w+\s+[0-9]{4})/i,
                );
                if (closesMatch) closes = clean(closesMatch[0]);

                return {
                    title,
                    location,
                    salary,
                    contractType,
                    descriptionText,
                    posted,
                    closes,
                };
            });

            const item = {
                title: clean(data.title),
                location: clean(data.location),
                salary: clean(data.salary),
                contract_type: clean(data.contractType),
                date_info: {
                    posted: clean(data.posted),
                    closes: clean(data.closes),
                },
                description_text: clean(data.descriptionText),
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
            log.error(`DETAIL request failed ${request.url}: ${error.message}`);
        },
    });

    // ---------------------------------------------------------------------
    // RUN BOTH PHASES
    // ---------------------------------------------------------------------
    log.info('Phase 1: LIST (CheerioCrawler)');
    await cheerioCrawler.run([
        {
            url: startUrl,
            userData: { label: 'LIST' },
        },
    ]);

    log.info(
        `LIST phase finished. Collected ${detailUrls.size} DETAIL URLs (requested ${target}).`,
    );

    if (collectDetails && detailUrls.size > 0) {
        log.info('Phase 2: DETAIL (PlaywrightCrawler)');
        await playwrightCrawler.run(
            Array.from(detailUrls).map((u) => ({
                url: u,
                userData: { label: 'DETAIL' },
            })),
        );
    } else if (collectDetails) {
        log.warning(
            'DETAIL phase skipped because no detail URLs were collected.',
        );
    }

    log.info('=== Randstad.com scraping DONE ===');
    log.info(`Total detail jobs saved: ${detailSaved}`);
});
