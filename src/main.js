// Hybrid Randstad scraper: Cheerio for LIST pages, Playwright for DETAIL pages
import { Actor, log } from 'apify';
import { PlaywrightCrawler, CheerioCrawler, Dataset } from 'crawlee';

// ---------- Shared helpers ----------

const toAbs = (href, base = 'https://www.randstad.fr') => {
    try {
        return new URL(href, base).href;
    } catch {
        return null;
    }
};

const cleanText = (text) => {
    if (!text) return '';
    return text.replace(/\s+/g, ' ').trim();
};

const buildStartUrl = (kw, loc, cat) => {
    const u = new URL('https://www.randstad.fr/emploi/');
    // Randstad search might use different parameters, but for now use basic URL
    return u.href;
};

// ---------- MAIN ----------

Actor.main(async () => {
    const input = (await Actor.getInput()) || {};

    const {
        keyword = '',
        location = '',
        category = '',
        results_wanted: RESULTS_WANTED_RAW = 100,
        max_pages: MAX_PAGES_RAW = 999,
        collectDetails = true,
        startUrl,
        startUrls,
        url,
        proxyConfiguration,
    } = input;

    const RESULTS_WANTED = Number.isFinite(+RESULTS_WANTED_RAW)
        ? Math.max(1, +RESULTS_WANTED_RAW)
        : Number.MAX_SAFE_INTEGER;
    const MAX_PAGES = Number.isFinite(+MAX_PAGES_RAW)
        ? Math.max(1, +MAX_PAGES_RAW)
        : 999;

    const proxyConf = await Actor.createProxyConfiguration(proxyConfiguration);

    // Initial LIST URLs
    const initialUrls = [];
    if (Array.isArray(startUrls) && startUrls.length) initialUrls.push(...startUrls);
    if (startUrl) initialUrls.push(startUrl);
    if (url) initialUrls.push(url);
    if (!initialUrls.length) initialUrls.push(buildStartUrl(keyword, location, category));

    let saved = 0;
    const detailUrls = new Set(); // for DETAIL phase

    // ---------- LIST helpers (Cheerio) ----------

    function findJobLinksCheerio($, crawlerLog) {
        const links = new Set();
        const jobLinkRegex = /\/emploi\/[^\/]+_[^\/]+_[^\/]+/i;

        $('h3 a[href*="/emploi/"]').each((_, el) => {
            const href = $(el).attr('href');
            if (!href) return;
            if (!jobLinkRegex.test(href)) return;
            const absoluteUrl = toAbs(href);
            if (absoluteUrl && absoluteUrl.includes('randstad.fr')) {
                links.add(absoluteUrl);
            }
        });

        crawlerLog.info(`Cheerio: found ${links.size} job links on this page`);
        return [...links];
    }

    function buildNextPageUrl(currentUrl) {
        // Randstad might use different pagination. For now, assume page parameter
        const u = new URL(currentUrl);
        const currentPage = parseInt(u.searchParams.get('page') || '1', 10);
        u.searchParams.set('page', String(currentPage + 1));
        return u.href;
    }

    // ---------- CheerioCrawler (LIST pages) ----------

    const cheerioCrawler = new CheerioCrawler({
        proxyConfiguration: proxyConf,
        maxRequestRetries: 2,
        maxConcurrency: 20, // Cheerio is cheap
        requestHandlerTimeoutSecs: 30,
        async requestHandler({ request, $, enqueueLinks, log: crawlerLog }) {
            const label = request.userData?.label || 'LIST';
            const pageNo = request.userData?.pageNo || 1;
            if (label !== 'LIST') return;

            const links = findJobLinksCheerio($, crawlerLog);
            crawlerLog.info(
                `LIST page ${pageNo}: ${links.length} job links (saved=${saved}, target=${RESULTS_WANTED}, collectedDetails=${detailUrls.size})`,
            );

            if (links.length === 0) {
                crawlerLog.warning(`No job links found on page ${pageNo}`);
                if (pageNo > 1) {
                    crawlerLog.warning(`Stopping pagination at page ${pageNo}`);
                    return;
                }
            }

            if (collectDetails) {
                for (const link of links) {
                    if (detailUrls.size >= RESULTS_WANTED) break;
                    detailUrls.add(link);
                }
            } else {
                const remaining = RESULTS_WANTED - saved;
                const toPush = links.slice(0, Math.max(0, remaining));
                if (toPush.length) {
                    await Dataset.pushData(
                        toPush.map((u) => ({ url: u, _source: 'randstad.fr' })),
                    );
                    saved += toPush.length;
                }
            }

            if (collectDetails && detailUrls.size >= RESULTS_WANTED) {
                crawlerLog.info(
                    `Collected enough detail URLs (${detailUrls.size}), not enqueueing more pages.`,
                );
                return;
            }

            if (pageNo < MAX_PAGES && links.length > 0) {
                const nextUrl = buildNextPageUrl(request.url);
                await enqueueLinks({
                    urls: [nextUrl],
                    userData: { label: 'LIST', pageNo: pageNo + 1 },
                });
            }
        },
    });

    // ---------- PlaywrightCrawler (DETAIL pages) ----------

    const playwrightCrawler = new PlaywrightCrawler({
        proxyConfiguration: proxyConf,
        useSessionPool: true,
        sessionPoolOptions: {
            maxPoolSize: 30,
            sessionOptions: {
                maxUsageCount: 50,
                maxAgeSecs: 24 * 60 * 60,
            },
        },
        persistCookiesPerSession: true,
        // Give autoscaler headroom; it will back off if CPU is too high
        maxConcurrency: 25,
        minConcurrency: 5,
        maxRequestRetries: 2,
        requestHandlerTimeoutSecs: 30,
        navigationTimeoutSecs: 15,
        launchContext: {
            launchOptions: {
                headless: true,
                args: [
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-blink-features=AutomationControlled',
                    '--mute-audio',
                    '--disable-background-networking',
                    '--disable-background-timer-throttling',
                    '--disable-backgrounding-occluded-windows',
                    '--disable-sync',
                    '--metrics-recording-only',
                    '--no-first-run',
                    '--lang=fr-FR',
                ],
            },
        },
        browserPoolOptions: {
            useFingerprints: true,
            fingerprintOptions: {
                locales: ['fr-FR'],
                browsers: ['chromium'],
                timeZones: ['Europe/Paris'],
            },
            retireBrowserAfterPageCount: 60,
            maxOpenPagesPerBrowser: 2,
        },
        preNavigationHooks: [
            async ({ page }, gotoOptions) => {
                // Block heavy resources for speed
                await page.route('**/*', (route) => {
                    const type = route.request().resourceType();
                    if (['image', 'stylesheet', 'font', 'media'].includes(type)) {
                        route.abort();
                    } else {
                        route.continue();
                    }
                });

                // We only need the DOM, not full load
                gotoOptions.waitUntil = 'domcontentloaded';
            },
        ],
        failedRequestHandler: async ({ request, error }) => {
            log.error(`DETAIL failed ${request.url}: ${error.message}`);
        },
        async requestHandler({ request, page, log: crawlerLog }) {
            if (saved >= RESULTS_WANTED) return;

            try {
                // Cookie banner - Randstad might have different banner
                try {
                    await page.click('#cookie-accept, [data-testid="cookie-accept"]', { timeout: 1500 });
                    await page.waitForTimeout(150);
                } catch {
                    // ignore
                }

                await page.waitForSelector('h1', { timeout: 7000 }).catch(() => {});

                // Expand truncated description if possible
                try {
                    const toggleBtn = await page.$(
                        'button[data-truncate-text-target="toggleButton"], button[data-action*="truncate-text#toggle"], button[aria-expanded]',
                    );
                    if (toggleBtn) {
                        await toggleBtn.click({ timeout: 1500 }).catch(() => {});
                        await page.waitForTimeout(120);
                    }
                } catch {
                    // ignore
                }

                const data = await page.evaluate(() => {
                    const result = {};

                    // Build new HTML tree with ONLY text tags (no section/div/svg/etc)
                    function extractTextualHtml(rootEl) {
                        if (!rootEl) return '';
                        const allowedInline = ['strong', 'b', 'em', 'i', 'br'];
                        const allowedBlock = ['p', 'ul', 'ol', 'li', 'h1', 'h2', 'h3', 'h4'];

                        const doc = document.implementation.createHTMLDocument('');
                        const outRoot = doc.createElement('div');

                        function appendNode(sourceNode, targetParent) {
                            if (sourceNode.nodeType === Node.TEXT_NODE) {
                                const text = sourceNode.nodeValue;
                                if (text && text.trim()) {
                                    targetParent.appendChild(doc.createTextNode(text));
                                }
                                return;
                            }
                            if (sourceNode.nodeType !== Node.ELEMENT_NODE) return;

                            const tag = sourceNode.nodeName.toLowerCase();

                            if (
                                allowedInline.includes(tag) ||
                                allowedBlock.includes(tag)
                            ) {
                                const newEl = doc.createElement(tag);
                                targetParent.appendChild(newEl);
                                for (const child of Array.from(sourceNode.childNodes)) {
                                    appendNode(child, newEl);
                                }
                                return;
                            }

                            // Disallowed tag: flatten children into parent
                            for (const child of Array.from(sourceNode.childNodes)) {
                                appendNode(child, targetParent);
                            }
                        }

                        appendNode(rootEl, outRoot);
                        return outRoot.innerHTML.trim();
                    }

                    // Remove cookie/consent banners
                    const bannersToRemove = [
                        '#cookie-banner',
                        '[class*="cookie"]',
                        '[class*="consent"]',
                        '[id*="cookie"]',
                        '[id*="consent"]',
                    ];
                    bannersToRemove.forEach((sel) => {
                        document.querySelectorAll(sel).forEach((el) => el.remove());
                    });

                    // Title
                    const h1 = document.querySelector('h1');
                    if (h1) {
                        result.title = h1.innerText.trim();
                    } else {
                        result.title = null;
                    }

                    // Company - Randstad is the agency, client might be mentioned
                    result.company = 'Randstad';

                    // Location
                    const locationEl = document.querySelector('[data-cy="job-location"], .location, [itemprop="jobLocation"]');
                    if (locationEl) {
                        result.location = locationEl.innerText.trim();
                    } else {
                        // Try to find in summary
                        const summary = document.querySelector('.summary, .job-summary');
                        if (summary) {
                            const locMatch = summary.innerText.match(/([A-Za-z\s]+,\s*[A-Za-z\s]+)/);
                            if (locMatch) result.location = locMatch[1].trim();
                        }
                    }

                    // === DESCRIPTION: focus on specific sections for Randstad ===

                    const descElements = [];

                    // Main description sections
                    const descSelectors = [
                        '[data-cy="job-description"]',
                        '.descriptif-du-poste',
                        '.profil-recherche',
                        '.a-propos-de-notre-client',
                        'section h3:contains("descriptif") + *',
                        'section h3:contains("profil") + *',
                        'section h3:contains("client") + *',
                    ];

                    descSelectors.forEach((sel) => {
                        document.querySelectorAll(sel).forEach((el) => {
                            descElements.push(el);
                        });
                    });

                    // Fallback if nothing found
                    if (!descElements.length) {
                        const fallbackSelectors = [
                            'article',
                            '.content',
                        ];
                        fallbackSelectors.forEach((sel) => {
                            document.querySelectorAll(sel).forEach((el) => {
                                descElements.push(el);
                            });
                        });
                    }

                    let descriptionText = '';
                    let descriptionHtml = '';

                    for (const el of descElements) {
                        const text = el.innerText.trim();
                        if (
                            text.length > 80 &&
                            !/traceur|cookie|consentement|GDPR/i.test(text)
                        ) {
                            const sanitized = extractTextualHtml(el);
                            if (sanitized) {
                                descriptionHtml += sanitized + '\n';
                                descriptionText += text + '\n';
                            }
                        }
                        if (descriptionText.length > 250) break; // keep it light
                    }

                    result.description_html = descriptionHtml.trim() || null;
                    result.description_text = descriptionText.trim() || null;

                    const bodyText = document.body.innerText || '';

                    const dateMatch = bodyText.match(/publié le (\d{1,2} \w+ \d{4})/i);
                    result.date_posted = dateMatch ? dateMatch[1] : null;

                    const salaryMatch = bodyText.match(/(\d+(?:[.,]\d+)?\s*€\s*(?:par heure|par mois|par année))/i);
                    result.salary = salaryMatch ? salaryMatch[1].trim() : null;

                    const contractMatch = bodyText.match(/(cdi|cdd|intérim|stage|freelance)/i);
                    result.contract_type = contractMatch ? contractMatch[1] : null;

                    return result;
                });

                const item = {
                    title: cleanText(data.title) || null,
                    company: cleanText(data.company) || null,
                    location: cleanText(data.location) || null,
                    salary: cleanText(data.salary) || null,
                    contract_type: cleanText(data.contract_type) || null,
                    date_posted: cleanText(data.date_posted) || null,
                    description_html: data.description_html || null, // text-only tags only
                    description_text: cleanText(data.description_text) || null,
                    url: request.url,
                };

                if (item.title) {
                    await Dataset.pushData(item);
                    saved++;
                    crawlerLog.info(
                        `Saved job #${saved}: ${item.title} (${item.company || 'Unknown company'})`,
                    );
                } else {
                    crawlerLog.warning(`Missing title for DETAIL page: ${request.url}`);
                }
            } catch (err) {
                crawlerLog.error(`DETAIL handler error ${request.url}: ${err.message}`);
            }
        },
    });

    // ---------- RUN HYBRID FLOW ----------

    log.info(
        `Starting HYBRID scraper with ${initialUrls.length} initial URL(s); target=${RESULTS_WANTED}, maxPages=${MAX_PAGES}`,
    );
    initialUrls.forEach((u, i) => log.info(`Initial URL ${i + 1}: ${u}`));

    log.info('Phase 1: CheerioCrawler (LIST pages, fast)');
    await cheerioCrawler.run(
        initialUrls.map((u) => ({
            url: u,
            userData: { label: 'LIST', pageNo: 1 },
        })),
    );

    const detailArray = Array.from(detailUrls);
    log.info(`LIST phase finished. Detail URLs collected: ${detailArray.length}`);

    if (collectDetails && detailArray.length > 0) {
        log.info('Phase 2: PlaywrightCrawler (DETAIL pages, high concurrency)');
        await playwrightCrawler.run(
            detailArray.map((u) => ({
                url: u,
            })),
        );
    } else if (collectDetails) {
        log.warning('DETAIL phase skipped: no detail URLs were collected.');
    }

    log.info('=== HYBRID SCRAPING COMPLETED ===');
    log.info(`Total jobs saved: ${saved}`);
    log.info(`Target was: ${RESULTS_WANTED}`);
    if (saved === 0) {
        log.error(
            'WARNING: No jobs were scraped. Check selectors, blocking, or recent DOM changes on Randstad.',
        );
    }
});
