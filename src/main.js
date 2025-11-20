import { Actor, log } from 'apify';
import { PlaywrightCrawler, Dataset, gotScraping } from 'crawlee';

// --- Randstad.com job API builder ---
function buildApiUrl(page = 1, country = "english") {
    return `https://www.randstad.com/jobs/api/search?language=en&location=${country}&page=${page}`;
}

async function fetchApiPage(page, country) {
    const url = buildApiUrl(page, country);
    log.info(`Fetching API page: ${url}`);

    try {
        const res = await gotScraping({
            url,
            responseType: 'json',
            timeout: 15000
        });
        return res.body;
    } catch (err) {
        log.error(`API request failed: ${err.message}`);
        return null;
    }
}

Actor.main(async () => {
    const input = (await Actor.getInput()) || {};

    const {
        results_wanted = 50,
        country = "english",
        collectDetails = true,
        proxyConfiguration
    } = input;

    const proxyConf = await Actor.createProxyConfiguration(proxyConfiguration);
    let saved = 0;
    const detailUrls = [];

    log.info(`Starting Randstad.com scraper → target = ${results_wanted}`);

    // -------------------- PHASE 1: API LIST SCRAPING ----------------------
    let page = 1;
    while (saved < results_wanted) {
        const data = await fetchApiPage(page, country);
        if (!data || !data.jobs || data.jobs.length === 0) {
            log.warning(`No more job results at page ${page}`);
            break;
        }

        for (const job of data.jobs) {
            if (saved >= results_wanted) break;

            const detailUrl = `https://www.randstad.com/jobs/${job.id}/`;
            detailUrls.push(detailUrl);

            // Save LIST info immediately (optional)
            await Dataset.pushData({
                title: job.title,
                location: job.location?.city || null,
                country: job.location?.country || null,
                company: job.company || null,
                date_posted: job.publishedDate || null,
                url: detailUrl,
                _source: "list-api"
            });

            saved++;
        }

        log.info(`Collected ${saved} basic LIST items so far…`);
        page++;
    }

    log.info(`LIST phase complete → ${detailUrls.length} detail URLs`);

    // -------------------- PHASE 2: DETAIL SCRAPING -----------------------
    if (collectDetails) {
        const crawler = new PlaywrightCrawler({
            proxyConfiguration: proxyConf,
            maxConcurrency: 20,
            requestHandlerTimeoutSecs: 30,

            async requestHandler({ request, page, log: crawlerLog }) {
                crawlerLog.info(`DETAIL: ${request.url}`);

                await page.waitForSelector("h1", { timeout: 8000 }).catch(() => {});

                const data = await page.evaluate(() => {
                    const clean = (t) => t?.replace(/\s+/g, " ").trim() || null;

                    const title = clean(document.querySelector("h1")?.innerText);

                    const company = clean(
                        document.querySelector("[data-testid='job-company']")?.innerText
                    );

                    const location = clean(
                        document.querySelector("[data-testid='job-location']")?.innerText
                    );

                    const posted = clean(
                        document.querySelector("[data-testid='job-posted']")?.innerText
                    );

                    const description = clean(
                        document.querySelector("[data-testid='job-description']")?.innerText
                    );

                    return {
                        title,
                        company,
                        location,
                        posted,
                        description
                    };
                });

                const item = {
                    ...data,
                    url: request.url,
                    _source: "detail-page"
                };

                await Dataset.pushData(item);
            }
        });

        await crawler.run(
            detailUrls.map((u) => ({
                url: u,
                userData: { label: "DETAIL" }
            }))
        );
    }

    log.info("SCRAPING COMPLETE 🎉");
});
