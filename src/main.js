// Randstad Job Scraper - CheerioCrawler implementation with stealth best practices
import { Actor, log } from 'apify';
import {
    CheerioCrawler,
    Dataset,
    RequestQueue,
    sleep,
} from 'crawlee';
import { load as cheerioLoad } from 'cheerio';
import { HeaderGenerator } from 'header-generator';
import { JSDOM } from 'jsdom';

const headerGenerator = new HeaderGenerator({
    browsers: [
        { name: 'chrome', minVersion: 121, maxVersion: 126 },
        { name: 'edge', minVersion: 120, maxVersion: 125 },
    ],
    devices: ['desktop'],
    operatingSystems: ['windows', 'macos'],
    locales: ['en-US', 'en-GB', 'nl-NL', 'fr-FR', 'de-DE'],
});

const randomBetween = (min, max) => Math.floor(Math.random() * (max - min + 1)) + min;

const waitHumanLike = async (type = 'short') => {
    const ranges = {
        micro: [40, 120],
        short: [250, 850],
        medium: [650, 1600],
        long: [1400, 2800],
    };
    const [min, max] = ranges[type] || ranges.short;
    await sleep(randomBetween(min, max));
};

const getBaseFromUrl = (rawUrl) => {
    try {
        const { origin } = new URL(rawUrl);
        return origin;
    } catch {
        return 'https://www.randstad.com';
    }
};

const getJobsPath = (rawUrl) => {
    try {
        const url = new URL(rawUrl);
        return url.pathname.startsWith('/jobs') ? url.pathname : '/jobs/';
    } catch {
        return '/jobs/';
    }
};

const buildSearchUrl = ({ keyword, location, postedDate, page = 1, baseOrigin, jobsPath }) => {
    const path = jobsPath || '/jobs/';
    const url = new URL(path, baseOrigin || 'https://www.randstad.com');
    if (keyword) url.searchParams.set('q', keyword.trim());
    if (location) url.searchParams.set('location', location.trim());
    if (postedDate && postedDate !== 'any') url.searchParams.set('date', postedDate);
    if (page > 1) url.searchParams.set('page', String(page));
    return url.href;
};

const normalizeWhitespace = (value) => {
    if (!value) return null;
    return String(value).replace(/\s+/g, ' ').trim() || null;
};

const htmlToText = (html) => {
    if (!html) return null;
    const $ = cheerioLoad(html);
    $('script, style, noscript').remove();
    return normalizeWhitespace($.root().text());
};

const composeJobUrl = (job, baseOrigin) => {
    const slugParts = [];
    const sanitized = job?.BlueXSanitized || {};
    const jobId = job?.BlueXJobData?.JobId || job?.JobId || job?._id;
    if (sanitized?.Title) slugParts.push(sanitized.Title.toLowerCase());
    if (sanitized?.City) slugParts.push(sanitized.City.toLowerCase());
    if (jobId) slugParts.push(String(jobId));
    if (!slugParts.length) return null;
    const slug = slugParts
        .map((part) => part
            .toString()
            .normalize('NFKD')
            .replace(/[\u0300-\u036f]/g, '')
            .replace(/[^a-z0-9]+/g, '_')
            .replace(/^_+|_+$/g, '')
        )
        .filter(Boolean)
        .join('_');
    if (!slug) return null;
    const base = baseOrigin || 'https://www.randstad.com';
    const url = new URL(base);
    url.pathname = `${getJobsPath(url.href).replace(/\/$/, '')}/${slug}/`;
    url.search = '';
    url.hash = '';
    return url.href;
};

const countableJson = (text, marker) => {
    const startIndex = text.indexOf(marker);
    if (startIndex === -1) return null;
    const firstBrace = text.indexOf('{', startIndex);
    if (firstBrace === -1) return null;
    let depth = 0;
    for (let i = firstBrace; i < text.length; i++) {
        const char = text[i];
        if (char === '{') depth += 1;
        else if (char === '}') {
            depth -= 1;
            if (depth === 0) {
                return text.slice(firstBrace, i + 1);
            }
        }
    }
    return null;
};

const extractRouteData = ($, rawHtml) => {
    const scripts = [];
    $('script').each((_, el) => {
        const text = $(el).html();
        if (text && text.includes('__ROUTE_DATA__')) scripts.push(text);
    });
    if (!scripts.length && rawHtml) {
        scripts.push(rawHtml);
    }
    for (const code of scripts) {
        const jsonString = countableJson(code, 'window.__ROUTE_DATA__');
        if (!jsonString) continue;
        try {
            return JSON.parse(jsonString);
        } catch (error) {
            log.debug(`Failed to parse __ROUTE_DATA__: ${error.message}`);
        }
    }
    return null;
};

const extractJsonLd = ($) => {
    const payloads = [];
    $('script[type="application/ld+json"], script[type="application/json+ld"]').each((_, el) => {
        const text = $(el).html();
        if (!text) return;
        try {
            const parsed = JSON.parse(text.trim());
            payloads.push(parsed);
        } catch (error) {
            // ignore invalid snippets
        }
    });
    const flat = [];
    const flatten = (value) => {
        if (!value) return;
        if (Array.isArray(value)) {
            value.forEach(flatten);
            return;
        }
        if (typeof value === 'object') {
            flat.push(value);
            Object.values(value).forEach(flatten);
        }
    };
    flatten(payloads);
    const jobPosting = flat.find((item) => {
        if (!item?.['@type']) return false;
        const types = Array.isArray(item['@type']) ? item['@type'] : [item['@type']];
        return types.includes('JobPosting');
    });
    return { jobPosting: jobPosting || null, all: payloads };
};

const deriveLocationFromJobPosting = (jobPostingInput) => {
    const jobPosting = jobPostingInput || {};
    if (!jobPosting) return {};
    const locationNode = Array.isArray(jobPosting.jobLocation)
        ? jobPosting.jobLocation[0]
        : jobPosting.jobLocation || {};
    const address = locationNode?.address || locationNode || {};
    const city = address.addressLocality || address.city || null;
    const region = address.addressRegion || address.region || null;
    const country = address.addressCountry || address.country || null;
    const postalCode = address.postalCode || null;
    const pieces = [city, region, country].filter(Boolean);
    return {
        city: city || null,
        region: region || null,
        country: country || null,
        postalCode: postalCode || null,
        location: pieces.length ? pieces.join(', ') : null,
    };
};

const extractSalaryFromJobPosting = (jobPostingInput) => {
    const jobPosting = jobPostingInput || {};
    const baseSalary = jobPosting?.baseSalary || {};
    const value = baseSalary?.value || {};
    const toNumber = (num) => {
        if (num == null) return null;
        const parsed = Number(num);
        return Number.isFinite(parsed) ? parsed : String(num);
    };
    return {
        minimum: toNumber(value.minValue ?? value.value ?? value.minimum),
        maximum: toNumber(value.maxValue ?? value.maximum),
        currency: baseSalary.currency || value.currency || null,
        interval: value.unitText || baseSalary.unitText || null,
        text: value.text || null,
    };
};

const deriveLocation = (source = {}) => {
    const location = source?.JobLocation || {};
    const sanitized = source?.BlueXSanitized || {};
    const city = location.City || sanitized.City;
    const region = location.Region || sanitized.Region;
    const country = location.Country || sanitized.Country;
    const postcode = location.Postcode || sanitized.Postcode;
    const pieces = [city, region, country].filter(Boolean);
    return {
        city: city || null,
        region: region || null,
        country: country || null,
        postalCode: postcode || null,
        location: pieces.length ? pieces.join(', ') : null,
    };
};

const extractSalary = (source = {}) => {
    const salary = source.Salary || {};
    const jobData = source.BlueXJobData || {};
    return {
        minimum: salary.SalaryMin || jobData.MinimumSalary || null,
        maximum: salary.SalaryMax || jobData.MaximumSalary || null,
        currency: salary.CurrencyId || jobData.Currency || null,
        interval: salary.CompensationType || jobData.CompensationType || null,
        text: jobData.CompensationText || salary.CompensationText || null,
    };
};

const parseDomFallbackList = ($, baseUrl) => {
    const jobs = [];
    $('[data-testid="job-card"], .cards__item').each((_, el) => {
        const element = $(el);
        const anchor = element.find('a[href*="/jobs/"]').first();
        const href = anchor.attr('href');
        if (!href) return;
        const jobUrl = href.startsWith('http') ? href : new URL(href, baseUrl).href;
        const title = normalizeWhitespace(anchor.text()) || normalizeWhitespace(element.find('h1, h2, h3').first().text());
        const company = normalizeWhitespace(element.find('[data-testid="job-company"], .cards__logo-title-container').first().text()) || 'Randstad';
        const location = normalizeWhitespace(element.find('[data-testid="job-location"], .cards__meta-item, [data-testid="job-card-location"]').first().text());
        const postedAt = normalizeWhitespace(element.find('time').attr('datetime') || element.find('.cards__time-info').text());
        const salaryText = normalizeWhitespace(element.find('[data-testid="renumeration indication of role"], [data-testid="job-salary"]').text());
        if (!title || !jobUrl) return;
        jobs.push({
            jobUrl,
            title,
            company,
            location,
            postedAt,
            salaryText,
            snippetHtml: element.find('.cards__description, .cards__backside-description, [data-testid="job-card-description"]').first().html() || null,
        });
    });
    return jobs;
};

const parseDomFallbackDetail = ($) => {
    if (!$) return { 
        title: null, 
        company: null,
        location: null,
        descriptionHtml: null, 
        postedAt: null,
        jobType: null,
        salary: null,
    };
    
    // Multiple selectors for title
    const title = $('h1').first().text().trim() 
        || $('.content-block__title, .block__title, .meta-content__title').first().text().trim()
        || $('[data-testid="job-title"]').first().text().trim()
        || null;
    
    // Company name
    const company = $('.cards__logo-title-container').first().text().trim()
        || $('[data-testid="company-name"]').first().text().trim()
        || 'Randstad';
    
    // Location
    const location = $('.behat-location').first().text().trim()
        || $('[data-testid="job-location"]').first().text().trim()
        || $('[class*="location"]').first().text().trim()
        || null;
    
    // Parse location into components
    let city = null, region = null, country = null, postalCode = null;
    if (location) {
        const parts = location.split(',').map(p => p.trim());
        if (parts.length >= 3) {
            city = parts[0];
            region = parts[1];
            country = parts[2];
        } else if (parts.length === 2) {
            city = parts[0];
            country = parts[1];
        } else {
            city = location;
        }
        const zipMatch = location.match(/\b\d{5}(?:-\d{4})?\b/);
        if (zipMatch) postalCode = zipMatch[0];
    }
    
    // Description - try multiple selectors
    const descriptionEl = $('.meta-content__description, .block__description, .content-block__description').first()
        || $('.job-description, .job-details, .description').first()
        || $('[data-testid="job-description"]').first()
        || $('.cards__backside-description').first();
    
    const descriptionHtml = descriptionEl.html() || null;
    
    // Posted date
    const postedAt = $('[data-testid="job-posted-date"]').attr('datetime') 
        || $('time[datetime]').attr('datetime')
        || $('.behat-expirationDate').first().text().trim()
        || $('[class*="date"]').first().text().trim()
        || null;
    
    // Job type
    const jobType = $('.behat-jobType').first().text().trim()
        || $('[data-testid="job-type"]').first().text().trim()
        || $('[class*="job-type"]').first().text().trim()
        || null;
    
    // Salary
    const salary = $('.behat-salary').first().text().trim()
        || $('[data-testid="job-salary"]').first().text().trim()
        || $('[class*="salary"]').first().text().trim()
        || null;
    
    return {
        title,
        company,
        location,
        city,
        region,
        country,
        postalCode,
        descriptionHtml,
        postedAt,
        jobType,
        salary: salary ? { text: salary } : null,
    };
};

const softNormalize = (value, fallback) => value ?? fallback ?? null;

const formatSalaryString = (salary = {}, fallback) => {
    if (!salary) return normalizeWhitespace(fallback);
    const currency = salary.currency || salary.Currency || null;
    const min = salary.minimum || salary.min || null;
    const max = salary.maximum || salary.max || null;
    const interval = salary.interval || salary.intervalText || null;
    const pieces = [];
    if (currency && (min || max)) pieces.push(currency);
    if (min && max && Number(min) !== Number(max)) {
        pieces.push(`${min} - ${max}`);
    } else if (min || max) {
        pieces.push(min || max);
    }
    if (interval) pieces.push(interval);
    const textual = salary.text || fallback;
    const formatted = normalizeWhitespace(pieces.join(' '));
    return formatted || normalizeWhitespace(textual);
};

const STRING_FIELD_SANITIZERS = {
    title: normalizeWhitespace,
    company: normalizeWhitespace,
    location: normalizeWhitespace,
    date_posted: normalizeWhitespace,
    job_type: normalizeWhitespace,
    job_category: normalizeWhitespace,
    description_html: (value) => (typeof value === 'string' ? value : value == null ? null : String(value)),
    description_text: normalizeWhitespace,
    job_url: (value) => (value == null ? null : String(value)),
    salary: normalizeWhitespace,
};

const sanitizeForDataset = (record) => {
    const output = { ...record };
    for (const [field, sanitizer] of Object.entries(STRING_FIELD_SANITIZERS)) {
        if (!(field in output)) continue;
        const value = output[field];
        if (value === undefined || value === null) {
            delete output[field];
            continue;
        }
        const sanitized = sanitizer(value);
        if (sanitized === undefined || sanitized === null || sanitized === '') {
            delete output[field];
        } else {
            output[field] = sanitized;
        }
    }
    return output;
};

await Actor.main(async () => {
    const input = (await Actor.getInput()) || {};
    const {
        keyword = '',
        location = '',
        posted_date: postedDate = 'any',
        results_wanted: resultsWantedRaw = 100,
        max_pages: maxPagesRaw = 999,
        collectDetails = true,
        startUrl,
        startUrls,
        url,
        cookies,
        cookiesJson,
        proxyConfiguration,
        dedupe = true,
        maxConcurrency = 5,
    } = input;

    const resultsWanted = Number.isFinite(+resultsWantedRaw) ? Math.max(1, +resultsWantedRaw) : Number.MAX_SAFE_INTEGER;
    const maxPages = Number.isFinite(+maxPagesRaw) ? Math.max(1, +maxPagesRaw) : 999;

    const initialUrls = [];
    if (Array.isArray(startUrls)) initialUrls.push(...startUrls.map((u) => u.url || u));
    if (startUrl) initialUrls.push(startUrl);
    if (url) initialUrls.push(url);
    if (!initialUrls.length) initialUrls.push(buildSearchUrl({
        keyword,
        location,
        postedDate,
        baseOrigin: 'https://www.randstad.com',
        jobsPath: '/jobs/',
    }));

    const proxy = proxyConfiguration
        ? await Actor.createProxyConfiguration({ ...proxyConfiguration })
        : undefined;

    const requestQueue = await RequestQueue.open();
    // Track the first effective URL we add to the queue so we can log it later
    let firstUrlToUse = null;
    for (const initialUrl of initialUrls) {
        const baseOrigin = getBaseFromUrl(initialUrl);
        const jobsPath = getJobsPath(initialUrl);
        const urlToUse = initialUrl.includes('/jobs/')
            ? initialUrl
            : buildSearchUrl({
                keyword,
                location,
                postedDate,
                baseOrigin,
                jobsPath,
            });
        await requestQueue.addRequest({
            url: urlToUse,
            uniqueKey: urlToUse,
            userData: {
                label: 'LIST',
                pageNo: 1,
                baseOrigin,
                jobsPath,
            },
        });
        if (!firstUrlToUse) firstUrlToUse = urlToUse;
    }

    const state = {
        saved: 0,
        seenJobs: new Set(),
        seenPages: new Set(),
        pendingDetail: new Set(),
    };

    const dataset = await Dataset.open();

    const getCookiesFromInput = () => {
        const cookieObjects = [];
        if (Array.isArray(cookies)) cookieObjects.push(...cookies);
        if (typeof cookiesJson === 'string') {
            try {
                const parsed = JSON.parse(cookiesJson);
                if (Array.isArray(parsed)) cookieObjects.push(...parsed);
            } catch (error) {
                log.warning(`Failed to parse cookiesJson: ${error.message}`);
            }
        }
        return cookieObjects
            .map((cookie) => {
                if (!cookie) return null;
                if (typeof cookie === 'string') {
                    const parts = cookie.split(';').map((part) => part.trim());
                    const [nameValue, ...rest] = parts;
                    if (!nameValue.includes('=')) return null;
                    const [name, value] = nameValue.split('=');
                    const cookieObj = { name, value, domain: '.randstad.com', path: '/' };
                    rest.forEach((segment) => {
                        const [key, val] = segment.split('=');
                        if (!key) return;
                        const lower = key.toLowerCase();
                        if (lower === 'domain') cookieObj.domain = val;
                        if (lower === 'path') cookieObj.path = val || '/';
                    });
                    return cookieObj;
                }
                if (typeof cookie === 'object') return cookie;
                return null;
            })
            .filter(Boolean);
    };

    const inputCookies = getCookiesFromInput();

    const crawler = new CheerioCrawler({
        requestQueue,
        proxyConfiguration: proxy,
        maxConcurrency,
        minConcurrency: 2,
        maxRequestRetries: 3,
        maxRequestsPerMinute: 80,
        additionalMimeTypes: ['application/json'],
        useSessionPool: true,
        sessionPoolOptions: {
            maxPoolSize: 40,
            sessionOptions: {
                maxUsageCount: 5,
            },
        },
        preNavigationHooks: [
            async ({ request, session }, requestOptions) => {
                const baseOrigin = request.userData.baseOrigin || getBaseFromUrl(request.url);
                if (session && !session.userData.headers) {
                    session.userData.headers = headerGenerator.getHeaders({ httpVersion: '2' });
                }
                const baseHeaders = session?.userData.headers || headerGenerator.getHeaders({ httpVersion: '2' });
                const headers = {
                    ...baseHeaders,
                    Referer: request.userData.referer || `${baseOrigin}/`,
                    'sec-ch-ua-platform': '"Windows"',
                    'accept-language': 'en-US,en;q=0.9',
                };
                requestOptions.headers = {
                    ...headers,
                    ...(requestOptions.headers || {}),
                };
                requestOptions.timeout = {
                    request: randomBetween(20000, 32000),
                };
                if (session && inputCookies.length && !session.userData.cookiesApplied) {
                    session.setCookies(inputCookies, baseOrigin);
                    session.userData.cookiesApplied = true;
                }
                await waitHumanLike('micro');
            },
        ],
        postNavigationHooks: [
            async () => {
                await waitHumanLike('short');
            },
        ],
        requestHandler: async (context) => {
            const { $, request, response, body, log: crawlerLog, session } = context;
            const label = request.userData.label || 'LIST';

            if (label === 'LIST') {
                const pageNo = request.userData.pageNo || 1;
                crawlerLog.info(`Processing list page ${pageNo}: ${request.url}`);

                const routeData = extractRouteData($, body?.toString?.());
                const hits = routeData?.searchResults?.hits?.hits || [];
                
                if (!hits.length) {
                    crawlerLog.warning(`No hits found in __ROUTE_DATA__ for page ${pageNo}, trying DOM fallback`);
                }
                
                const previews = [];

                for (const hit of hits) {
                    const source = hit?._source;
                    if (!source) continue;
                    let jobUrl = source.BlueXJobData?.JobUrl || source.JobInformation?.JobUrl || null;
                    if (jobUrl && !jobUrl.startsWith('http')) {
                        const baseOrigin = request.userData.baseOrigin || getBaseFromUrl(request.url);
                        jobUrl = new URL(jobUrl, baseOrigin).href;
                    }
                    if (!jobUrl) {
                        jobUrl = composeJobUrl({ ...source, _id: hit._id }, request.userData.baseOrigin);
                    }
                    if (!jobUrl) continue;
                    const jobId = source.BlueXJobData?.JobId || source.JobId || hit._id;
                    if (dedupe && jobId && state.seenJobs.has(jobId)) continue;
                    if (dedupe && state.seenJobs.has(jobUrl)) continue;
                    const locationInfo = deriveLocation(source);
                    const salary = extractSalary(source);
                    previews.push({
                        jobId,
                        jobUrl,
                        title: normalizeWhitespace(source.JobInformation?.Title) || normalizeWhitespace(source.BlueXJobData?.Title),
                        company: normalizeWhitespace(source.JobIdentity?.CompanyName || source.BlueXJobData?.CompanyName || 'Randstad'),
                        location: locationInfo.location,
                        city: locationInfo.city,
                        region: locationInfo.region,
                        country: locationInfo.country,
                        postalCode: locationInfo.postalCode,
                        jobType: normalizeWhitespace(source.JobInformation?.JobType || source.BlueXJobData?.JobType),
                        jobCategory: normalizeWhitespace(source.BlueXSanitized?.Specialism || source.BlueXJobData?.Specialism),
                        postedAt: source.JobDates?.DateCreated || source.JobDates?.DateCreatedTime || null,
                        salary,
                        snippet: htmlToText(source.JobInformation?.Description || source.BlueXJobData?.Description),
                        snippetHtml: source.JobInformation?.Description || source.BlueXJobData?.Description || null,
                        sourceRaw: hit,
                    });
                    if (dedupe) {
                        if (jobId) state.seenJobs.add(jobId);
                        state.seenJobs.add(jobUrl);
                    }
                }
                
                crawlerLog.info(`Extracted ${previews.length} jobs from page ${pageNo} (via ${hits.length ? '__ROUTE_DATA__' : 'DOM fallback'})`);

                if (!previews.length) {
                    crawlerLog.warning(`No jobs extracted from __ROUTE_DATA__, attempting DOM fallback for page ${pageNo}`);
                    const fallback = parseDomFallbackList($, request.url);
                    crawlerLog.info(`DOM fallback found ${fallback.length} jobs on page ${pageNo}`);
                    for (const job of fallback) {
                        if (dedupe && state.seenJobs.has(job.jobUrl)) continue;
                        const idMatch = job.jobUrl.match(/_(\d+)(?:\/|$)/);
                        const jobId = idMatch ? idMatch[1] : null;
                        if (dedupe && jobId && state.seenJobs.has(jobId)) continue;
                        previews.push({
                            jobId,
                            jobUrl: job.jobUrl,
                            title: job.title,
                            company: job.company,
                            location: job.location,
                            city: null,
                            region: null,
                            country: null,
                            postalCode: null,
                            jobType: null,
                            jobCategory: null,
                            postedAt: job.postedAt,
                            salary: {
                                minimum: null,
                                maximum: null,
                                currency: null,
                                interval: null,
                                text: job.salaryText,
                            },
                            snippet: htmlToText(job.snippetHtml),
                            sourceRaw: { dom: job },
                        });
                        if (dedupe) {
                            if (jobId) state.seenJobs.add(jobId);
                            state.seenJobs.add(job.jobUrl);
                        }
                    }
                }

                if (!previews.length) {
                    crawlerLog.warning(`No jobs detected on ${request.url}`);
                }

                if (!collectDetails) {
                    const remaining = resultsWanted - state.saved;
                    if (remaining > 0) {
                        const toStore = previews.slice(0, remaining).map((preview) => sanitizeForDataset({
                            title: preview.title,
                            company: preview.company,
                            job_url: preview.jobUrl,
                            job_id: preview.jobId,
                            location: preview.location,
                            city: preview.city,
                            region: preview.region,
                            country: preview.country,
                            postal_code: preview.postalCode,
                            job_type: preview.jobType,
                            job_category: preview.jobCategory,
                            date_posted: preview.postedAt,
                            salary: formatSalaryString(preview.salary),
                            salary_min: preview.salary?.minimum || null,
                            salary_max: preview.salary?.maximum || null,
                            salary_currency: preview.salary?.currency || null,
                            salary_interval: preview.salary?.interval || null,
                            salary_text: preview.salary?.text || null,
                            snippet: preview.snippet,
                            data_source: 'list',
                            extraction_notes: 'Parsed from __ROUTE_DATA__ searchResults payload with DOM fallback.',
                            scraped_at: new Date().toISOString(),
                        }));
                        if (toStore.length) {
                            await dataset.pushData(toStore);
                            state.saved += toStore.length;
                            crawlerLog.info(`Stored ${toStore.length} list records. Total: ${state.saved}`);
                        }
                    }
                } else {
                    for (const preview of previews) {
                        if (state.saved + state.pendingDetail.size >= resultsWanted) break;
                        if (state.pendingDetail.has(preview.jobUrl)) continue;
                        state.pendingDetail.add(preview.jobUrl);
                        const detailBaseOrigin = getBaseFromUrl(preview.jobUrl);
                        const detailJobsPath = getJobsPath(preview.jobUrl);
                        const result = await requestQueue.addRequest({
                            url: preview.jobUrl,
                            uniqueKey: preview.jobUrl,
                            userData: {
                                label: 'DETAIL',
                                preview,
                                jobId: preview.jobId,
                                referer: request.url,
                                baseOrigin: detailBaseOrigin,
                                jobsPath: detailJobsPath,
                                backoffAttempt: 0,
                            },
                        });
                        if (result?.wasAlreadyPresent) {
                            state.pendingDetail.delete(preview.jobUrl);
                        }
                    }
                }

                if (state.saved >= resultsWanted || pageNo >= maxPages) {
                    crawlerLog.info(`Stopping after page ${pageNo}. Saved ${state.saved}.`);
                    return;
                }

                const pageSize = hits.length || 30;
                const total = routeData?.searchResults?.hits?.total || 0;
                const totalPages = pageSize ? Math.ceil(total / pageSize) : pageNo + 1;
                const nextPage = pageNo + 1;
                if (nextPage > maxPages || nextPage > totalPages) {
                    return;
                }
                const nextUrl = buildSearchUrl({
                    keyword,
                    location,
                    postedDate,
                    page: nextPage,
                    baseOrigin: request.userData.baseOrigin || getBaseFromUrl(request.url),
                    jobsPath: request.userData.jobsPath || getJobsPath(request.url),
                });
                if (state.seenPages.has(nextUrl)) return;
                state.seenPages.add(nextUrl);
                await requestQueue.addRequest({
                    url: nextUrl,
                    uniqueKey: nextUrl,
                    userData: {
                        label: 'LIST',
                        pageNo: nextPage,
                        referer: request.url,
                        baseOrigin: request.userData.baseOrigin,
                        jobsPath: request.userData.jobsPath,
                    },
                });
                await waitHumanLike('long');
                return;
            }

            if (label === 'DETAIL') {
                const { preview, jobId, backoffAttempt = 0 } = request.userData;
                const jobUrl = request.url;

                if (state.saved >= resultsWanted) {
                    state.pendingDetail.delete(jobUrl);
                    return;
                }

                const routeData = extractRouteData($, body?.toString?.());
                const jobData = routeData?.jobData?.hits?.hits?.[0]?._source || null;

                const jsonLd = extractJsonLd($);
                const jobPosting = jsonLd.jobPosting || null;
                const domFallback = parseDomFallbackDetail($);

                const hasStructuredData = Boolean(jobData || jobPosting);
                
                // If we have DOM data with at least title and description, we can proceed
                const hasDomData = Boolean(domFallback.title && domFallback.descriptionHtml);
                
                if (!hasStructuredData && !hasDomData && backoffAttempt < 2) {
                    const delay = (2 ** backoffAttempt) * 1000 + randomBetween(250, 900);
                    crawlerLog.warning(`Missing all data sources for ${jobUrl}. Retrying (${backoffAttempt + 1}/2) after ${delay}ms.`);
                    await sleep(delay);
                    state.pendingDetail.delete(jobUrl);
                    await requestQueue.addRequest({
                        url: jobUrl,
                        uniqueKey: `${jobUrl}#retry${backoffAttempt + 1}`,
                        userData: {
                            ...request.userData,
                            backoffAttempt: backoffAttempt + 1,
                        },
                    });
                    if (session?.markBad) session.markBad();
                    return;
                }
                
                // If we still have no data after retries, check if we have preview or DOM data
                if (!hasStructuredData && !hasDomData && !preview) {
                    crawlerLog.error(`Unable to extract job detail data for ${jobUrl} - no data sources available`);
                    state.pendingDetail.delete(jobUrl);
                    return;
                }
                
                // Log which data source we're using
                const dataSources = [];
                if (jobData) dataSources.push('__ROUTE_DATA__');
                if (jobPosting) dataSources.push('JSON-LD');
                if (hasDomData) dataSources.push('DOM');
                if (preview) dataSources.push('preview');
                crawlerLog.info(`Extracting from: ${dataSources.join(', ')} for ${jobUrl}`);

                const locationFromJobData = deriveLocation(jobData) || {};
                const locationFromJsonLd = deriveLocationFromJobPosting(jobPosting) || {};
                const locationInfo = {
                    city: domFallback.city || locationFromJobData.city || locationFromJsonLd.city || preview?.city || null,
                    region: domFallback.region || locationFromJobData.region || locationFromJsonLd.region || preview?.region || null,
                    country: domFallback.country || locationFromJobData.country || locationFromJsonLd.country || preview?.country || null,
                    postalCode: domFallback.postalCode || locationFromJobData.postalCode || locationFromJsonLd.postalCode || preview?.postalCode || null,
                    location: domFallback.location || locationFromJobData.location || locationFromJsonLd.location || preview?.location || null,
                };

                const salaryFromJobData = extractSalary(jobData) || {};
                const salaryFromJsonLd = extractSalaryFromJobPosting(jobPosting) || {};
                const salaryFromDom = domFallback.salary || {};
                const combinedSalary = {
                    minimum: salaryFromDom.minimum ?? salaryFromJobData.minimum ?? salaryFromJsonLd.minimum ?? preview?.salary?.minimum ?? null,
                    maximum: salaryFromDom.maximum ?? salaryFromJobData.maximum ?? salaryFromJsonLd.maximum ?? preview?.salary?.maximum ?? null,
                    currency: salaryFromDom.currency ?? salaryFromJobData.currency ?? salaryFromJsonLd.currency ?? preview?.salary?.currency ?? null,
                    interval: salaryFromDom.interval ?? salaryFromJobData.interval ?? salaryFromJsonLd.interval ?? preview?.salary?.interval ?? null,
                    text: salaryFromDom.text ?? salaryFromJobData.text ?? salaryFromJsonLd.text ?? preview?.salary?.text ?? null,
                };

                const employmentType = (() => {
                    if (domFallback.jobType) return domFallback.jobType;
                    if (jobData?.JobInformation?.JobType) return jobData.JobInformation.JobType;
                    if (Array.isArray(jobPosting?.employmentType)) return jobPosting.employmentType.join(', ');
                    if (typeof jobPosting?.employmentType === 'string') return jobPosting.employmentType;
                    return preview?.jobType || null;
                })();

                const descriptionHtml = domFallback.descriptionHtml
                    || jobData?.JobInformation?.Description
                    || jobPosting?.description
                    || preview?.snippetHtml
                    || null;

                const identifierFromJsonLd = (() => {
                    const identifier = jobPosting?.identifier;
                    if (!identifier) return null;
                    if (typeof identifier === 'string') return identifier;
                    if (typeof identifier === 'object') return identifier.value || identifier.name || identifier.propertyID || null;
                    return null;
                })();

                const postedDate = softNormalize(
                    jobData?.JobDates?.DateCreatedTime,
                    domFallback.postedAt
                        || jobPosting?.datePosted
                        || preview?.postedAt,
                );

                const record = {
                    title: softNormalize(domFallback.title, jobData?.JobInformation?.Title || preview?.title) || 'Untitled',
                    company: softNormalize(domFallback.company, jobData?.JobIdentity?.CompanyName || preview?.company || jsonLd.jobPosting?.hiringOrganization?.name || 'Randstad'),
                    job_url: jobUrl,
                    job_id: softNormalize(jobData?.JobId, preview?.jobId || identifierFromJsonLd || jobId),
                    reference_number: softNormalize(jobData?.BlueXJobData?.ReferenceNumber, identifierFromJsonLd),
                    location: softNormalize(locationInfo.location, preview?.location),
                    city: softNormalize(locationInfo.city, null),
                    region: softNormalize(locationInfo.region, null),
                    country: softNormalize(locationInfo.country, null),
                    postal_code: softNormalize(locationInfo.postalCode, null),
                    job_type: softNormalize(employmentType, jobData?.JobInformation?.JobType),
                    employment_type: softNormalize(employmentType, jobData?.JobInformation?.JobType),
                    job_category: softNormalize(jobData?.BlueXSanitized?.Specialism, preview?.jobCategory || jobPosting?.industry),
                    date_posted: postedDate,
                    valid_through: softNormalize(jobPosting?.validThrough, jobData?.JobDates?.ExpirationDate),
                    salary_min: combinedSalary.minimum,
                    salary_max: combinedSalary.maximum,
                    salary_currency: combinedSalary.currency,
                    salary_interval: combinedSalary.interval,
                    salary: formatSalaryString(combinedSalary, preview?.salary?.text),
                    salary_text: combinedSalary.text || preview?.salary?.text || null,
                    description_html: descriptionHtml,
                    description_text: htmlToText(descriptionHtml),
                    requirements: htmlToText(jobData?.JobInformation?.Requirements || jsonLd.jobPosting?.qualifications || jsonLd.jobPosting?.responsibilities),
                    benefits: htmlToText(jobData?.JobInformation?.Benefits || jsonLd.jobPosting?.jobBenefits),
                    tags: Array.from(new Set([
                        jobData?.BlueXSanitized?.Specialism,
                        jobData?.BlueXSanitized?.SubSpecialism,
                        jsonLd.jobPosting?.occupationalCategory,
                    ].filter(Boolean))),
                    seniority: jobData?.JobInformation?.Seniority || null,
                    work_hours: jobData?.JobInformation?.Hours || jsonLd.jobPosting?.workHours || null,
                    remote_type: jsonLd.jobPosting?.jobLocationType || null,
                    seo_title: softNormalize(jsonLd.jobPosting?.title, null),
                    breadcrumbs: jsonLd.all
                        .flatMap((item) => (item['@type'] === 'BreadcrumbList' ? item.itemListElement || [] : []))
                        .map((crumb) => ({
                            position: crumb.position,
                            name: crumb.name,
                            item: crumb.item,
                        })),
                    api_source: jobData,
                    json_ld: jsonLd.jobPosting || null,
                    scraped_at: new Date().toISOString(),
                    data_source: hasStructuredData ? 'detail' : 'preview_fallback',
                    extraction_notes: hasStructuredData 
                        ? 'Combined __ROUTE_DATA__.jobData payload, JSON-LD, and DOM fallbacks.' 
                        : 'Fallback to preview data due to missing structured data',
                };

                const sanitized = sanitizeForDataset(record);
                
                if (!sanitized.title || !sanitized.job_url) {
                    crawlerLog.error(`Skipping record - missing required fields (title: ${sanitized.title}, url: ${sanitized.job_url})`);
                    state.pendingDetail.delete(jobUrl);
                    return;
                }

                await dataset.pushData(sanitized);
                state.saved += 1;
                state.pendingDetail.delete(jobUrl);
                crawlerLog.info(`✓ Stored detail: ${sanitized.title} - Total ${state.saved}/${resultsWanted}`);
                await waitHumanLike('medium');
                return;
            }
        },
        failedRequestHandler: async ({ request, error }) => {
            log.error(`Request ${request.url} failed: ${error?.message}`);
            state.pendingDetail.delete(request.url);
        },
    });

    log.info('Randstad Job Scraper started.');
    log.info(`Configuration: keyword="${keyword}", location="${location}", posted_date="${postedDate}"`);
    log.info(`Limits: results_wanted=${resultsWanted}, max_pages=${maxPages}, collectDetails=${collectDetails}`);
    log.info(`Starting URL: ${firstUrlToUse || initialUrls[0]}`);
    
    await crawler.run();
    
    log.info(`✓ Run completed successfully. Stored ${state.saved} job records.`);
    
    if (state.saved < resultsWanted) {
        log.warning(`Note: Only collected ${state.saved} jobs out of ${resultsWanted} requested.`);
    }
});
