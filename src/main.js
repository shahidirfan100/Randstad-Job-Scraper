// Randstad Job Scraper - CheerioCrawler implementation with stealth best practices
import { Actor, log } from 'apify';
import { CheerioCrawler, Dataset, RequestQueue, sleep } from 'crawlee';
import { load as cheerioLoad } from 'cheerio';
import { HeaderGenerator } from 'header-generator';
import { JSDOM } from 'jsdom';

const BASE_URL = 'https://www.randstad.com';
const JOBS_PATH = '/jobs/';

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
        micro: [60, 180],
        short: [320, 1100],
        medium: [900, 2000],
        long: [1800, 3500],
    };
    const [min, max] = ranges[type] || ranges.short;
    await sleep(randomBetween(min, max));
};

const buildSearchUrl = ({ keyword, location, postedDate, page = 1 }) => {
    const url = new URL(`${BASE_URL}${JOBS_PATH}`);
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

const composeJobUrl = (job) => {
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
    return `${BASE_URL}${JOBS_PATH}${slug}/`;
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
        text: jobData.CompensationText || null,
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

const parseDomFallbackDetail = (html) => {
    const dom = new JSDOM(html);
    const { document } = dom.window;
    const descriptionEl = document.querySelector('.meta-content__description, .block__description, .content-block__description, [data-testid="job-description"]');
    return {
        title: document.querySelector('h1')?.textContent || null,
        descriptionHtml: descriptionEl?.innerHTML || null,
        postedAt: document.querySelector('[data-testid="job-posted-date"], time[datetime]')?.getAttribute('datetime') || null,
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
        maxConcurrency = 3,
    } = input;

    const resultsWanted = Number.isFinite(+resultsWantedRaw) ? Math.max(1, +resultsWantedRaw) : Number.MAX_SAFE_INTEGER;
    const maxPages = Number.isFinite(+maxPagesRaw) ? Math.max(1, +maxPagesRaw) : 999;

    const initialUrls = [];
    if (Array.isArray(startUrls)) initialUrls.push(...startUrls.map((u) => u.url || u));
    if (startUrl) initialUrls.push(startUrl);
    if (url) initialUrls.push(url);
    if (!initialUrls.length) initialUrls.push(buildSearchUrl({ keyword, location, postedDate }));

    const proxy = proxyConfiguration
        ? await Actor.createProxyConfiguration({ ...proxyConfiguration })
        : undefined;

    const requestQueue = await RequestQueue.open();
    for (const initialUrl of initialUrls) {
        await requestQueue.addRequest({
            url: initialUrl,
            uniqueKey: initialUrl,
            userData: { label: 'LIST', pageNo: 1 },
        });
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
        minConcurrency: 1,
        maxRequestRetries: 2,
        requestTimeoutSecs: 45,
        maxRequestsPerMinute: 60,
        additionalMimeTypes: ['application/json'],
        useSessionPool: true,
        sessionPoolOptions: {
            maxPoolSize: 40,
            sessionOptions: {
                maxUsageCount: 4,
                maxSessionAgeSecs: 300,
            },
        },
        preNavigationHooks: [
            async ({ request, session }, gotoOptions) => {
                if (!session.userData.headers) {
                    session.userData.headers = headerGenerator.getHeaders({
                        httpVersion: '2',
                    });
                    if (inputCookies.length) {
                        session.setCookies(inputCookies, BASE_URL);
                    }
                }
                const headers = { ...session.userData.headers };
                headers.Referer = request.userData.referer || `${BASE_URL}/`;
                headers['sec-ch-ua-platform'] = headers['sec-ch-ua-platform'] || '"Windows"';
                headers['accept-language'] = headers['accept-language'] || 'en-US,en;q=0.9';
                gotoOptions.headers = {
                    ...headers,
                    ...gotoOptions.headers,
                };
                gotoOptions.timeout = randomBetween(20000, 32000);
                gotoOptions.retry = { limit: 1 };
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
                const previews = [];

                for (const hit of hits) {
                    const source = hit?._source;
                    if (!source) continue;
                    const jobUrl = composeJobUrl({ ...source, _id: hit._id }) || source.BlueXJobData?.JobUrl;
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
                        postedAt: source.JobDates?.DateCreated || source.JobDates?.DateCreatedTime || null,
                        jobCategory: normalizeWhitespace(source.BlueXSanitized?.Specialism || source.BlueXJobData?.Specialism),
                        salary,
                        snippet: htmlToText(source.JobInformation?.Description || source.BlueXJobData?.Description),
                        sourceRaw: hit,
                    });
                    if (dedupe) {
                        if (jobId) state.seenJobs.add(jobId);
                        state.seenJobs.add(jobUrl);
                    }
                }

                if (!previews.length) {
                    const fallback = parseDomFallbackList($, request.url);
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
                        const result = await requestQueue.addRequest({
                            url: preview.jobUrl,
                            uniqueKey: preview.jobUrl,
                            userData: {
                                label: 'DETAIL',
                                preview,
                                jobId: preview.jobId,
                                referer: request.url,
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
                const nextUrl = buildSearchUrl({ keyword, location, postedDate, page: nextPage });
                if (state.seenPages.has(nextUrl)) return;
                state.seenPages.add(nextUrl);
                await requestQueue.addRequest({
                    url: nextUrl,
                    uniqueKey: nextUrl,
                    userData: { label: 'LIST', pageNo: nextPage, referer: request.url },
                });
                await waitHumanLike('long');
                return;
            }

            if (label === 'DETAIL') {
                const { preview, jobId, backoffAttempt = 0 } = request.userData;
                const jobUrl = request.url;

                const routeData = extractRouteData($, body?.toString?.());
                const jobData = routeData?.jobData?.hits?.hits?.[0]?._source || null;

                if (!jobData && backoffAttempt < 3) {
                    const delay = (2 ** backoffAttempt) * 1000 + randomBetween(250, 900);
                    crawlerLog.warning(`Missing jobData for ${jobUrl}. Retrying after ${delay}ms.`);
                    await sleep(delay);
                    state.pendingDetail.delete(jobUrl);
                    await requestQueue.addRequest({
                        url: jobUrl,
                        uniqueKey: `${jobUrl}#${Date.now()}`,
                        userData: {
                            ...request.userData,
                            backoffAttempt: backoffAttempt + 1,
                        },
                    });
                    session?.markBad?.();
                    return;
                }
                if (!jobData && backoffAttempt >= 3) {
                    state.pendingDetail.delete(jobUrl);
                    throw new Error('Unable to load jobData after retries.');
                }

                const jsonLd = extractJsonLd($);
                const domFallback = parseDomFallbackDetail($.html());

                const locationInfo = deriveLocation(jobData) || {};
                const salary = extractSalary(jobData) || {};

                const jobPostingLocation = (() => {
                    const raw = jsonLd.jobPosting?.jobLocation;
                    if (Array.isArray(raw)) return raw[0];
                    return raw || null;
                })();
                const jobPostingAddress = jobPostingLocation?.address || jobPostingLocation;
                const employmentType = Array.isArray(jsonLd.jobPosting?.employmentType)
                    ? jsonLd.jobPosting.employmentType.join(', ')
                    : jsonLd.jobPosting?.employmentType || null;

                const descriptionHtml = jobData?.JobInformation?.Description
                    || domFallback.descriptionHtml
                    || null;

                const record = {
                    title: softNormalize(jobData?.JobInformation?.Title, domFallback.title || preview?.title),
                    company: softNormalize(jobData?.JobIdentity?.CompanyName, preview?.company || jsonLd.jobPosting?.hiringOrganization?.name || 'Randstad'),
                    job_url: jobUrl,
                    job_id: softNormalize(jobData?.JobId, preview?.jobId || jsonLd.jobPosting?.identifier || jobPostingLocation?.jobId || jobId),
                    reference_number: softNormalize(jobData?.BlueXJobData?.ReferenceNumber, jsonLd.jobPosting?.identifier),
                    location: softNormalize(locationInfo.location, preview?.location || jobPostingAddress?.addressLocality || jobPostingAddress?.address?.addressLocality),
                    city: softNormalize(locationInfo.city, jobPostingAddress?.addressLocality || jobPostingAddress?.address?.addressLocality),
                    region: softNormalize(locationInfo.region, jobPostingAddress?.addressRegion || jobPostingAddress?.address?.addressRegion),
                    country: softNormalize(locationInfo.country, jobPostingAddress?.addressCountry || jobPostingAddress?.address?.addressCountry),
                    postal_code: softNormalize(locationInfo.postalCode, jobPostingAddress?.postalCode || jobPostingAddress?.address?.postalCode),
                    job_type: softNormalize(jobData?.JobInformation?.JobType, preview?.jobType || employmentType),
                    employment_type: softNormalize(employmentType, jobData?.JobInformation?.JobType),
                    job_category: softNormalize(jobData?.BlueXSanitized?.Specialism, preview?.jobCategory || jsonLd.jobPosting?.industry),
                    date_posted: softNormalize(jobData?.JobDates?.DateCreatedTime, domFallback.postedAt || jsonLd.jobPosting?.datePosted || preview?.postedAt),
                    valid_through: softNormalize(jsonLd.jobPosting?.validThrough, jobData?.JobDates?.ExpirationDate),
                    salary_min: salary.minimum || jsonLd.jobPosting?.baseSalary?.value?.minValue || jsonLd.jobPosting?.baseSalary?.value?.value || null,
                    salary_max: salary.maximum || jsonLd.jobPosting?.baseSalary?.value?.maxValue || null,
                    salary_currency: salary.currency || jsonLd.jobPosting?.baseSalary?.value?.currency || null,
                    salary_interval: salary.interval || jsonLd.jobPosting?.baseSalary?.value?.unitText || null,
                    salary: formatSalaryString(salary, preview?.salary?.text),
                    salary_text: preview?.salary?.text || salary.text || jsonLd.jobPosting?.baseSalary?.value?.text || null,
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
                    data_source: 'detail',
                    extraction_notes: 'Combined __ROUTE_DATA__.jobData payload, JSON-LD, and DOM fallbacks.',
                };

                await dataset.pushData(sanitizeForDataset(record));
                state.saved += 1;
                state.pendingDetail.delete(jobUrl);
                crawlerLog.info(`Stored detail: ${record.title || record.job_url}. Total ${state.saved}`);
                await waitHumanLike('medium');
                return;
            }
        },
        failedRequestHandler: async ({ request, error }) => {
            log.error(`Request ${request.url} failed: ${error?.message}`);
            state.pendingDetail.delete(request.url);
        },
    });

    log.info('Randstad Job Scraper (Cheerio) started.');
    await crawler.run();
    log.info(`Run finished. Stored ${state.saved} records.`);
});
