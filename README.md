# Randstad Jobs Scraper

> Scrape job listings from Randstad's global job board with ease. Extract detailed job information including titles, companies, locations, descriptions, and more.

This Apify actor automates the extraction of job postings from Randstad's website, allowing you to collect data on available positions, filter by keywords, locations, and posting dates, and retrieve comprehensive job details.

## 🚀 What This Actor Does

- **Comprehensive Job Extraction**: Collects job listings from Randstad's search results pages.
- **Flexible Filtering**: Supports keyword searches, location-based filtering, and date-based posting filters.
- **Detailed Information**: Retrieves full job descriptions, company details, salary information, and more.
- **Pagination Handling**: Automatically navigates through multiple pages of results.
- **Customizable Depth**: Option to scrape basic listing info or full job details.
- **Dataset Storage**: Saves all extracted data to an Apify dataset for easy access and export.

## 📥 Input Parameters

The actor accepts various input parameters to customize your scraping job. All parameters are optional unless specified.

### Basic Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `startUrl` | string | - | Start scraping from a specific Randstad URL. Overrides other filters if provided. |
| `startUrls` | array | - | Array of Randstad URLs to scrape from. |
| `url` | string | - | Single Randstad URL to start scraping. |
| `keyword` | string | - | Job search keywords (e.g., "Software Developer", "Marketing Manager"). |
| `location` | string | - | Location filter (e.g., "New York", "London"). |
| `posted_date` | string | `any` | Filter jobs by posting date. Options: `any`, `last_24_hours`, `last_7_days`, `last_30_days`. |

### Scraping Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `collectDetails` | boolean | `true` | Whether to visit individual job pages for full descriptions. Set to `false` for faster basic scraping. |
| `results_wanted` | integer | `100` | Maximum number of jobs to collect. |
| `max_pages` | integer | `20` | Maximum number of result pages to process. |

### Advanced Settings

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `proxyConfiguration` | object | Residential proxy | Proxy settings for requests. Use Apify Proxy for best results. |
| `cookies` | string | - | Raw Cookie header string for authentication or session handling. |
| `cookiesJson` | string | - | JSON-formatted cookies array or object. |
| `dedupe` | boolean | `true` | Remove duplicate job URLs from results. |

## 📤 Output

The actor stores results in an Apify dataset. Each item represents a single job posting with the following structure:

```json
{
  "title": "Software Engineer",
  "company": "Tech Corp",
  "location": "New York, NY",
  "date_posted": "2023-10-15",
  "job_type": "Full-time",
  "job_category": "Technology",
  "description_html": "<p>We are looking for a skilled software engineer...</p>",
  "description_text": "We are looking for a skilled software engineer...",
  "job_url": "https://www.randstad.com/job/software-engineer-12345",
  "salary": "$80,000 - $100,000 per year"
}
```

### Output Fields

- **`title`** (string): Job position title
- **`company`** (string): Hiring company name
- **`location`** (string): Job location
- **`date_posted`** (string): Date the job was posted
- **`job_type`** (string): Employment type (e.g., Full-time, Part-time)
- **`job_category`** (string): Job category or industry
- **`description_html`** (string): Full job description in HTML format
- **`description_text`** (string): Plain text version of the job description
- **`job_url`** (string): Direct link to the job posting
- **`salary`** (string): Salary information if available

## 🛠️ Usage Examples

### Basic Usage

Run the actor with default settings to scrape recent jobs from Randstad:

```json
{
  "results_wanted": 50,
  "max_pages": 5
}
```

### Keyword Search

Search for specific job types:

```json
{
  "keyword": "data analyst",
  "location": "San Francisco",
  "posted_date": "last_24_hours",
  "results_wanted": 25
}
```

### Custom URL Scraping

Scrape from a specific Randstad search URL:

```json
{
  "startUrl": "https://www.randstad.com/jobs/?q=marketing&location=chicago",
  "collectDetails": true
}
```

### Fast Overview Mode

Get basic job info quickly without full descriptions:

```json
{
  "keyword": "engineer",
  "collectDetails": false,
  "results_wanted": 100
}
```

## ⚙️ Configuration

### Proxy Settings

For optimal performance and to avoid IP blocking, configure proxy settings:

```json
{
  "proxyConfiguration": {
    "useApifyProxy": true,
    "apifyProxyGroups": ["RESIDENTIAL"]
  }
}
```

### Cookie Handling

If you need to handle authentication or consent banners:

```json
{
  "cookiesJson": [
    {"name": "consent", "value": "accepted", "domain": ".randstad.com"}
  ]
}
```

## 📊 Limits and Performance

- **Rate Limiting**: The actor includes built-in delays and respects website policies.
- **Concurrency**: Optimized for efficient scraping without overwhelming the target site.
- **Data Volume**: Can handle thousands of jobs per run, depending on your Apify plan.
- **Timeout**: Individual requests have a 60-second timeout to handle slow responses.

## 💰 Cost Estimation

- **Free Tier**: Up to 100 jobs per run
- **Paid Plans**: $0.50 per 1,000 jobs scraped
- **Proxy Usage**: Additional costs for residential proxies if used extensively

## 🔧 Troubleshooting

### Common Issues

- **No Results**: Check your keyword and location filters. Try broader search terms.
- **Incomplete Data**: Ensure `collectDetails` is set to `true` for full descriptions.
- **Rate Limiting**: Reduce `results_wanted` or increase delays if encountering blocks.

### Best Practices

- Use specific keywords for better results
- Set reasonable `results_wanted` limits to avoid long run times
- Enable proxies for large-scale scraping
- Monitor your Apify usage to stay within plan limits

## 📝 Notes

- Results are saved to an Apify dataset for easy export to JSON, CSV, or other formats.
- The actor respects Randstad's robots.txt and implements ethical scraping practices.
- Job data accuracy depends on Randstad's website structure, which may change over time.
- For large-scale data collection, consider running the actor during off-peak hours.

## 🤝 Support

If you encounter issues or need help configuring the actor:

- Check the Apify documentation for general guidance
- Review the input parameters carefully
- Test with small result sets first
- Contact Apify support for technical assistance

---

*This actor is maintained by the Apify community. Contributions and feedback are welcome!*