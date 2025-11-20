# Randstad Jobs Scraper

Scrape job listings from Randstad.com, the global staffing and recruitment leader. This Apify actor automatically extracts comprehensive job data including positions, locations, salaries, and descriptions from Randstad's extensive international job database.

## 🌟 What is Randstad?

Randstad is the global leader in HR services, connecting people and organizations worldwide. With operations in 39 countries and over 4,700 branches, Randstad provides workforce solutions including temporary staffing, permanent placement, and HR consulting across all industries and sectors.

## 🚀 Key Features

- **⚡ High-Performance Scraping**: Optimized for speed and efficiency with minimal resource usage
- **🎯 Complete Job Data Extraction**: Captures all essential job details including title, location, salary, contract type, posting date, and full descriptions
- **🔍 Flexible Search Capabilities**: Search by keywords, locations, or specific job categories across France
- **📄 Intelligent Pagination**: Automatically navigates through multiple result pages
- **🛡️ Reliable Data Collection**: Built-in mechanisms to handle rate limits and ensure consistent scraping
- **📊 Clean Structured Output**: Delivers data in standardized JSON format for easy analysis and integration
- **🌐 Proxy Integration**: Supports proxy configurations for enhanced reliability
- **📈 Scalable Architecture**: Designed for both small searches and large-scale job market analysis

## 📋 Input Parameters

Configure your job search with these parameters:

| Parameter | Type | Description | Default | Required |
|-----------|------|-------------|---------|----------|
| `keyword` | string | Job title, skill, or keyword to search for (e.g., "software engineer", "chef de projet", "data analyst") | - | No |
| `location` | string | Geographic location filter (e.g., "Paris", "Lyon", "Marseille") | - | No |
| `category` | string | Job sector or category filter | - | No |
| `startUrl` / `url` / `startUrls` | string/array | Direct Randstad search URL(s) to begin scraping from | - | No |
| `results_wanted` | integer | Maximum number of job listings to collect (1-10000) | 100 | No |
| `max_pages` | integer | Maximum number of search result pages to process | 20 | No |
| `collectDetails` | boolean | Whether to scrape full job descriptions from detail pages | true | No |
| `proxyConfiguration` | object | Proxy settings for improved scraping reliability | Apify Proxy | No |

### Input Examples

#### Basic Job Search
```json
{
  "keyword": "développeur web",
  "location": "Paris",
  "results_wanted": 50
}
```

#### Advanced Configuration with Proxy
```json
{
  "startUrls": ["https://www.randstad.com/jobs/"],
  "collectDetails": true,
  "max_pages": 10,
  "proxyConfiguration": {
    "useApifyProxy": true,
    "apifyProxyGroups": ["RESIDENTIAL"]
  }
}
```

#### Targeted Industry Search
```json
{
  "keyword": "infirmier",
  "location": "Lyon",
  "category": "santé",
  "results_wanted": 200,
  "collectDetails": true
}
```

## 📊 Output Data Structure

Each job listing is returned as a structured JSON object:

```json
{
  "title": "Software Engineer H/F",
  "company": "Randstad",
  "location": "Paris, Île-de-France",
  "salary": "45 000 € / an",
  "contract_type": "CDI",
  "date_posted": "20/11/2025",
  "description_html": "<p>Detailed job description with requirements...</p>",
  "description_text": "Plain text version of the complete job description...",
  "url": "https://www.randstad.com/jobs/software-engineer-london_uk_12345.html"
}
```

### Field Descriptions

- **`title`**: Job position title
- **`company`**: Always "Randstad" (the staffing agency)
- **`location`**: Job location worldwide (city, region, country)
- **`salary`**: Compensation information when available
- **`contract_type`**: Employment type (CDI, CDD, Intérim, etc.)
- **`date_posted`**: Publication date of the job posting
- **`description_html`**: Full job description with HTML formatting
- **`description_text`**: Plain text version for easy processing
- **`url`**: Direct link to the job posting on Randstad.com

## 🛠️ Usage Guide

### Running on Apify Platform

1. **Create a New Task**: Go to your Apify account and create a new task
2. **Select Actor**: Search for "Randstad Jobs Scraper" and select this actor
3. **Configure Input**: Enter your search parameters using the form or JSON editor
4. **Run Task**: Click "Run" to start the scraping process
5. **Download Results**: Once complete, download your data in JSON, CSV, or other formats

### API Integration

Use the Apify API for programmatic access:

```bash
curl -X POST https://api.apify.com/v2/acts/your-actor-id/runs \
  -H "Authorization: Bearer YOUR_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "keyword": "commercial",
    "location": "Bordeaux",
    "results_wanted": 100,
    "collectDetails": true
  }'
```

### Webhook Integration

Set up webhooks to automatically receive results when scraping completes:

```json
{
  "webhookUrl": "https://your-app.com/webhook",
  "webhookMethod": "POST"
}
```

## ⚙️ Configuration & Best Practices

### Memory & Performance

- **Recommended Memory**: 4GB for optimal performance
- **Concurrent Processing**: Handles multiple pages simultaneously for faster results
- **Rate Limiting**: Built-in delays prevent blocking and ensure reliable operation

### Optimization Tips

- **Targeted Searches**: Use specific keywords and locations for better results
- **Result Limits**: Set reasonable `results_wanted` to balance speed and data volume
- **Detail Collection**: Enable `collectDetails` for comprehensive job information
- **Proxy Usage**: Always use proxy configuration for production scraping
- **Scheduling**: Run during off-peak hours for best performance

### Cost Optimization

- **Free Tier**: Suitable for small searches (up to 100 jobs)
- **Pay-as-you-go**: Scales with usage for larger datasets
- **Data Storage**: Results stored securely in your Apify account

## 🔧 Troubleshooting

### Common Issues & Solutions

| Issue | Cause | Solution |
|-------|-------|----------|
| No results found | Search terms too specific | Try broader keywords or different locations |
| Incomplete descriptions | `collectDetails` disabled | Enable `collectDetails` for full job info |
| Rate limiting errors | Too many requests | Use proxy configuration and reduce concurrency |
| Timeout errors | Large result sets | Decrease `results_wanted` or increase memory |
| Location not found | Spelling or format issues | Use standard French city names |

### Error Messages

- **"No job links found"**: Check your search parameters and try different keywords
- **"Detail page failed"**: Temporary site issue; the actor will retry automatically
- **"Proxy required"**: Enable proxy configuration for better reliability

## 📈 Use Cases & Applications

### Recruitment & HR
- **Talent Acquisition**: Find candidates for specific roles across France
- **Market Research**: Analyze job market trends and salary ranges
- **Competitive Intelligence**: Monitor competitor hiring patterns

### Job Seekers
- **Personal Job Search**: Aggregate jobs from Randstad's extensive database
- **Career Planning**: Research salary trends and job availability by region

### Data Analysis
- **Economic Research**: Study employment trends in different sectors
- **Business Intelligence**: Analyze hiring patterns by company and industry

### Integration Examples
- **CRM Systems**: Import job data into recruitment software
- **Job Boards**: Sync with other job platforms
- **Analytics Tools**: Feed data into BI dashboards

## 📊 Data Quality & Limitations

### Data Freshness
- **Real-time Updates**: Scrapes current live data from Randstad.com
- **Update Frequency**: Jobs are updated as they appear on the site

### Coverage
- **Comprehensive**: Covers all job categories and contract types
- **Geographic**: All French regions and major cities
- **Language**: Primarily French job listings

### Limitations
- **Site Dependency**: Relies on Randstad.com website structure
- **Rate Limits**: Subject to website restrictions
- **Data Availability**: Only includes jobs currently posted on Randstad

## 🔒 Legal & Compliance

### Terms of Service
- Review Randstad's terms before large-scale scraping
- Respect robots.txt and website policies
- Use for legitimate business and research purposes

### Data Protection
- Comply with French data protection regulations (RGPD)
- Handle personal data responsibly
- Use scraped data in accordance with applicable laws

## 🤝 Support & Resources

### Getting Help
- **Apify Community**: Join discussions and get help from other users
- **Documentation**: Check Apify's official documentation
- **Support**: Contact Apify support for technical issues

### Related Resources
- [Randstad Global Official Website](https://www.randstad.com)
- [Apify Platform Documentation](https://docs.apify.com)
- [Apify Community Forum](https://community.apify.com)

### Version History
- **v1.0.0**: Initial release with full Randstad.com scraping capabilities

---

**Keywords**: job scraper, France jobs, Randstad scraper, employment data, recruitment scraper, French job market, automated job scraping, staffing agency data, CDI jobs, CDD positions, interim jobs, job listings France, emploi France, recrutement France, offres d'emploi, job search automation, HR data collection, talent acquisition tools, job market analysis

*Last updated: November 2025*