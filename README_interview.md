# Interview Notes

## Project Planning
### Success Metrics
- Logically structured (reasonable benefits for drawbacks)
- Concurrency used effectively
- Extensibility (e.g. ability to make updates during interview)

### High-Level Requirements
1. Seed url to be provided via command line
2. Crawls only internal links
3. No use of web crawling frameworks (scrapy, crawlee)
4. Must use concurrency as appropriate
5. *Written as a production piece of code


### Assumptions
 - If provided a sub domain, we should limit the search to said sub-domain
 - Persistence of some sort is desirable for fault tolerance/error handling
   -
 - Memory is limited to the average laptop (e.g. cache management is needed )

### Unknowns
- Purpose
  - What data is needed from these sites (e.g. are images needed? downloadable data sets?)
  - Are periodic updates needed to the stored html docs?
- Scale
  - What size of website, how many websites, etc. should the process be able to store data for?
- Latency
  - What constraints will there be regarding turn around time?


### Design Decisions
#### Link and HTML Aggregation (Indexing)
- In [Data Flow](#data-flow) 2, we would ideally collect links via a site map, but smaller sites often do not surface a site map. So, we use the set of links on the seed webpage as a proxy for the site map urls
- We separate link aggregation and the pulling of html data from the parsing of the html to facilitate the storage of raw html data. This data is retained to minimize the effect our 'politeness' (see robots.txt) has on restarts after a failure, reparsing needed after a change in parsing procedure, data review during error handling etc.
- Similarly to sitemaps, we save the contents of robots.txt to our database for reference during crawling. Though repulling this data does not take many resources, this allows:
  - concurrently running crawlers to ensure consistency in restrictions
  - analytics around projected crawl times to be considered

#### HTML Parsing
- HTML parsing is handled by concurrency-capable workers, receiving

#### Limitations
- If a sigkill signal is reveived, the manager class does not close cleanly
-


## Project Structure


## Data Flow
1. Seed url (possibly as part of a batch) is passed by the user
2. The Manager class creates/connects to a sqlite db, and a redis-server
   1. Redis server interaction occurs through a HTML Content
3. First order links are scrapped, collected and written to the db
      - Links are aggregated via the sitemap
      - Politness protocol is determined via robots.txt (if available)
4. Links are reviewd holistically and are organized into work items
      - The set of links within each work queue is based on the structure of the site.
      - There is room for more sophisticated scheduling logic here
5. Work items are a queue, with configurations based off of the meta data gathered in step 2
      - Back-off times on failure, max retries, politeness permissions etc.
6. Work items sequentially process items based on the following procedure
      - The HTML for each link is downloaded and written to the database
      - The manager is informed that the contents of the work item have been completed
      - The HTML for each link is parsed, and relevant data for said link is saved to the db
      - The manager is informed that the relevant data has been parsed
      - The WARC file representing the crawl performed is saved to object based storage
7. Download workers read from the queue and read data from the
8. Workers that process the contents of the queue are started



## Workflow Tools
Due to [requirement 5](#high-level-requirements), more robust workflow tooling has been added than one might expect for a command line tool. Tools used:

- Pre-commit run, ruff powered linting
  - May catch small 'obvious' errors, but primarily is used for read ability
  - Makes the review process easier, enables future collaboration
- GitActions Workflows
  - Primarily, enables automated testing and eliminates manual work on the part of the developer
- Logging Configuration
  - Rich text formatting both improves UX for the CLI interface and the developer experience
  - File based logging enhances visibility into past runs



### Components

###
