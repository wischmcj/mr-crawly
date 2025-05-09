# Components
## Manager
This is where we would source seed urls for the crawler.
At minimum this could just be a user provided queue (as is the case in the base project). However, when you want to avoid being locked out of websites that do not provide guidelines via robots.txt, some form of backoff and visibility based retry queue can be really helpful.

## Parser
This is the portion of the project that would handle extracting info from the html data. It will:
 - Read the html data extracted from the formerly-fronteir urls via crawler
 - Extract features such as download links, image locations etc.
 - Organize the extracted information in the DB

## Worker
This is the portion of the project that would handle:
- Building a network of urls based on the seedurl
- pulling down the contents of the webpages for the parser to parse
