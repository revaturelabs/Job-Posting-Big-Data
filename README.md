# Job-Posting-Big-Data

## Prerequisites

Before you begin, ensure you have met the following requirements:
<!--- These are just example requirements. Add, duplicate or remove as required --->
* You have installed the latest version of [cdx-toolkit](https://github.com/cocrawler/cdx_toolkit)
* You have a `<Windows/Linux/Mac>` machine. 
* You have read a list of links about how to download and manipuate common crawl data as follows:

  - [ccmain starts](https://commoncrawl.org/the-data/get-started/)

  - [ccmain index](https://index.commoncrawl.org/)

  - [ccmain projs](http://commoncrawl.org/the-data/examples/)

  - [ccmain warcs](https://commoncrawl.org/2018/03/index-to-warc-files-and-urls-in-columnar-format/)

  - [search 2021.01](https://index.commoncrawl.org/CC-MAIN-2021-04)

## HTML, CSS, Web Scraping and Beautiful Soup

HTML, or HyperText Markup Language, is a markup language that describes the structure and semantic meaning of web pages. Web browsers, such as Mozilla Firefox, Internet Explorer, and Google Chrome interpret the HTML code and use it to render output. Unlike Python, JavaScript and other programming languages, markup languages like HTML don't have any logic behind them. Instead, they simply surround the content to convey structure and meaning.

HTML lets us mark-up our content with semantic structure. It forms the skeleton of our web page. It would be great to be able to say, "Browser, when we see a `p` tag with `id` of `my-name`, make the first letter be huge!" Or, to get our readers' attention, we might say, "Browser, if you see any tag with a class of warning surround it with a red box!" HTML authors believe that **creating** marked-up documents and **styling** marked-up documents are entirely separate tasks. They see a difference between **writing content** (the data within the HTML document) and specifying presentation, the rules for **displaying** the rendered **elements**.

Browsers combine the content (HTML) and presentation (CSS) layers to display web pages. CSS, or "Cascading Style Sheets," tells us how to write rules that define how browsers will present HTML. Rules in CSS won't look like HTML and they usually live in a file apart from our HTML file. CSS is the language for styling web pages. CSS instructions live apart from the HTML elements and have a different look and feel ("syntax"). CSS directives give web pages their specific look and feel. If you have ever been impressed by how a website can be displayed on a desktop browser while the same content looks great on a mobile device, you have CSS to thank for it!

Web pages can be represented by the objects that comprise their structure and content. This representation is known as the Document Object Model. ![DOM](./fig/DOM-model.svg.png) The purpose of the DOM is to provide an interface for programs to change the structure, style, and content of web pages. The DOM represents the document as nodes and objects. Amongst other things, this allows programming languages to interactively change the page and HTML!

Beautiful Soup is a Python library designed for quick scraping projects. It allows us to select and navigate the tree-like structure of HTML documents, searching for particular tags, attributes or ids. It also allows us to then further traverse the HTML documents through relations like children or siblings. In other words, with Beautiful Soup, we could first select a specific div tag and then search through all of its nested tags. With the help of Beautiful Soup, we can:

* Navigate HTML documents using Beautiful Soup's children and sibling relations
* Select specific elements from HTML using Beautiful Soup
* Use regular expressions to extract items with a certain pattern within Beautiful Soup
* Determine the pagination scheme of a website and scrape multiple pages
* Identify and scrape images from a web page
* Save images from the web as well as display them in a Pandas DataFrame for easy perusal

## API, Client-Server Model and HTTP Request/Response Cycle

**APIs** (short for **Application Programming Interfaces**) are an important aspect of the modern internet. APIs are what allows everything on the internet to play nicely with each other and work together. An API is a communication protocol between 2 software systems. It describes the mechanism through which if one system **requests** some information using a predefined format, a remote system **responds** with an outcome that gets sent back to the first system.

APIs are a way of allowing 2 applications to interact with each other. This is an incredibly common task in modern web-based programs. For instance, if you've ever connected your facebook profile to another service such as Spotify or Instagram, this is done through APIs. An API represents a way for 2 pieces of software to interact with one another. Under the hood, the actual request and response is done as an **HTTP Request**. The following diagram shows the ![HTTP Request/Response Cycle](./fig/new_client-server-illustration.png):


## Questions:

### 1/ Tech ads proportional to population (selected)
Q: Where do we see relatively fewer tech ads proportional to population?

### 2/ Largest job seekers
Q: What are the three companies posting the most tech job ads?

### 3/ Percent of relatively infrequent job seekers (selected)
Q: What percent of tech job posters post no more than three job ads a month?

### 4/ Job posting Spikes
Q: Is there a significant spike in tech job postings at the end of business quarters?

Q: If so, which quarter spikes the most?

### 5/ Tech job posting trend
Q: Is there a general trend in tech job postings over the past year?

Q: What about the past month?

### 6/ Entry Level Experience
Q: What percentage of entry level jobs require previous experience?

### 7/ Qualifications and Certifications
Q: What are the top three qualifications or certifications requested by employers?

### 8/ Low Code/No Code [Ref.](https://en.wikipedia.org/wiki/Low-code_development_platform)
Q: What percentage of Tech Job Listings require experience in a Low Code or No Code solution? 

