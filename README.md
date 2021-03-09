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

Web pages can be represented by the objects that comprise their structure and content. This representation is known as the Document Object Model (DOM). ![DOM](./fig/DOM-model.svg.png) The purpose of the DOM is to provide an interface for programs to change the structure, style, and content of web pages. The DOM represents the document as nodes and objects. Amongst other things, this allows programming languages to interactively change the page and HTML!

Beautiful Soup is a Python library designed for quick scraping projects. It allows us to select and navigate the tree-like structure of HTML documents, searching for particular tags, attributes or ids. It also allows us to then further traverse the HTML documents through relations like children or siblings. In other words, with Beautiful Soup, we could first select a specific div tag and then search through all of its nested tags. With the help of Beautiful Soup, we can:

* Navigate HTML documents using Beautiful Soup's children and sibling relations
* Select specific elements from HTML using Beautiful Soup
* Use regular expressions to extract items with a certain pattern within Beautiful Soup
* Determine the pagination scheme of a website and scrape multiple pages
* Identify and scrape images from a web page
* Save images from the web as well as display them in a Pandas DataFrame for easy perusal

## API, Client-Server Model and HTTP Request/Response Cycle

**APIs** (short for **Application Programming Interfaces**) are an important aspect of the modern internet. APIs are what allows everything on the internet to play nicely with each other and work together. An API is a communication protocol between 2 software systems. It describes the mechanism through which if one system **requests** some information using a predefined format, a remote system **responds** with an outcome that gets sent back to the first system.

APIs are a way of allowing 2 applications to interact with each other. This is an incredibly common task in modern web-based programs. For instance, if you've ever connected your facebook profile to another service such as Spotify or Instagram, this is done through APIs. An API represents a way for 2 pieces of software to interact with one another. Under the hood, the actual request and response is done as an **HTTP Request**. The following diagram shows the HTTP Request/Response Cycle: ![HTTP Request/Response Cycle](./fig/new_client-server-illustration.png)

### Web Applications

A Web application (Web app) is an application program that is stored on a remote server and delivered over the Internet through a browser interface. Web services are Web apps by definition and many, although not all, websites contain Web apps. Any website component that performs some function for the user qualifies as a Web app. Google’s search engine is a web app, yet its root concept is hardly different from a phone directory that enables you to search for names or numbers.

Most web apps actually use a browser interface for interaction, i.e. end users request access and request information/service from these applications through a modern web browser interface. There are hundreds of ways to build and configure a Web application but most of them follow the same basic structure: a web client, a web server, and a database.

### Web Clients

The client is what the end user interacts with. "Client-side" code is actually responsible for most of what a user actually sees. For `requesting` some information as a web page, the client side may be responsible for: includes:

* Defining the structure of the Web page
* Setting the look and feel of the Web page
* Implementing a mechanism for responding to user interactions (clicking buttons, entering text, etc.)

Most of these tasks are managed by HTML/CSS/JavaScript-like technologies to structure the information, style of the page and provide interactive objects for navigation and focus.

### Web Servers

A web server in a Web application is what *listens to requests* coming in from the clients. When you set up an HTTP (HyperText Transfer Protocol - Language of the internet) server, we set it up to *listen to a port number*. A port number is always associated with the IP address of a computer. You can think of ports as separate channels on a computer that we can use to perform different tasks: one port could be surfing www.facebook.com while another fetches your email. This is possible because each of the applications (the Web browser and the email client) use different port numbers.

Once you've set up an HTTP server to listen to a specific port, the server waits for client requests coming to that specific port. After authenticating the client, the server performs any actions stated by the request and sends any requested data via an HTTP response.

### Web Databases

Databases are the foundations of Web architecture. An SQL/NoSQL or a similar type of database is a place to store information so that it can easily be accessed, managed, and updated. If you're building a social media site, for example, you might use a database to store information about your users, posts, comments, etc. When a visitor requests a page, the data inserted into the page comes from the site's database, allowing real-time user interactions with sites like Facebook or apps like Gmail. ![web database]()

In the example image above, we can see the above-mentioned setup in action. A browser sends a request to a web server by calling its domain i.e. www.google.com. Based on who the requester is, the server collects necessary information for an SQL database. This information is wrapped as HTML code and sent back to the client. The web browser reads the structuring and styling information embedded within HTML and displays the page to the user accordingly.

When developing a Web application, the request/response cycle is a useful guide to see how all the components of the app fit together. The request/response cycle traces how a user's request flows through the app. Understanding the request/response cycle is helpful to figure out which files to edit when developing an app (and where to look when things aren't working).

## Questions:

### 7/ Qualifications and Certifications
Q: What are the top three qualifications or certifications requested by employers?

