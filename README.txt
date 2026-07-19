================================================================================
  CommodityOS - Automated Market Intelligence Operating System
  Version: 1.0.0
  GitHub: https://github.com/iamgokhu/Commodityapp01
  Dashboard: https://iamgokhu.github.io/Commodityapp01/
================================================================================

WHAT IS THIS?
-------------
CommodityOS is a Python-based automated data collection system that crawls
26 websites to collect commodity product data across India. It collects
manufacturer, wholesaler, exporter, and retailer information for products
like sugar, rice, wheat, pulses, spices, grains, dairy, and more.

The system features:
- 26 CrawlSpiders with real HTTP requests (aiohttp + BeautifulSoup)
- robots.txt compliance and per-domain rate limiting
- 8-stage data processing pipeline
- Knowledge graph with relationship discovery
- Auto-generated dashboard (HTML/JS) published to GitHub Pages
- Real-time monitoring (CPU, RAM, Disk)

REQUIREMENTS
------------
- Python 3.9 or higher
- pip (Python package manager)
- Git (optional, for GitHub integration)

INSTALLATION
------------
1. Extract this zip file to any folder.

2. Open a terminal/command prompt and navigate to the extracted folder:
   cd path\to\Commodity

3. Install the required Python packages:
   pip install -r requirements.txt

   Or install manually:
   pip install networkx aiohttp psutil beautifulsoup4 lxml

QUICK START
-----------
To run the full data collection pipeline:

   python -m commodity_os.main

This will:
- Initialize all 26 CrawlSpiders
- Crawl websites and collect commodity entity data
- Process data through the pipeline (dedup, validation, scoring)
- Build a knowledge graph with relationships
- Generate an HTML dashboard in the output/ folder
- Auto-commit and push to GitHub (if configured)
- Repeat every 30 seconds in a loop

To stop the collection loop, press Ctrl+C.

INDIVIDUAL COMMANDS
-------------------
Run tests:
   python -m commodity_os.testing.tests

Run a single collection cycle:
   python -c "
   import asyncio
   from commodity_os.integration import IntegrationOrchestrator
   async def run():
       integ = IntegrationOrchestrator()
       summary = await integ.run_collection_cycle()
       print(summary)
       await integ.shutdown()
   asyncio.run(run())
   "

View the dashboard locally:
   Open output/dashboard.html in your web browser.

PROJECT STRUCTURE
-----------------
Commodity/
|-- commodity_os/              # Main Python package
|   |-- main.py               # Entry point - run this to start
|   |-- integration.py        # Wires crawlers to pipeline to dashboard
|   |-- config.json           # Products, regions, schedules config
|   |-- crawlers/
|   |   |-- base.py           # Old mock crawlers (kept as reference)
|   |   |-- spider.py         # NEW: 26 CrawlSpiders with real HTTP
|   |-- data_pipeline/
|   |   |-- pipeline.py       # 8-stage data processing pipeline
|   |-- knowledge_graph/
|   |   |-- graph.py          # NetworkX knowledge graph
|   |-- dashboard/
|   |   |-- generator.py      # HTML dashboard generator
|   |-- monitoring/
|   |   |-- monitor.py        # CPU/RAM/Disk monitoring
|   |-- meta_agents/          # System, Quality, Executive agents
|   |-- github_integration/   # Git auto-commit and push
|   |-- reports/              # Report generation (JSON, HTML, PDF)
|   |-- testing/              # Test suite (23 tests)
|-- docs/                     # GitHub Pages (dashboard files)
|   |-- index.html            # Live dashboard
|   |-- data.json             # Dashboard data (auto-refreshes)
|-- output/                   # Generated files
|   |-- all_entities.json     # All collected entities
|   |-- dashboard.html        # Generated dashboard
|   |-- dashboard.json        # Dashboard data
|-- data/                     # Knowledge graph persistence
|-- requirements.txt          # Python dependencies
|-- README.txt                # This file

CRAWLSPIDERS (26 Total)
------------------------
INDIA B2B:
  1. IndiaMART (indiamart.com) - B2B marketplace
  2. TradeIndia (tradeindia.com) - Trade portal
  3. AgMarkNet (agmarknet.gov.in) - Government prices

INDIA RETAIL:
  4. Amazon India (amazon.in) - B2B listings
  5. Flipkart (flipkart.com) - Wholesale
  6. JioMart (jiomart.com) - Grocery
  7. BigBasket (bigbasket.com) - Grocery
  8. Blinkit (blinkit.com) - Quick commerce
  9. Zepto (zepto.com) - Quick commerce
 10. Swiggy Instamart (swiggy.com) - Quick commerce
 11. Reliance Fresh (reliancefresh.com) - Retail
 12. DMart (dmart.in) - Supermarket
 13. More Retail (more.com) - Supermarket
 14. Spencer's (spencers.in) - Retail

CORPORATE FARMING:
 15. Corporate Farm - Agri-business
 16. Marine Harvest - Aquaculture
 17. Seafood Export - Marine trade

GLOBAL:
 18. Alibaba (alibaba.com) - B2B global
 19. Amazon Global (amazon.com) - Global marketplace
 20. Walmart (walmart.com) - Global retail
 21. Costco (costco.com) - Wholesale
 22. Carrefour (carrefour.com) - Global retail
 23. Tesco (tesco.com) - Global retail

OTHER:
 24. LinkedIn (linkedin.com) - Company profiles
 25. DataGovIn (data.gov.in) - Government data
 26. News (commodity_news) - Commodity news

DATA COLLECTED PER ENTITY
--------------------------
- Company name and entity type (Manufacturer/Wholesaler/Exporter)
- Contact details (phone, email, website)
- Address (state, district, taluk)
- Commodity group and specific product
- Pricing (market price, purchase price, selling price)
- Year of establishment
- GST number (when available)
- Payment terms and delivery availability
- Source website and collection timestamp

COMMODITY CATEGORIES
--------------------
- Cereals: Rice, Wheat, Maize, Bajra, Jowar, Ragi
- Pulses: Toor, Moong, Masoor, Chana, Urad, Rajma
- Sugar: Raw, Refined, Jaggery, Brown, Organic
- Oilseeds: Mustard, Groundnut, Soybean, Sunflower, Sesame
- Spices: Turmeric, Chili, Cumin, Coriander, Cardamom, Pepper
- Fruits: Mango, Banana, Apple, Grapes, Orange, Pomegranate
- Vegetables: Onion, Potato, Tomato, Cabbage, Cauliflower
- Tea & Coffee: Green Tea, Black Tea, Coffee Beans
- Cotton & Fiber: Cotton, Jute, Silk, Wool
- Dairy: Milk, Butter, Cheese, Ghee, Paneer
- Nuts & Dry Fruits: Almonds, Cashews, Walnuts, Raisins

CONFIGURATION
-------------
Edit commodity_os/config.json to change:
- Products to crawl
- Regions (states/districts) to cover
- Collection schedule and intervals
- GitHub repository settings

GITHUB PAGES DEPLOYMENT
------------------------
The dashboard is auto-deployed to GitHub Pages:
https://iamgokhu.github.io/Commodityapp01/

To enable auto-push, configure git:
  git config user.email "commodity@os.local"
  git config user.name "CommodityOS"

TROUBLESHOOTING
---------------
1. "Module not found" errors:
   -> Run: pip install -r requirements.txt

2. "Git not found" errors:
   -> Refresh PATH or restart terminal after installing Git

3. Low entity count:
   -> Increase max_pages in spider.py CrawlSpiderManager
   -> Check network connectivity

4. Dashboard not updating:
   -> Run: python -m commodity_os.main
   -> Check output/dashboard.html exists

5. Tests failing:
   -> Run: python -m commodity_os.testing.tests

LICENSE
-------
MIT License - Free to use and modify.

SUPPORT
-------
GitHub Issues: https://github.com/iamgokhu/Commodityapp01/issues
