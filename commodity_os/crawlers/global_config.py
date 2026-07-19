"""Global region and country configurations for 2400+ CrawlSpiders.

9 regions, 180+ countries, commodity-specific spider templates.
Each country gets spiders for: B2B, Retail, Government, News, Corporate Farm.
"""
from typing import Dict, List, Any

# ---------------------------------------------------------------------------
# 9 Global Regions
# ---------------------------------------------------------------------------

REGIONS: Dict[str, Dict[str, Any]] = {
    "South Asia": {
        "countries": [
            "India", "Bangladesh", "Pakistan", "Sri Lanka", "Nepal",
            "Bhutan", "Maldives", "Afghanistan",
        ],
        "priority_commodities": ["Rice", "Wheat", "Pulses", "Sugar", "Tea", "Cotton"],
        "b2b_sites": ["indiamart.com", "tradeindia.com", "alibaba.com"],
        "retail_sites": ["amazon.in", "flipkart.com", "jiomart.com", "bigbasket.com"],
        "gov_sites": ["data.gov.in", "agmarknet.gov.in"],
        "news_sites": ["economictimes.indiatimes.com", "business-standard.com"],
    },
    "Southeast Asia": {
        "countries": [
            "Thailand", "Vietnam", "Indonesia", "Philippines", "Malaysia",
            "Myanmar", "Cambodia", "Laos", "Singapore", "Brunei", "Timor-Leste",
        ],
        "priority_commodities": ["Rice", "Rubber", "Palm Oil", "Spices", "Seafood", "Coconut"],
        "b2b_sites": ["alibaba.com", "indiamart.com", "tradeindia.com"],
        "retail_sites": ["lazada.com", "shopee.com", "tokopedia.com"],
        "gov_sites": ["data.go.th", "bps.go.id"],
        "news_sites": ["bangkokpost.com", "thejakartapost.com"],
    },
    "East Asia": {
        "countries": [
            "China", "Japan", "South Korea", "Taiwan", "Mongolia",
            "Hong Kong", "Macau", "North Korea",
        ],
        "priority_commodities": ["Rice", "Wheat", "Soybean", "Tea", "Seafood", "Silk"],
        "b2b_sites": ["alibaba.com", "made-in-china.com", "globalsources.com"],
        "retail_sites": ["amazon.co.jp", "jd.com", "coupang.com"],
        "gov_sites": ["stats.gov.cn", "maff.go.jp"],
        "news_sites": ["nikkei.com", "globaltimes.cn"],
    },
    "Middle East": {
        "countries": [
            "UAE", "Saudi Arabia", "Qatar", "Kuwait", "Bahrain",
            "Oman", "Jordan", "Lebanon", "Iraq", "Iran", "Israel",
            "Turkey", "Syria", "Yemen", "Palestine",
        ],
        "priority_commodities": ["Wheat", "Rice", "Sugar", "Dates", "Oilseeds", "Dairy"],
        "b2b_sites": ["alibaba.com", "tradearabia.com", "dubizzle.com"],
        "retail_sites": ["amazon.ae", "noon.com", "carrefouruae.com"],
        "gov_sites": ["zayed.gov.ae", "samra.gov.sa"],
        "news_sites": ["gulfbusiness.com", "arabianbusiness.com"],
    },
    "Europe": {
        "countries": [
            "UK", "Germany", "France", "Italy", "Spain", "Netherlands",
            "Belgium", "Portugal", "Switzerland", "Austria", "Poland",
            "Czech Republic", "Hungary", "Romania", "Bulgaria", "Greece",
            "Sweden", "Norway", "Finland", "Denmark", "Ireland", "Iceland",
            "Croatia", "Serbia", "Slovakia", "Slovenia", "Lithuania",
            "Latvia", "Estonia", "Ukraine", "Belarus", "Russia",
            "Moldova", "Albania", "North Macedonia", "Bosnia", "Montenegro",
            "Kosovo", "Luxembourg", "Malta", "Cyprus", "Georgia", "Armenia",
            "Azerbaijan", "Turkey",
        ],
        "priority_commodities": ["Wheat", "Sugar", "Dairy", "Wine", "Olive Oil", "Grains"],
        "b2b_sites": ["alibaba.com", "made-in-germany.com", "europages.com"],
        "retail_sites": ["amazon.de", "amazon.co.uk", "tesco.com", "carrefour.com"],
        "gov_sites": ["ec.europa.eu/eurostat", "gov.uk"],
        "news_sites": ["reuters.com", "ft.com", "bbc.com"],
    },
    "Africa": {
        "countries": [
            "Nigeria", "South Africa", "Kenya", "Ethiopia", "Ghana",
            "Tanzania", "Uganda", "Mozambique", "Madagascar", "Cameroon",
            "Cote d'Ivoire", "Senegal", "Mali", "Burkina Faso", "Niger",
            "Chad", "Sudan", "South Sudan", "Somalia", "Djibouti",
            "Eritrea", "Rwanda", "Burundi", "Malawi", "Zambia",
            "Zimbabwe", "Botswana", "Namibia", "Angola", "Congo",
            "DRC", "Gabon", "Equatorial Guinea", "Sao Tome", "Cape Verde",
            "Comoros", "Mauritius", "Seychelles", "Lesotho", "Eswatini",
            "Sierra Leone", "Liberia", "Guinea", "Gambia", "Guinea-Bissau",
            "Togo", "Benin", "Central African Rep.", "Libya", "Tunisia",
            "Algeria", "Morocco", "Egypt", "Mauritania", "Western Sahara",
            "Réunion", "Mayotte", "Somaliland",
        ],
        "priority_commodities": ["Cocoa", "Coffee", "Tea", "Cotton", "Cashew", "Spices"],
        "b2b_sites": ["alibaba.com", "indiamart.com", "afrikta.com"],
        "retail_sites": ["jumia.com", "amazon.com", "takealot.com"],
        "gov_sites": ["worldbank.org", "fao.org"],
        "news_sites": ["africanews.com", "bloombergafrica.com"],
    },
    "North America": {
        "countries": [
            "USA", "Canada", "Mexico", "Guatemala", "Honduras",
            "El Salvador", "Nicaragua", "Costa Rica", "Panama",
            "Belize", "Cuba", "Jamaica", "Haiti", "Dominican Republic",
            "Trinidad and Tobago", "Barbados", "Bahamas", "Antigua",
            "Dominica", "Grenada", "St. Lucia", "St. Vincent",
            "St. Kitts", "Puerto Rico", "Guadeloupe", "Martinique",
        ],
        "priority_commodities": ["Corn", "Wheat", "Soybean", "Sugar", "Cotton", "Coffee"],
        "b2b_sites": ["alibaba.com", "amazon.com", "thomasnet.com"],
        "retail_sites": ["amazon.com", "walmart.com", "costco.com", "target.com"],
        "gov_sites": ["usda.gov", "statistics.canada.ca"],
        "news_sites": ["reuters.com", "bloomberg.com", "wsj.com"],
    },
    "South America": {
        "countries": [
            "Brazil", "Argentina", "Chile", "Colombia", "Peru",
            "Venezuela", "Ecuador", "Bolivia", "Paraguay", "Uruguay",
            "Guyana", "Suriname", "French Guiana",
        ],
        "priority_commodities": ["Soybean", "Sugar", "Coffee", "Corn", "Wheat", "Beef"],
        "b2b_sites": ["alibaba.com", "mercadolibre.com", "b2brazil.com"],
        "retail_sites": ["mercadolibre.com", "amazon.com.br", "magazineluiza.com.br"],
        "gov_sites": ["ibge.gov.br", "indec.gob.ar"],
        "news_sites": ["reuters.com", "bloomberg.com.br", "infobae.com"],
    },
    "Oceania": {
        "countries": [
            "Australia", "New Zealand", "Papua New Guinea", "Fiji",
            "Solomon Islands", "Vanuatu", "Samoa", "Tonga", "Kiribati",
            "Micronesia", "Palau", "Marshall Islands", "Nauru", "Tuvalu",
        ],
        "priority_commodities": ["Wheat", "Wool", "Beef", "Dairy", "Sugar", "Rice"],
        "b2b_sites": ["alibaba.com", "indiamart.com", "tradeindia.com"],
        "retail_sites": ["amazon.com.au", "woolworths.com.au", "countdown.co.nz"],
        "gov_sites": ["abs.gov.au", "stats.govt.nz"],
        "news_sites": ["abc.net.au", "nzherald.co.nz"],
    },
}

# ---------------------------------------------------------------------------
# Commodity categories with country-specific variants
# ---------------------------------------------------------------------------

COMMODITY_GROUPS = {
    "Cereals": ["Rice", "Wheat", "Corn", "Maize", "Barley", "Oats", "Millet", "Sorghum", "Rye", "Triticale"],
    "Pulses": ["Toor Dal", "Moong Dal", "Masoor Dal", "Chana Dal", "Urad Dal", "Chickpea", "Lentil", "Bean"],
    "Sugar": ["Raw Sugar", "Refined Sugar", "Jaggery", "Brown Sugar", "Organic Sugar", "Cane Sugar"],
    "Oilseeds": ["Soybean", "Sunflower", "Mustard", "Groundnut", "Peanut", "Sesame", "Rapeseed", "Flaxseed"],
    "Spices": ["Turmeric", "Chili", "Cumin", "Coriander", "Cardamom", "Pepper", "Cinnamon", "Clove", "Ginger"],
    "Fruits": ["Mango", "Banana", "Apple", "Grape", "Orange", "Pineapple", "Papaya", "Avocado", "Lemon"],
    "Vegetables": ["Onion", "Potato", "Tomato", "Cabbage", "Carrot", "Pepper", "Eggplant", "Squash"],
    "Tea & Coffee": ["Green Tea", "Black Tea", "Coffee Bean", "Espresso", "Instant Coffee", "Herbal Tea"],
    "Cotton & Fiber": ["Cotton", "Jute", "Silk", "Wool", "Hemp", "Linen", "Polyester"],
    "Dairy": ["Milk", "Butter", "Cheese", "Ghee", "Paneer", "Yogurt", "Cream", "Whey"],
    "Nuts & Dry Fruits": ["Almond", "Cashew", "Walnut", "Raisin", "Pistachio", "Hazelnut", "Macadamia"],
    "Seafood": ["Shrimp", "Prawn", "Lobster", "Crab", "Tuna", "Salmon", "Sardine", "Mackerel"],
    "Meat": ["Beef", "Chicken", "Mutton", "Pork", "Turkey", "Duck", "Goat"],
    "Fertilizer": ["Urea", "DAP", "NPK", "Potash", "Compost", "Organic Fertilizer"],
    "Rubber & Cocoa": ["Natural Rubber", "Cocoa Bean", "Cocoa Powder", "Cocoa Butter"],
}

# ---------------------------------------------------------------------------
# Entity types per spider category
# ---------------------------------------------------------------------------

ENTITY_TYPES = {
    "b2b": ["Manufacturer", "Wholesaler", "Exporter", "Distributor", "Supplier"],
    "retail": ["Online Seller", "Retailer", "Supermarket", "Quick Commerce"],
    "gov": ["Government Source", "Market Yard", "Statistics Bureau"],
    "news": ["News Reference", "Market Analysis", "Price Report"],
    "corporate_farm": ["Corporate Farm", "Contract Farm", "Organic Farm", "Marine Farm"],
}

# ---------------------------------------------------------------------------
# Spider templates (URL patterns per site type)
# ---------------------------------------------------------------------------

SPIDER_TEMPLATES = {
    "b2b": {
        "alibaba.com": "https://www.alibaba.com/trade/search?SearchText={commodity}+{country}",
        "indiamart.com": "https://dir.indiamart.com/search.mp?ss={commodity}+{country}&catid=&lid=&res=RC4",
        "tradeindia.com": "https://www.tradeindia.com/search.html?keyword={commodity}+{country}",
    },
    "retail": {
        "amazon": "https://www.amazon.{tld}/s?k={commodity}+wholesale",
        "walmart": "https://www.walmart.com/search?q={commodity}+bulk",
        "flipkart": "https://www.flipkart.com/search?q={commodity}+wholesale",
    },
    "gov": {
        "data.gov.in": "https://data.gov.in/search?title={commodity}+price",
        "agmarknet": "https://agmarknet.gov.in/MarketAndCommoditySearch.aspx",
    },
    "news": {
        "reuters": "https://www.reuters.com/markets/commodities/",
        "bloomberg": "https://www.bloomberg.com/markets/commodities",
    },
}

# ---------------------------------------------------------------------------
# Country TLDs for URL generation
# ---------------------------------------------------------------------------

COUNTRY_TLDS = {
    "India": "in", "USA": "com", "UK": "co.uk", "Germany": "de", "France": "fr",
    "Japan": "co.jp", "China": "cn", "Brazil": "com.br", "Australia": "com.au",
    "Canada": "ca", "Mexico": "com.mx", "Italy": "it", "Spain": "es",
    "Netherlands": "nl", "Russia": "ru", "South Korea": "co.kr",
    "Thailand": "co.th", "Vietnam": "vn", "Indonesia": "co.id",
    "Philippines": "com.ph", "Malaysia": "com.my", "Singapore": "com.sg",
    "UAE": "ae", "Saudi Arabia": "sa", "Turkey": "com.tr",
    "Nigeria": "com.ng", "South Africa": "co.za", "Kenya": "co.ke",
    "Egypt": "com.eg", "Argentina": "com.ar", "Chile": "cl",
    "Colombia": "com.co", "Peru": "com.pe", "New Zealand": "co.nz",
    "Pakistan": "pk", "Bangladesh": "com.bd", "Sri Lanka": "lk",
    "Nepal": "np", "Israel": "co.il", "Poland": "pl",
    "Czech Republic": "cz", "Hungary": "hu", "Romania": "ro",
    "Sweden": "se", "Norway": "no", "Denmark": "dk", "Finland": "fi",
    "Switzerland": "ch", "Austria": "at", "Belgium": "be",
    "Portugal": "pt", "Greece": "gr", "Ireland": "ie",
    "Taiwan": "tw", "Hong Kong": "com.hk",
}
