# Commodity Data Collection - Agent Team

## Project Purpose
32-agent team collecting commodity product data across India:
- **Products**: Sugar, Rice, Grains, Pulses, Wheat, Dals, Basmathi Rice
- **Geography**: By Taluk → District → State (all India)
- **Entities**: Manufacturer, Wholesaler, Exporter

## Data Fields Per Entity
- Contact details
- Year of establishment
- GST (optional)
- Office address
- Market price today (per SKU)
- Purchase price & market selling price
- Payment terms
- Support services
- Delivery availability

## Agent Architecture
- 32 agents total
- Likely organized by: region (state/district), product category, entity type
- Each agent responsible for data collection in assigned scope

## Development Notes
- New project - no existing codebase, config, or tooling
- Define agent coordination, data schema, storage, and deduplication strategy
- Consider rate limiting, respectful scraping, and data freshness