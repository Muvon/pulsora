# Pulsora Website

This directory contains the static website for Pulsora - High-Performance Time-Series Database.

## Files

- `index.html` - Main website page with all sections
- `styles.css` - Styling (unchanged from original, uses Muvon branding colors)
- `script.js` - JavaScript for animations and GitHub stars fetching

## Structure

The website includes the following sections:

1. **Hero Section** - Main introduction with key value proposition
2. **Problem/Solution** - Comparison with traditional time-series databases
3. **How It Works** - API examples showing ingestion, queries, and exports
4. **Core Features** - 6 key features with icons
5. **Architecture** - Technical components and design
6. **Performance** - Benchmarked metrics for ingestion, queries, and compression
7. **Configuration** - Tuning options and settings
8. **Data Formats** - Supported formats (CSV, Arrow, Protobuf, JSON)
9. **Why Pulsora** - Key differentiators
10. **Quick Start** - Installation and setup instructions
11. **Open Source** - License and community information

## Branding

- Uses Muvon logo and color scheme (#76B744 / #7CB342 green)
- "OPEN SOURCE" badge in navigation
- Apache 2.0 license
- Muvon Un Limited (Hong Kong) copyright

## Deployment

Simply serve the `website/` directory with any static file server:

```bash
# Python
python3 -m http.server 8000

# Node.js
npx serve website

# Or copy to your web server
cp -r website/* /var/www/html/
```

## Customization

All content is now specific to Pulsora:
- Time-series database focus
- Market data optimization
- Performance metrics from actual benchmarks
- API examples from documentation
- Links to GitHub repository and docs

The style and structure remain consistent with the original Muvon design system.
