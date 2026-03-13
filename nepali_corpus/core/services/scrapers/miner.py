
import datetime
import logging
import re
import time
from typing import List, Set, Optional, Dict
from urllib.parse import urljoin, urlparse
import xml.etree.ElementTree as ET

from .scraper_base import ScraperBase

logger = logging.getLogger("nepali_corpus.scrapers.miner")


class DiscoveryMiner(ScraperBase):
    """
    Exhaustive URL discovery tool for websites without predefined patterns.
    Uses sitemaps, internal link crawling, pagination, archives, navigation
    sections, and auto-feed discovery to maximize article URL coverage.
    """

    def __init__(self, base_url: str, delay: float = 0.5, verify_ssl: bool = False):
        super().__init__(base_url, delay=delay, verify_ssl=verify_ssl)
        self.discovered_urls: Set[str] = set()
        self.visited_urls: Set[str] = set()
        self._robots_crawl_delay: Optional[float] = None

    def discover_all(self, max_pages: int = 200, batch_size: int = 50):
        """Run all discovery methods and yield URLs in batches."""
        logger.info(f"Starting exhaustive streaming discovery for {self.base_url}")

        reported_urls: Set[str] = set()

        def _batch_and_yield(new_urls: Set[str]):
            # Filter and normalize
            filtered = []
            for u in new_urls:
                clean_u = u.split("#")[0].rstrip("/")
                if self._is_internal(clean_u) and clean_u not in reported_urls:
                    filtered.append(clean_u)
                    reported_urls.add(clean_u)

            # Yield in chunks
            for i in range(0, len(filtered), batch_size):
                yield filtered[i : i + batch_size]

        # 0. Parse robots.txt for Crawl-delay (also discovers sitemap locs)
        self._parse_robots_txt()

        # 1. Sitemap discovery (fastest, most accurate)
        sitemaps = {u for u in self.discover_from_sitemaps() if self._is_potential_article(u)}
        yield from _batch_and_yield(sitemaps)

        # 2. Feed discovery
        feeds = {u for u in self.discover_from_feeds() if self._is_potential_article(u)}
        yield from _batch_and_yield(feeds)

        # 3. Navigation / section discovery from homepage
        nav_urls = {u for u in self.discover_from_navigation() if self._is_potential_article(u)}
        yield from _batch_and_yield(nav_urls)

        # 4. Pattern-based discovery
        patterns = {u for u in self.discover_common_patterns() if self._is_potential_article(u)}
        yield from _batch_and_yield(patterns)

        # 5. Pagination on listing pages discovered so far
        listing_pages = {u for u in reported_urls if self._is_listing_page(u)}
        if listing_pages:
            pagination_urls = self.discover_from_pagination(listing_pages, max_pages=max_pages)
            yield from _batch_and_yield(pagination_urls)

        # 6. Archive / date-based discovery
        archive_urls = self.discover_from_archives(months_back=24)
        yield from _batch_and_yield(archive_urls)

        # 7. Deeper BFS crawl if we still have very few URLs
        if len(reported_urls) < 50:
            crawl_urls = self.crawl_internal_links(max_pages=max_pages)
            yield from _batch_and_yield(crawl_urls)

        logger.info(
            f"Discovery finished for {self.base_url}. "
            f"Total unique URLs found: {len(reported_urls)}"
        )

    # ── robots.txt ──────────────────────────────────────────────────────

    def _parse_robots_txt(self) -> None:
        """Parse robots.txt for Crawl-delay and Sitemap directives."""
        try:
            resp = self.session.get(
                urljoin(self.base_url, "/robots.txt"), timeout=10
            )
            if resp.status_code != 200:
                return
            for line in resp.text.splitlines():
                line = line.strip()
                if line.lower().startswith("crawl-delay:"):
                    try:
                        self._robots_crawl_delay = float(
                            line.split(":", 1)[1].strip()
                        )
                    except ValueError:
                        pass
        except Exception:
            pass

    # ── Sitemap discovery ───────────────────────────────────────────────

    def discover_from_sitemaps(self) -> Set[str]:
        """Try to find and parse sitemaps from robots.txt or common locations."""
        sitemap_locs = [
            urljoin(self.base_url, "/robots.txt"),
            urljoin(self.base_url, "/sitemap.xml"),
            urljoin(self.base_url, "/sitemap_index.xml"),
            urljoin(self.base_url, "/sitemap/sitemap.xml"),
            urljoin(self.base_url, "/post-sitemap.xml"),
            urljoin(self.base_url, "/news-sitemap.xml"),
            urljoin(self.base_url, "/category-sitemap.xml"),
        ]

        urls: Set[str] = set()

        # Check robots.txt for Sitemap: directive
        try:
            resp = self.session.get(sitemap_locs[0], timeout=10)
            if resp.status_code == 200:
                matches = re.findall(
                    r"^Sitemap:\s*(.*)$", resp.text, re.IGNORECASE | re.MULTILINE
                )
                for m in matches:
                    sitemap_locs.append(m.strip())
        except Exception:
            pass

        for loc in set(sitemap_locs):
            if loc.endswith(".txt") and "robots.txt" not in loc:
                continue  # Skip robots.txt if we already processed it

            try:
                resp = self.session.get(loc, timeout=15)
                if resp.status_code == 200:
                    ct = resp.headers.get("Content-Type", "").lower()
                    if "xml" in ct or resp.text.strip().startswith("<?xml"):
                        urls.update(self._parse_xml_sitemap(resp.content, loc))
                    elif "text/plain" in ct:
                        new_urls = {
                            l.strip()
                            for l in resp.text.split("\n")
                            if l.strip().startswith("http")
                        }
                        urls.update(new_urls)
            except Exception as e:
                logger.debug(f"Failed to fetch sitemap {loc}: {e}")

        return urls

    def _parse_xml_sitemap(
        self, content: bytes, original_url: str, depth: int = 0
    ) -> Set[str]:
        """Recursively parse XML sitemaps and sitemap indexes."""
        if depth > 3:  # Guard against infinite recursion
            return set()

        urls: Set[str] = set()
        try:
            root = ET.fromstring(content)
            # Handle potential namespaces
            ns = ""
            if root.tag.startswith("{"):
                ns = root.tag.split("}")[0] + "}"

            # If it's a sitemap index, recurse
            for sitemap in root.findall(f".//{ns}sitemap"):
                loc = sitemap.find(f"{ns}loc")
                if loc is not None and loc.text:
                    try:
                        resp = self.session.get(loc.text.strip(), timeout=10)
                        if resp.status_code == 200:
                            urls.update(
                                self._parse_xml_sitemap(
                                    resp.content, loc.text.strip(), depth + 1
                                )
                            )
                    except Exception:
                        pass

            # Extract URLs
            for url_tag in root.findall(f".//{ns}url"):
                loc = url_tag.find(f"{ns}loc")
                if loc is not None and loc.text:
                    urls.add(loc.text.strip())
        except Exception as e:
            logger.debug(f"XML parsing error in {original_url}: {e}")
        return urls

    # ── Feed discovery ──────────────────────────────────────────────────

    def discover_from_feeds(self) -> Set[str]:
        """Look for RSS/Atom feeds in the homepage <head> and parse them."""
        urls: Set[str] = set()
        soup = self.fetch_page(self.base_url)
        if not soup:
            return urls

        feed_links = soup.find_all(
            "link", type=["application/rss+xml", "application/atom+xml"]
        )

        # Also try common feed paths
        common_feeds = ["/feed", "/feed/", "/rss", "/rss.xml", "/atom.xml"]
        for path in common_feeds:
            feed_url = urljoin(self.base_url, path)
            try:
                resp = self.session.get(feed_url, timeout=10)
                if resp.status_code == 200 and ("xml" in resp.headers.get("Content-Type", "").lower()):
                    links = re.findall(r"<link>(.*?)<\/link>", resp.text)
                    for l in links:
                        l = l.split("]]>")[-1].strip()
                        if l.startswith("http"):
                            urls.add(l)
            except Exception:
                pass

        for link in feed_links:
            feed_url = link.get("href")
            if not feed_url:
                continue
            feed_url = urljoin(self.base_url, feed_url)

            try:
                resp = self.session.get(feed_url, timeout=10)
                if resp.status_code == 200:
                    links = re.findall(r"<link>(.*?)<\/link>", resp.text)
                    for l in links:
                        l = l.split("]]>")[-1].strip()
                        if l.startswith("http"):
                            urls.add(l)
            except Exception:
                pass
        return urls

    # ── Navigation / section discovery ──────────────────────────────────

    def discover_from_navigation(self) -> Set[str]:
        """Extract section/category links from homepage nav elements."""
        urls: Set[str] = set()
        soup = self.fetch_page(self.base_url)
        if not soup:
            return urls

        # Look for links in <nav>, <header>, and common menu containers
        nav_containers = soup.find_all(["nav"]) + soup.select(
            ".menu, .nav, .navigation, #menu, #nav, .main-menu, "
            ".primary-menu, .header-menu, .navbar"
        )

        for container in nav_containers:
            for a in container.find_all("a", href=True):
                href = urljoin(self.base_url, a["href"]).split("#")[0].rstrip("/")
                if self._is_internal(href) and href != self.base_url.rstrip("/"):
                    urls.add(href)

        # Also grab section links from the homepage body (listing pages)
        for a in (soup.find_all("a", href=True) if not nav_containers else []):
            href = urljoin(self.base_url, a["href"]).split("#")[0].rstrip("/")
            path = urlparse(href).path
            # Section pages tend to have short paths like /news/, /sports/
            parts = [p for p in path.split("/") if p]
            if (
                self._is_internal(href)
                and 1 <= len(parts) <= 2
                and not any(
                    p in path
                    for p in ["/wp-", "/assets/", "/css/", "/js/", "/images/"]
                )
            ):
                urls.add(href)

        return urls

    # ── Pagination discovery ────────────────────────────────────────────

    def discover_from_pagination(
        self, listing_urls: Set[str], max_pages: int = 20
    ) -> Set[str]:
        """Follow pagination patterns on listing/category pages."""
        urls: Set[str] = set()

        # Common pagination patterns
        pagination_patterns = [
            "?page={n}",
            "?p={n}",
            "/page/{n}/",
            "/page/{n}",
            "?paged={n}",
        ]

        for listing_url in listing_urls:
            for pattern in pagination_patterns:
                found_any = False
                for page_num in range(2, max_pages + 2):
                    page_url = listing_url.rstrip("/") + pattern.format(n=page_num)

                    try:
                        resp = self.session.get(page_url, timeout=15)
                        if resp.status_code != 200:
                            break  # No more pages

                        soup = self.fetch_page(page_url)
                        if not soup:
                            break

                        # Extract article links from this page
                        page_found = 0
                        for a in soup.find_all("a", href=True):
                            full = (
                                urljoin(page_url, a["href"]).split("#")[0].rstrip("/")
                            )
                            if self._is_internal(full) and self._is_potential_article(
                                full
                            ):
                                urls.add(full)
                                page_found += 1

                        if page_found == 0:
                            break  # Empty page, stop pagination

                        found_any = True
                    except Exception:
                        break

                if found_any:
                    break  # Found a working pattern, no need to try others

        return urls

    # ── Archive discovery ───────────────────────────────────────────────

    def discover_from_archives(self, months_back: int = 24) -> Set[str]:
        """Generate and probe date-based archive URLs."""
        urls: Set[str] = set()
        now = datetime.datetime.now()

        for month_offset in range(months_back):
            dt = now - datetime.timedelta(days=month_offset * 30)
            year = dt.year
            month = dt.month

            # Common archive URL patterns
            archive_paths = [
                f"/{year}/{month:02d}/",
                f"/{year}/{month:02d}",
                f"/archives/{year}/{month:02d}/",
                f"/archive/{year}/{month:02d}/",
            ]

            for path in archive_paths:
                archive_url = urljoin(self.base_url, path)
                try:
                    resp = self.session.get(archive_url, timeout=10)
                    if resp.status_code != 200:
                        continue

                    soup = self.fetch_page(archive_url)
                    if not soup:
                        continue

                    # Extract article links
                    for a in soup.find_all("a", href=True):
                        full = (
                            urljoin(archive_url, a["href"])
                            .split("#")[0]
                            .rstrip("/")
                        )
                        if self._is_internal(full) and self._is_potential_article(full):
                            urls.add(full)
                except Exception:
                    continue

        return urls

    # ── Pattern-based discovery ─────────────────────────────────────────

    def discover_common_patterns(self) -> Set[str]:
        """Try common Nepali news/govt URL patterns and date-based structures."""
        common_segments = [
            "news",
            "notices",
            "press-release",
            "circulars",
            "archives",
            "latest",
            "category",
            "category/news",
            "category/politics",
            "category/sports",
            "category/entertainment",
            "category/health",
            "category/lifestyle",
            "category/interview",
            "category/economy",
            "category/world",
            "nation",
            "province",
            "main-news",
            "top-stories",
            "trending",
            "samachar",
            "bichar",
            "rajniti",
            "artha",
            "khelkud",
            "manoranjan",
            "prabash",
            "crime",
            "shikshya",
            "swasthya",
            "pravidi",
            "kala",
            "shitya",
            "news-details",
            "news-article",
            "detail",
            "node",
            "content",
            # Additional common Nepali segments
            "desh",
            "pradesh",
            "duniya",
            "bazar",
            "sahitya",
            "prabidhi",
            "photo-feature",
            "blog",
            "opinion",
            "editorial",
            "feature",
            "special",
        ]

        patterns = [f"/{s}" for s in common_segments]

        # Add dynamic date patterns (last 2 years)
        now = datetime.datetime.now()
        years = [now.year, now.year - 1]
        for y in years:
            for m in range(1, 13):
                patterns.append(f"/{y}/{m:02d}")

        urls: Set[str] = set()
        for p in patterns:
            urls.add(urljoin(self.base_url, p))
        return urls

    # ── BFS crawl ───────────────────────────────────────────────────────

    def crawl_internal_links(self, max_pages: int = 200) -> Set[str]:
        """BFS crawl of internal links to discover content.

        Prioritizes shorter URLs (likely listing pages) to find
        more article links per crawled page.
        """
        to_visit = [self.base_url]
        discovered: Set[str] = set()
        visited: Set[str] = set()

        while to_visit and len(visited) < max_pages:
            # Sort by URL length — shorter = more likely a listing page
            to_visit.sort(key=len)
            url = to_visit.pop(0)
            if url in visited:
                continue
            visited.add(url)

            soup = self.fetch_page(url)
            if not soup:
                continue

            for link in soup.find_all("a", href=True):
                full_url = urljoin(url, link["href"]).split("#")[0].rstrip("/")
                if self._is_internal(full_url) and full_url not in visited:
                    if self._is_potential_article(full_url):
                        discovered.add(full_url)

                    if full_url not in visited and full_url not in to_visit:
                        to_visit.append(full_url)

        return discovered

    # ── Helpers ──────────────────────────────────────────────────────────

    def _is_internal(self, url: str) -> bool:
        """Check if URL belongs to the base domain."""
        base_netloc = urlparse(self.base_url).netloc.lower()
        url_netloc = urlparse(url).netloc.lower()
        return base_netloc == url_netloc or url_netloc.endswith("." + base_netloc)

    def _is_potential_article(self, url: str) -> bool:
        """Heuristic to identify article URLs vs navigation links."""
        path = urlparse(url).path.lower()
        if not path or path == "/":
            return False

        # Ignore common technical paths
        blacklist = [
            "/category/",
            "/tag/",
            "/author/",
            "/page/",
            "/search/",
            "/wp-content/",
            "/includes/",
            "/assets/",
            "/css/",
            "/js/",
            "/images/",
            "/uploads/",
            "/wp-admin/",
            "/wp-login",
            "/feed/",
            "/feed",
            "/comments/",
            "/trackback/",
        ]
        if any(b in path for b in blacklist):
            return False

        # Article URLs often have IDs or long descriptive slugs
        parts = [p for p in path.split("/") if p]
        if not parts:
            return False

        last_part = parts[-1]
        # Has an ID (digits) or is a long slug
        if re.search(r"\d{4,}", last_part) or len(last_part) > 20:
            return True

        # Many Nepali news sites use /news/ID or /content/ID
        if len(parts) >= 2 and parts[-2] in [
            "news",
            "content",
            "post",
            "article",
            "detail",
            "story",
        ]:
            return True

        # URL has ≥3 path segments (common for articles: /category/date/slug)
        if len(parts) >= 3:
            return True

        return False

    def _is_listing_page(self, url: str) -> bool:
        """Heuristic: listing pages have short paths and known segment names."""
        path = urlparse(url).path.lower().strip("/")
        parts = [p for p in path.split("/") if p]

        if not parts:
            return False

        # 1-2 path segments like /news, /category/sports
        if len(parts) <= 2:
            listing_segments = {
                "news",
                "category",
                "tag",
                "archive",
                "archives",
                "latest",
                "trending",
                "top-stories",
                "main-news",
                "notices",
                "samachar",
                "nation",
                "province",
                "opinion",
                "sports",
                "politics",
                "entertainment",
                "health",
                "economy",
                "world",
                "rajniti",
                "khelkud",
                "manoranjan",
                "artha",
            }
            return any(p in listing_segments for p in parts)

        return False
