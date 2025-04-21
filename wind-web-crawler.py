#!/usr/bin/env python3
"""
wind-web-crawler.py – quantum‑enabled CSV crawler (timeout‑safe)
(C)Tsubasa Kato - Inspire Search Corp. - 2025/4/21 
Created with help of ChatGPT o3
Our company website: https://www.inspiresearch.io/en

Web Crawling Strategies:
  brownian | levy | ballistic | wind | dla | quantum

CSV columns (always quoted):
  "URL","keywords","description","title"
"""

import argparse, csv, hashlib, math, random, sys, time, cmath, requests
from collections import defaultdict
from pathlib import Path
from urllib.parse import urldefrag, urljoin, urlparse
from urllib.robotparser import RobotFileParser
from bs4 import BeautifulSoup
from tqdm import tqdm

# ─── HTTP & politeness ───────────────────────────────────────────────
UA = ("PhysicsCrawler/2.2 (+https://github.com/yourrepo; "
      "polite crawler; email you@example.com)")

CONNECT_TIMEOUT = 2      # DNS + TCP handshake seconds
READ_TIMEOUT    = 3     # socket idle seconds
THROTTLE        = 1.0    # gap between requests to same host
MAX_BYTES_PER_HOST = 1_000_000

# ─── URL helpers ─────────────────────────────────────────────────────
def canon(url: str) -> str:
    url = urldefrag(url)[0]
    p = urlparse(url)
    return p._replace(scheme=p.scheme.lower(),
                      netloc=p.netloc.lower()).geturl()

def within_scope(url): return urlparse(url).scheme in {"http", "https"}

# ─── HTTP fetch with robots/timeout/throttle ────────────────────────
def polite_get(sess, url, host_last, host_bytes, robots):
    host = urlparse(url).netloc

    if host not in robots:                                 # robots.txt
        rp = RobotFileParser()
        try:
            rp.set_url(f"{urlparse(url).scheme}://{host}/robots.txt")
            rp.read()
        except Exception:
            rp.disallow_all = False
        robots[host] = rp
    if not robots[host].can_fetch(UA, url):
        raise PermissionError("robots.txt")

    wait = THROTTLE - (time.time() - host_last[host])
    if wait > 0:
        time.sleep(wait)

    r = sess.get(url,
                 timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
                 headers={"User-Agent": UA})
    r.raise_for_status()
    host_last[host] = time.time()

    host_bytes[host] += len(r.content)
    if host_bytes[host] > MAX_BYTES_PER_HOST:
        raise MemoryError("byte cap")

    return r.text, r.headers.get("content-type", "")

# ─── HTML scraping ──────────────────────────────────────────────────
def extract_links(html, base):
    soup = BeautifulSoup(html, "html.parser")   # BS4 docs
    return {canon(urljoin(base, a["href"]))
            for a in soup.find_all("a", href=True)
            if within_scope(urljoin(base, a["href"]))}

def meta_tag(soup, names):
    for n in names:
        t = soup.find("meta", attrs={"name": n}) or soup.find("meta", attrs={"property": n})
        if t and t.get("content"):
            return t["content"].strip()
    return ""

def scrape_meta(html):
    soup = BeautifulSoup(html, "html.parser")
    return (meta_tag(soup, ["keywords", "og:keywords"]),
            meta_tag(soup, ["description", "og:description"]),
            soup.title.string.strip() if soup.title else "")

# ─── Tiny quantum simulator (Hadamard & measurement) ───────────────
H = [[1 / math.sqrt(2),  1 / math.sqrt(2)],
     [1 / math.sqrt(2), -1 / math.sqrt(2)]]

def kron(a, b):
    return [[ai * bj for bj in row_b for ai in row_a]
            for row_a in a for row_b in b]

def nqubit_gate(single, target, n):
    g = single
    for q in range(n - 1, -1, -1):
        g = kron(g, [[1, 0], [0, 1]]) if q != target else kron(g, [[1]])
    return g

def apply(g, state):
    return [sum(g[i][j] * state[j] for j in range(len(state)))
            for i in range(len(state))]

def normalise(vec):
    norm = math.sqrt(sum(abs(a)**2 for a in vec))
    return [a / norm for a in vec] if norm else vec

def measure(state):
    r, acc = random.random(), 0.0
    for i, amp in enumerate(state):
        acc += abs(amp)**2
        if r <= acc:
            return i
    return len(state) - 1

def quantum_choice(items):
    n = len(items)
    q = math.ceil(math.log2(max(2, n)))
    dim = 2 ** q

    state = [0j] * dim; state[0] = 1
    for qubit in range(q):
        state = apply(nqubit_gate(H, qubit, q), state)

    idx = measure(state)
    while idx >= n:                       # discard padding states
        idx = measure(state)
    return items[idx]

# ─── Frontier with six traversal modes ──────────────────────────────
def hash_angle(url):
    h = int(hashlib.md5(urlparse(url).netloc.encode()).hexdigest(), 16)
    return (h % 360_000) / 360_000 * 2 * math.pi

def levy_len(r, alpha=1.6): return max(1, int(r.paretovariate(alpha)))

class Frontier:
    def __init__(self, strategy, start, rnd=None):
        self.strategy = strategy
        self.rnd = rnd or random.Random()
        self.queue = [start]
        self.wind = hash_angle(start)
        self.momentum = None
        self.cluster = {start}

    def push_many(self, links):
        for u in links:
            if self.strategy == "dla" and u in self.cluster: continue
            self.queue.append(u)

    def pop(self):
        if not self.queue: raise IndexError
        strat = self.strategy

        if strat == "brownian":
            idx = self.rnd.randrange(len(self.queue))
        elif strat == "levy":
            idx = self.rnd.randrange(min(levy_len(self.rnd), len(self.queue)))
        elif strat == "ballistic":
            if self.momentum and self.rnd.random() < 0.85:
                idx = next((i for i, u in enumerate(self.queue)
                            if urlparse(u).netloc == self.momentum), 0)
            else:
                idx = 0
            self.momentum = urlparse(self.queue[idx]).netloc
        elif strat == "wind":
            wt = [1 + 2 * (1 + math.cos(hash_angle(u) - self.wind)) / 2
                  for u in self.queue]
            s = self.rnd.random() * sum(wt); acc = 0
            for i, w in enumerate(wt):
                acc += w
                if s <= acc: idx = i; break
        elif strat == "dla":
            idx = 0
        elif strat == "quantum":
            chosen = quantum_choice(self.queue)
            idx = self.queue.index(chosen)
        else:
            idx = 0
        return self.queue.pop(idx)

# ─── Main crawl loop ───────────────────────────────────────────────
def crawl(seeds, strategy, max_pages, writer):
    sess = requests.Session()
    frontier = Frontier(strategy, seeds[0]); frontier.push_many(seeds[1:])

    visited, host_last, host_bytes, robots = set(), defaultdict(float), defaultdict(int), {}
    bar = tqdm(total=max_pages, desc=f"{strategy} crawl")

    while frontier.queue and len(visited) < max_pages:
        url = frontier.pop()
        if url in visited: continue
        try:
            html, ctype = polite_get(sess, url, host_last, host_bytes, robots)
            visited.add(url); bar.update(1)
            if "text/html" in ctype:
                kw, desc, title = scrape_meta(html)
                writer.writerow([url, kw, desc, title])
                frontier.push_many(extract_links(html, url) - visited)
                if strategy == "dla": frontier.cluster.add(url)
        except (PermissionError, MemoryError, requests.exceptions.Timeout):
            continue
        except Exception:
            continue
    bar.close()

# ─── CLI ────────────────────────────────────────────────────────────
def main():
    p = argparse.ArgumentParser()
    p.add_argument("--seeds", default="seeds.txt")
    p.add_argument("--strategy", choices=["brownian","levy","ballistic","wind","dla","quantum"],
                   default="quantum")
    p.add_argument("--max-pages", type=int, default=100)
    p.add_argument("--csv-out", default="crawl_output.csv")
    args = p.parse_args()

    seeds = [canon(u.strip()) for u in Path(args.seeds).read_text().splitlines() if u.strip()]
    if not seeds: print("No seeds."); sys.exit(1)

    with open(args.csv_out, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(["URL","keywords","description","title"])
        crawl(seeds, args.strategy, args.max_pages, writer)
    print("CSV saved to", args.csv_out)

if __name__ == "__main__":
    main()
