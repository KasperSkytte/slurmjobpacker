"""The cluster the analysis tools describe, read from a TOML site file.

The file is --site PATH on the command line, else $SQP_SITE, else ./site.toml.
tools/site.example.toml documents the format; copy it to site.toml (kept out of
git) and describe your own partitions. Importing this module loads the file,
so every tool, and every worker process a tool starts, sees the same cluster.
"""
import os, re, sys, time, tomllib


def expand(nl):
    """'node12' / 'node[03-05,07]' / comma lists -> [names]"""
    out = []
    for tok in re.findall(r'[^,\[]+(?:\[[^\]]*\])?', nl or ''):
        tok = tok.strip()
        if not tok:
            continue
        m = re.match(r'^(.*?)\[([0-9,\-]+)\]$', tok)
        if not m:
            out.append(tok)
            continue
        pre, body = m.groups()
        for seg in body.split(','):
            if '-' in seg:
                a, b = seg.split('-')
                out += [f'{pre}{i:0{len(a)}d}' for i in range(int(a), int(b) + 1)]
            else:
                out.append(f'{pre}{seg}')
    return out


def _path():
    if '--site' in sys.argv:
        i = sys.argv.index('--site')
        os.environ['SQP_SITE'] = os.path.abspath(sys.argv[i + 1])   # for workers
        del sys.argv[i:i + 2]
    return os.environ.get('SQP_SITE', 'site.toml')


class Site:
    def __init__(self, path):
        if not os.path.exists(path):
            sys.exit(f'{path}: no site file. Copy tools/site.example.toml to site.toml '
                     'and describe your cluster, or pass --site PATH.')
        with open(path, 'rb') as f:
            c = tomllib.load(f)
        parts = c['partitions']
        self.name = c.get('name', 'cluster')
        since = c.get('since', 0)            # a date, or epoch seconds
        self.t0 = since if isinstance(since, int) else \
            int(time.mktime(time.strptime(since, '%Y-%m-%d')))
        self.ratio_threshold = c.get('ratio_threshold', 6000)
        q = c.get('qos', {})
        self.max_cpu_per_user = q.get('max_cpu_per_user', 10 ** 9)
        self.max_cpu_per_account = q.get('max_cpu_per_account', 10 ** 9)
        # every listed partition, for cluster-wide totals
        self.part_nodes = {p: expand(v['nodes']) for p, v in parts.items()}
        self.node_part = {n: p for p, ns in self.part_nodes.items() for n in ns}
        # the slim and fat ones, which jobs are placed on, in PriorityTier order
        placed = [p for p, v in parts.items() if v.get('class') in ('slim', 'fat')]
        self.tier = {p: parts[p].get('tier', 1) for p in placed}
        self.speed = {p: parts[p].get('speed', 1.0) for p in placed}
        order = sorted(placed, key=lambda p: -self.tier[p])
        self.slim = tuple(p for p in order if parts[p]['class'] == 'slim')
        self.fat = tuple(p for p in order if parts[p]['class'] == 'fat')
        self.batch_node_part = {n: p for n, p in self.node_part.items() if p in self.tier}
        # the demand mix, as `python3 -m sqp.fit` prints it; else sqp's defaults
        self.demand = [tuple(x) for x in c.get('demand', [
            [800, .10], [1280, .15], [4267, .25], [7680, .25], [14178, .15], [30720, .10]])]
        self.demand_median = c.get('demand_median', 4267)


SITE = Site(_path())
