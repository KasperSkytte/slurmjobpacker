#!/usr/bin/env python3
"""Record the 3D view of a simulated cluster as a video, frame by frame.

The simulator and the page are stepped together, one frame at a time, so the
video is smooth however slowly the browser renders (headless browsers render
WebGL in software). Needs Playwright with Chromium, and imageio-ffmpeg:

    python3 -m venv ~/sjp-rec && ~/sjp-rec/bin/pip install playwright imageio-ffmpeg
    ~/sjp-rec/bin/python -m playwright install chromium
    python3 -m sjp.viz --demo --port 8651 &          # serves the page
    ~/sjp-rec/bin/python tools/record_demo.py demo.mp4

Options: --view 3d|2d --seconds 10 --fps 30 --size 1920x1080 (1080x1920 for phones)
         --slim 3 --fat 3 (nodes in the demo cluster) --pinned 0.5 (jobs waiting
         pinned to a node) --ui 1.0 (panel size)
         --sim 60 (simulated seconds per frame) --orbit 0.3 (radians of camera turn)
         --load 2.0 --seed 5 --title ... --subtitle ...
"""
import argparse, asyncio, os, subprocess, sys, tempfile, urllib.parse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sjp import config, viz                                      # noqa: E402


async def record(a):
    from playwright.async_api import async_playwright
    import imageio_ffmpeg
    w, h = (int(x) for x in a.size.split("x"))
    demo = viz.Demo(config.defaults(), speed=a.sim * a.fps, seed=a.seed, load=a.load,
                    slim=a.slim, fat=a.fat, pinned=a.pinned)
    for _ in range(a.warmup):                                    # fill the cluster first
        demo.step(0.1)
    query = urllib.parse.urlencode({k: v for k, v in dict(
        manual=1, clean=1, labels=1, title=a.title, subtitle=a.subtitle, ui=a.ui).items()
        if v is not None})
    frames = tempfile.mkdtemp(prefix="sjp-frames-")
    async with async_playwright() as p:
        # 3D needs WebGL, which a headless browser renders in software; 2D does not.
        browser = await p.chromium.launch(args=[] if a.view == "2d" else [
            "--use-gl=angle", "--use-angle=swiftshader", "--enable-unsafe-swiftshader"])
        page = await browser.new_page(viewport={"width": w, "height": h})
        await page.goto(f"{a.url.rstrip('/')}/{'2d' if a.view == '2d' else ''}?{query}")
        await page.wait_for_function("window.sjpViz !== undefined")
        await page.evaluate("s => window.sjpViz.apply(s)", demo.get())
        for _ in range(60):                                      # let the first frame settle
            await page.evaluate("() => window.sjpViz.frame(1/30)")
        n = int(a.seconds * a.fps)
        for i in range(n):
            demo.step(1 / a.fps)
            await page.evaluate("s => window.sjpViz.apply(s)", demo.get())
            await page.evaluate(f"() => window.sjpViz.frame({1 / a.fps}, {a.orbit / n})")
            await page.screenshot(path=f"{frames}/f{i:05d}.png")
            if i % a.fps == 0:
                print(f"{i}/{n} frames", flush=True)
        await browser.close()
    subprocess.run([imageio_ffmpeg.get_ffmpeg_exe(), "-y", "-loglevel", "error",
                    "-framerate", str(a.fps), "-i", f"{frames}/f%05d.png", "-c:v", "libx264",
                    "-pix_fmt", "yuv420p", "-crf", "18", "-movflags", "+faststart", a.out],
                   check=True)
    print("wrote", a.out)


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("out")
    ap.add_argument("--url", default="http://localhost:8651/")
    ap.add_argument("--seconds", type=float, default=10)
    ap.add_argument("--fps", type=int, default=30)
    ap.add_argument("--size", default="1920x1080")
    ap.add_argument("--view", choices=("3d", "2d"), default="3d")
    ap.add_argument("--sim", type=float, default=60, help="simulated seconds per frame")
    ap.add_argument("--orbit", type=float, default=0.3, help="camera turn over the video, radians")
    ap.add_argument("--load", type=float, default=2.0)
    ap.add_argument("--slim", type=int, default=3, help="slim nodes in the demo cluster")
    ap.add_argument("--fat", type=int, default=3, help="fat nodes in the demo cluster")
    ap.add_argument("--pinned", type=float, default=0.5,
                    help="share of jobs that wait pinned to a node (wait_for_room)")
    ap.add_argument("--ui", type=float, default=1.0, help="size of the text panels, e.g. 1.6 for phones")
    ap.add_argument("--seed", type=int, default=5)
    ap.add_argument("--warmup", type=int, default=800, help="simulator steps before recording")
    ap.add_argument("--title", default="slurmjobpacker")
    ap.add_argument("--subtitle", help="default: the page's own")
    asyncio.run(record(ap.parse_args()))


if __name__ == "__main__":
    main()
