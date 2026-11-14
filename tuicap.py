#!/usr/bin/env python3
"""Run ytraceroute TUI in a pty, force a full repaint via resize, dump text."""
import fcntl
import os
import pty
import re
import select
import struct
import sys
import termios
import time

DEST = sys.argv[1] if len(sys.argv) > 1 else "308:25:40:bd::"
WAIT = float(sys.argv[2]) if len(sys.argv) > 2 else 35.0

pid, fd = pty.fork()
if pid == 0:
    os.execv(
        "/root/yggtest/ytraceroute",
        ["ytraceroute", "-e", "tcp://127.0.0.1:9001", "--deadline", "30", DEST],
    )
fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", 40, 130, 0, 0))

out = b""
end = time.time() + WAIT
while time.time() < end:
    r, _, _ = select.select([fd], [], [], 1)
    if r:
        try:
            d = os.read(fd, 65536)
        except OSError:
            break
        if not d:
            break
        out += d

# Force a complete repaint by resizing; grab everything after it.
fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", 44, 130, 0, 0))
time.sleep(1.5)
try:
    while True:
        r, _, _ = select.select([fd], [], [], 0.4)
        if not r:
            break
        d = os.read(fd, 65536)
        if not d:
            break
        out += d
except OSError:
    pass
os.write(fd, b"q")
time.sleep(0.5)
try:
    os.kill(pid, 9)
except OSError:
    pass

# Keep only the post-resize tail, strip escapes, print non-empty lines.
txt = re.sub(
    r"\x1b\[[0-9;?]*[a-zA-Z]|\x1b[()][0-9A-B]|\x1b[=>]|\r",
    "",
    out[-120000:].decode("utf-8", "replace"),
)
lines = [l.rstrip() for l in txt.split("\n") if l.strip()]
print("\n".join(lines[-36:]))
