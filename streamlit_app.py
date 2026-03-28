import streamlit as st
import pandas as pd
from collections import defaultdict
from io import BytesIO
import hashlib
import urllib.parse

from streamlit_vis_network import streamlit_vis_network

#To run locally: python -m streamlit run .\streamlit_app.py

# ============================================================
# Noise-level helpers
# ============================================================

def infer_noise_from_name(run_name: str) -> set[int]:
    name = str(run_name or "")
    if "T6" in name:
        return {1}
    if "T4" in name:
        return {2}
    if "T3" in name:
        return {3}
    return set()

def parse_noise_levels(val, run_name=None):
    if pd.isna(val) or str(val).strip() == "":
        return infer_noise_from_name(run_name)

    if isinstance(val, (int, float)) and not pd.isna(val):
        return {int(val)}

    s = str(val).strip()
    parts = [p.strip() for p in s.split(",")]
    out = set()
    for p in parts:
        if p:
            try:
                out.add(int(float(p)))
            except ValueError:
                pass
    return out

# ============================================================
# Excel I/O helpers
# ============================================================

REQUIRED_SHEETS = ["Tray", "Connections"]

def load_excel_to_dfs(file_bytes: bytes) -> tuple[dict, bool]:
    """
    Load Excel file into dataframes.
    Returns: (dfs dict, is_older_template bool)
    """
    xls = pd.ExcelFile(BytesIO(file_bytes))
    dfs = {}

    for sh in REQUIRED_SHEETS:
        if sh not in xls.sheet_names:
            raise ValueError(f"Missing required sheet: {sh}")
        converters = None
        if sh == "Tray":
            converters = {"RunName": str, "Noise Level": str}
        elif sh == "Connections":
            converters = {"From": str, "To": str, "Exposed?": str}

        dfs[sh] = pd.read_excel(xls, sh, converters=converters)

    if "Tray" in dfs:
        dfs["Tray"] = ensure_person_columns(ensure_xy_columns(dfs["Tray"]))

    if "Connections" in dfs:
        if "Exposed?" not in dfs["Connections"].columns and "Exposed Conduit Route?" not in dfs["Connections"].columns:
            dfs["Connections"]["Exposed?"] = ""
        elif "Exposed Conduit Route?" in dfs["Connections"].columns and "Exposed?" not in dfs["Connections"].columns:
            dfs["Connections"].rename(columns={"Exposed Conduit Route?": "Exposed?"}, inplace=True)

    return dfs, False

def write_updated_workbook_bytes(dfs: dict) -> bytes:
    out = BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        for sh, df in dfs.items():
            df.to_excel(writer, sheet_name=sh, index=False)
    out.seek(0)
    return out.getvalue()


def validate_dfs(dfs: dict) -> list[str]:
    errors = []
    expected_cols = {
        "Tray": {"RunName"},
        "Connections": {"From", "To"},
    }

    if "Connections" in dfs:
        if "Exposed?" not in dfs["Connections"].columns and "Exposed Conduit Route?" not in dfs["Connections"].columns:
            dfs["Connections"]["Exposed?"] = ""
        elif "Exposed Conduit Route?" in dfs["Connections"].columns and "Exposed?" not in dfs["Connections"].columns:
            dfs["Connections"].rename(columns={"Exposed Conduit Route?": "Exposed?"}, inplace=True)

    for sh, cols in expected_cols.items():
        missing = cols - set(dfs[sh].columns)
        if missing:
            errors.append(f"{sh}: missing columns {sorted(missing)}")

    try:
        tray_df = dfs.get("Tray")
        con_df = dfs.get("Connections")
        if tray_df is not None and con_df is not None and not tray_df.empty and not con_df.empty:
            tray_names = set(tray_df["RunName"].astype(str).str.strip())
            from_set = set(con_df["From"].astype(str).str.strip())
            to_set = set(con_df["To"].astype(str).str.strip())
            missing_nodes = sorted(n for n in (from_set | to_set) if n and n not in tray_names)
            if missing_nodes:
                errors.append("Connections: missing Tray.RunName entries for " + ", ".join(missing_nodes))
    except Exception:
        pass

    return errors

def build_demo_workbook_bytes() -> bytes:
    tray = pd.DataFrame([
        {"RunName": "Alex", "Noise Level": "1",   "Email": "alex@example.com",   "Phone": "555-0101", "Role": "Manager",   "Notes": "Team lead",         "X": pd.NA, "Y": pd.NA},
        {"RunName": "Blair", "Noise Level": "1",   "Email": "blair@example.com",  "Phone": "555-0102", "Role": "Developer", "Notes": "Frontend focus",    "X": pd.NA, "Y": pd.NA},
        {"RunName": "Casey", "Noise Level": "1",   "Email": "casey@example.com",  "Phone": "555-0103", "Role": "Analyst",   "Notes": "Reports and data",  "X": pd.NA, "Y": pd.NA},
        {"RunName": "Drew",  "Noise Level": "1",   "Email": "drew@example.com",   "Phone": "555-0104", "Role": "Designer",  "Notes": "Visual systems",   "X": pd.NA, "Y": pd.NA},
        {"RunName": "Erin",  "Noise Level": "2",   "Email": "erin@example.com",   "Phone": "555-0105", "Role": "Developer", "Notes": "Backend services", "X": pd.NA, "Y": pd.NA},
        {"RunName": "Flynn", "Noise Level": "2",   "Email": "flynn@example.com",  "Phone": "555-0106", "Role": "Sales",     "Notes": "Client contact",   "X": pd.NA, "Y": pd.NA},
        {"RunName": "Gray",  "Noise Level": "2",   "Email": "gray@example.com",   "Phone": "555-0107", "Role": "HR",        "Notes": "People ops",       "X": pd.NA, "Y": pd.NA},
        {"RunName": "Harper","Noise Level": "2",   "Email": "harper@example.com", "Phone": "555-0108", "Role": "Designer",  "Notes": "Brand work",       "X": pd.NA, "Y": pd.NA},
        {"RunName": "Indy",  "Noise Level": "1,2", "Email": "indy@example.com",   "Phone": "555-0109", "Role": "Manager",   "Notes": "Cross-team owner", "X": pd.NA, "Y": pd.NA},
        {"RunName": "Jules", "Noise Level": "1,2", "Email": "jules@example.com",  "Phone": "555-0110", "Role": "Analyst",   "Notes": "Operations",       "X": pd.NA, "Y": pd.NA},
        {"RunName": "Kai",   "Noise Level": "1",   "Email": "kai@example.com",    "Phone": "555-0111", "Role": "Developer", "Notes": "Automation",       "X": pd.NA, "Y": pd.NA},
        {"RunName": "Lane",  "Noise Level": "2",   "Email": "lane@example.com",   "Phone": "555-0112", "Role": "Other",     "Notes": "Contractor",       "X": pd.NA, "Y": pd.NA},
    ])

    connections = pd.DataFrame([
        {"From": "Alex", "To": "Blair", "Exposed?": ""},
        {"From": "Blair", "To": "Casey", "Exposed?": ""},
        {"From": "Blair", "To": "Kai", "Exposed?": ""},
        {"From": "Casey", "To": "Drew", "Exposed?": ""},
        {"From": "Erin", "To": "Flynn", "Exposed?": ""},
        {"From": "Flynn", "To": "Gray", "Exposed?": ""},
        {"From": "Flynn", "To": "Lane", "Exposed?": ""},
        {"From": "Gray", "To": "Harper", "Exposed?": ""},
        {"From": "Drew", "To": "Indy", "Exposed?": ""},
        {"From": "Harper", "To": "Indy", "Exposed?": ""},
        {"From": "Indy", "To": "Jules", "Exposed?": ""},
        {"From": "Kai", "To": "Indy", "Exposed?": ""},
        {"From": "Lane", "To": "Indy", "Exposed?": ""},
    ])

    out = BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        tray.to_excel(writer, sheet_name="Tray", index=False)
        connections.to_excel(writer, sheet_name="Connections", index=False)
    out.seek(0)
    return out.getvalue()


# ============================================================
# Graph helpers (SVG nodes + seam fix + highlighting)
# ============================================================

ORANGE = "#FFA500"
GREEN  = "#00A651"
YELLOW = "#FFD200"
GRAY   = "#CFCFCF"

EDGE_COLOR = "#000000"
EDGE_WIDTH = 2

NODE_BORDER_COLOR = "#333333"
SVG_BORDER_WIDTH = 2

TRAY_SIDE = 70
TRAY_RADIUS = 14
IMAGE_SIZE = 32

PROBE_ID = "__POS_PROBE__"

PERSON_COLUMNS = ["Email", "Phone", "Role", "Notes"]

HIGHLIGHT_BORDER_WIDTH = 10
HIGHLIGHT_BORDER_COLOR = "#FF00FF"
HIGHLIGHT_GLOW_SIZE = 22

def ensure_xy_columns(tray_df: pd.DataFrame) -> pd.DataFrame:
    tray_df = tray_df.copy()
    if "X" not in tray_df.columns:
        tray_df["X"] = pd.NA
    if "Y" not in tray_df.columns:
        tray_df["Y"] = pd.NA
    return tray_df

def ensure_person_columns(tray_df: pd.DataFrame) -> pd.DataFrame:
    tray_df = tray_df.copy()
    for col in PERSON_COLUMNS:
        if col not in tray_df.columns:
            tray_df[col] = ""
    return tray_df

def tray_has_any_xy(tray_df: pd.DataFrame) -> bool:
    if tray_df is None or tray_df.empty:
        return False
    df = ensure_xy_columns(tray_df)
    xs = pd.to_numeric(df["X"], errors="coerce")
    ys = pd.to_numeric(df["Y"], errors="coerce")
    mask = xs.notna() & ys.notna()
    return bool(mask.any())

def noise_title(run_name: str, noise_val) -> str:
    lv = parse_noise_levels(noise_val, run_name=run_name)
    if not lv:
        return "Noise Levels: N/A"
    return "Noise Levels: " + ",".join(str(x) for x in sorted(lv))

def infer_type_from_name(name: str) -> str:
    s = str(name).upper()
    if "EP" in s:
        return "Manhole"
    if "CND" in s:
        return "Conduit"
    if "LT" in s:
        return "Tray"
    return "Node"

def svg_data_uri(svg: str) -> str:
    return "data:image/svg+xml;utf8," + urllib.parse.quote(svg)

def person_circle_svg(diameter: int, color: str, border: str = NODE_BORDER_COLOR) -> str:
    r = diameter / 2
    cx = diameter / 2
    cy = diameter / 2
    head_r = diameter * 0.16
    shoulder_w = diameter * 0.48
    shoulder_h = diameter * 0.24
    shoulder_x = (diameter - shoulder_w) / 2
    shoulder_y = diameter * 0.54
    return f"""
    <svg xmlns="http://www.w3.org/2000/svg" width="{diameter}" height="{diameter}" viewBox="0 0 {diameter} {diameter}">
      <circle cx="{cx}" cy="{cy}" r="{r - 1}" fill="{color}"/>
      <circle cx="{cx}" cy="{cy}" r="{r - 1}" fill="none" stroke="{border}" stroke-width="{SVG_BORDER_WIDTH}"/>
      <circle cx="{cx}" cy="{diameter * 0.34}" r="{head_r}" fill="rgba(255,255,255,0.92)"/>
      <rect x="{shoulder_x}" y="{shoulder_y}" width="{shoulder_w}" height="{shoulder_h}" rx="{shoulder_h / 2}" fill="rgba(255,255,255,0.92)"/>
    </svg>
    """.strip()

def solid_rounded_square_svg(side: int, r: int, color: str, border: str = NODE_BORDER_COLOR) -> str:
    return f"""
    <svg xmlns="http://www.w3.org/2000/svg" width="{side}" height="{side}" viewBox="0 0 {side} {side}">
      <rect x="0" y="0" width="{side}" height="{side}" rx="{r}" ry="{r}" fill="{color}"/>
      <rect x="1" y="1" width="{side-2}" height="{side-2}" rx="{r}" ry="{r}"
            fill="none" stroke="{border}" stroke-width="{SVG_BORDER_WIDTH}"/>
    </svg>
    """.strip()

def split_rounded_square_svg(side: int, r: int, left_color: str, right_color: str, border: str = NODE_BORDER_COLOR) -> str:
    half = side // 2
    left_w = half + 1
    right_x = half
    right_w = side - half
    return f"""
    <svg xmlns="http://www.w3.org/2000/svg" width="{side}" height="{side}" viewBox="0 0 {side} {side}">
      <defs>
        <clipPath id="clipR">
          <rect x="0" y="0" width="{side}" height="{side}" rx="{r}" ry="{r}" />
        </clipPath>
      </defs>
      <g clip-path="url(#clipR)">
        <rect x="0" y="0" width="{left_w}" height="{side}" fill="{left_color}"/>
        <rect x="{right_x}" y="0" width="{right_w}" height="{side}" fill="{right_color}"/>
      </g>
      <rect x="1" y="1" width="{side-2}" height="{side-2}" rx="{r}" ry="{r}"
            fill="none" stroke="{border}" stroke-width="{SVG_BORDER_WIDTH}"/>
    </svg>
    """.strip()

def solid_circle_svg(d: int, color: str, border: str = NODE_BORDER_COLOR) -> str:
    r = d / 2
    rr = r - 1
    return f"""
    <svg xmlns="http://www.w3.org/2000/svg" width="{d}" height="{d}" viewBox="0 0 {d} {d}">
      <circle cx="{r}" cy="{r}" r="{rr}" fill="{color}"/>
      <circle cx="{r}" cy="{r}" r="{rr}" fill="none" stroke="{border}" stroke-width="{SVG_BORDER_WIDTH}"/>
    </svg>
    """.strip()

def split_circle_svg(d: int, left_color: str, right_color: str, border: str = NODE_BORDER_COLOR) -> str:
    r = d / 2
    rr = r - 1
    left_w = int(r) + 1
    right_x = int(r)
    right_w = d - int(r)
    return f"""
    <svg xmlns="http://www.w3.org/2000/svg" width="{d}" height="{d}" viewBox="0 0 {d} {d}">
      <defs>
        <clipPath id="clipC">
          <circle cx="{r}" cy="{r}" r="{rr}"/>
        </clipPath>
      </defs>
      <g clip-path="url(#clipC)">
        <rect x="0" y="0" width="{left_w}" height="{d}" fill="{left_color}"/>
        <rect x="{right_x}" y="0" width="{right_w}" height="{d}" fill="{right_color}"/>
      </g>
      <circle cx="{r}" cy="{r}" r="{rr}" fill="none" stroke="{border}" stroke-width="{SVG_BORDER_WIDTH}"/>
    </svg>
    """.strip()

def solid_diamond_svg(size: int, color: str, border: str = NODE_BORDER_COLOR) -> str:
    # Manhole cover: circle with lugs
    center = size / 2
    outer_r = size * 0.40
    lug_size = size * 0.25
    gap = 2
    
    return f"""
    <svg xmlns="http://www.w3.org/2000/svg" width="{size}" height="{size}" viewBox="0 0 {size} {size}">
      <defs>
        <filter id="shadow" x="-50%" y="-50%" width="200%" height="200%">
          <feDropShadow dx="1" dy="1" stdDeviation="1" flood-opacity="0.4"/>
        </filter>
      </defs>
      
      <!-- Lugs (square handles) with shadow -->
      <rect x="{center - lug_size/2}" y="{gap}" width="{lug_size}" height="{lug_size}" fill="{color}" stroke="{border}" stroke-width="{SVG_BORDER_WIDTH}" filter="url(#shadow)"/>
      <rect x="{center - lug_size/2}" y="{size - lug_size - gap}" width="{lug_size}" height="{lug_size}" fill="{color}" stroke="{border}" stroke-width="{SVG_BORDER_WIDTH}" filter="url(#shadow)"/>
      <rect x="{gap}" y="{center - lug_size/2}" width="{lug_size}" height="{lug_size}" fill="{color}" stroke="{border}" stroke-width="{SVG_BORDER_WIDTH}" filter="url(#shadow)"/>
      <rect x="{size - lug_size - gap}" y="{center - lug_size/2}" width="{lug_size}" height="{lug_size}" fill="{color}" stroke="{border}" stroke-width="{SVG_BORDER_WIDTH}" filter="url(#shadow)"/>
      
      <!-- Central circle -->
      <circle cx="{center}" cy="{center}" r="{outer_r}" fill="{color}"/>
      <circle cx="{center}" cy="{center}" r="{outer_r}" fill="none" stroke="{border}" stroke-width="{SVG_BORDER_WIDTH}"/>
    </svg>
    """.strip()

def split_diamond_svg(size: int, left_color: str, right_color: str, border: str = NODE_BORDER_COLOR) -> str:
    # Manhole cover: circle with lugs (split colors)
    center = size / 2
    outer_r = size * 0.40
    lug_size = size * 0.25
    gap = 2
    
    return f"""
    <svg xmlns="http://www.w3.org/2000/svg" width="{size}" height="{size}" viewBox="0 0 {size} {size}">
      <defs>
        <filter id="shadowSplit" x="-50%" y="-50%" width="200%" height="200%">
          <feDropShadow dx="1" dy="1" stdDeviation="1" flood-opacity="0.4"/>
        </filter>
        <clipPath id="clipLugTop">
          <rect x="{center - lug_size/2}" y="{gap}" width="{lug_size}" height="{lug_size}"/>
        </clipPath>
        <clipPath id="clipLugBottom">
          <rect x="{center - lug_size/2}" y="{size - lug_size - gap}" width="{lug_size}" height="{lug_size}"/>
        </clipPath>
        <clipPath id="clipLugLeft">
          <rect x="{gap}" y="{center - lug_size/2}" width="{lug_size}" height="{lug_size}"/>
        </clipPath>
        <clipPath id="clipLugRight">
          <rect x="{size - lug_size - gap}" y="{center - lug_size/2}" width="{lug_size}" height="{lug_size}"/>
        </clipPath>
      </defs>
      
      <!-- Lugs with split colors -->
      <g clip-path="url(#clipLugTop)" filter="url(#shadowSplit)">
        <rect x="{center - lug_size/2}" y="{gap}" width="{lug_size/2}" height="{lug_size}" fill="{left_color}" stroke="{border}" stroke-width="{SVG_BORDER_WIDTH}"/>
        <rect x="{center}" y="{gap}" width="{lug_size/2}" height="{lug_size}" fill="{right_color}" stroke="{border}" stroke-width="{SVG_BORDER_WIDTH}"/>
      </g>
      <g clip-path="url(#clipLugBottom)" filter="url(#shadowSplit)">
        <rect x="{center - lug_size/2}" y="{size - lug_size - gap}" width="{lug_size/2}" height="{lug_size}" fill="{left_color}" stroke="{border}" stroke-width="{SVG_BORDER_WIDTH}"/>
        <rect x="{center}" y="{size - lug_size - gap}" width="{lug_size/2}" height="{lug_size}" fill="{right_color}" stroke="{border}" stroke-width="{SVG_BORDER_WIDTH}"/>
      </g>
      <g clip-path="url(#clipLugLeft)" filter="url(#shadowSplit)">
        <rect x="{gap}" y="{center - lug_size/2}" width="{lug_size}" height="{lug_size/2}" fill="{left_color}" stroke="{border}" stroke-width="{SVG_BORDER_WIDTH}"/>
        <rect x="{gap}" y="{center}" width="{lug_size}" height="{lug_size/2}" fill="{right_color}" stroke="{border}" stroke-width="{SVG_BORDER_WIDTH}"/>
      </g>
      <g clip-path="url(#clipLugRight)" filter="url(#shadowSplit)">
        <rect x="{size - lug_size - gap}" y="{center - lug_size/2}" width="{lug_size}" height="{lug_size/2}" fill="{left_color}" stroke="{border}" stroke-width="{SVG_BORDER_WIDTH}"/>
        <rect x="{size - lug_size - gap}" y="{center}" width="{lug_size}" height="{lug_size/2}" fill="{right_color}" stroke="{border}" stroke-width="{SVG_BORDER_WIDTH}"/>
      </g>
      
      <!-- Central circle with split colors -->
      <defs>
        <clipPath id="clipCircleLeft">
          <rect x="0" y="0" width="{center}" height="{size}"/>
        </clipPath>
        <clipPath id="clipCircleRight">
          <rect x="{center}" y="0" width="{center}" height="{size}"/>
        </clipPath>
      </defs>
      <circle cx="{center}" cy="{center}" r="{outer_r}" fill="{left_color}" clip-path="url(#clipCircleLeft)"/>
      <circle cx="{center}" cy="{center}" r="{outer_r}" fill="{right_color}" clip-path="url(#clipCircleRight)"/>
      <circle cx="{center}" cy="{center}" r="{outer_r}" fill="none" stroke="{border}" stroke-width="{SVG_BORDER_WIDTH}"/>
    </svg>
    """.strip()

def noise_color_kind(levels: set[int]) -> str:
    if levels == {1}:
        return "nl1"
    if levels == {2}:
        return "nl2"
    if levels == {3} or levels == {4}:
        return "nl34"
    if (1 in levels) and (2 in levels):
        return "mixed12"
    return "other"

def build_adjacency(connections_df: pd.DataFrame) -> dict[str, set[str]]:
    adj = defaultdict(set)
    if connections_df is None or connections_df.empty:
        return adj
    if "From" not in connections_df.columns or "To" not in connections_df.columns:
        return adj
    for _, r in connections_df.iterrows():
        a = r.get("From", None)
        b = r.get("To", None)
        if pd.isna(a) or pd.isna(b):
            continue
        a = str(a).strip()
        b = str(b).strip()
        if not a or not b:
            continue
        adj[a].add(b)
        adj[b].add(a)
    return adj

def neighborhood_nodes(adj: dict[str, set[str]], start: str, depth: int) -> set[str]:
    start = str(start).strip()
    if not start:
        return set()
    seen = {start}
    frontier = {start}
    for _ in range(max(0, int(depth))):
        nxt = set()
        for u in frontier:
            nxt |= set(adj.get(u, set()))
        nxt -= seen
        seen |= nxt
        frontier = nxt
        if not frontier:
            break
    return seen

def build_vis_nodes_edges(
    tray_df: pd.DataFrame,
    connections_df: pd.DataFrame,
    focus_node: str | None = None,
    focus_depth: int = 2,
    include_probe: bool = False,
    highlight_nodes: set[str] | None = None,
):
    tray_df = ensure_xy_columns(tray_df)
    tray_df = ensure_person_columns(tray_df)
    highlight_nodes = set(highlight_nodes or [])

    focus_set = None
    if focus_node:
        adj = build_adjacency(connections_df)
        focus_set = neighborhood_nodes(adj, focus_node, focus_depth)

    nodes = []
    for _, r in tray_df.iterrows():
        rn = r.get("RunName", None)
        if pd.isna(rn) or str(rn).strip() == "":
            continue
        rn = str(rn).strip()

        if focus_set is not None and rn not in focus_set:
            continue

        is_focus = (focus_node is not None and rn == str(focus_node).strip())
        is_highlight = rn in highlight_nodes

        levels = parse_noise_levels(r.get("Noise Level", None), run_name=rn)
        kind = noise_color_kind(levels)
        email = str(r.get("Email", "") or "").strip()
        phone = str(r.get("Phone", "") or "").strip()
        role = str(r.get("Role", "") or "").strip()
        notes = str(r.get("Notes", "") or "").strip()

        if kind == "nl1":
            svg = person_circle_svg(84, ORANGE, NODE_BORDER_COLOR)
        elif kind == "nl2":
            svg = person_circle_svg(84, GREEN, NODE_BORDER_COLOR)
        elif kind == "nl34":
            svg = person_circle_svg(84, YELLOW, NODE_BORDER_COLOR)
        elif kind == "mixed12":
            svg = person_circle_svg(84, "#72BF44", NODE_BORDER_COLOR)
        else:
            svg = person_circle_svg(84, GRAY, NODE_BORDER_COLOR)

        node = {
            "id": rn,
            "label": rn,
            "title": (
                f"<b>{rn}</b><br/>Person node"
                + (f"<br/>Role: {role}" if role else "")
                + (f"<br/>Email: {email}" if email else "")
                + (f"<br/>Phone: {phone}" if phone else "")
                + (f"<br/>Notes: {notes}" if notes else "")
            ),
            "shape": "image",
            "image": svg_data_uri(svg),
            "size": IMAGE_SIZE,
            "borderWidth": 0,
            "font": {"vadjust": 0},
        }

        if is_highlight:
            node["borderWidth"] = HIGHLIGHT_BORDER_WIDTH
            node["color"] = {"border": HIGHLIGHT_BORDER_COLOR}
            node["shadow"] = {"enabled": True, "size": HIGHLIGHT_GLOW_SIZE, "x": 0, "y": 0}
            node["size"] = IMAGE_SIZE + 18
            node["font"] = {"vadjust": 0, "size": 20}

        if is_focus:
            node["borderWidth"] = max(node.get("borderWidth", 0), 3)
            node["color"] = {"border": "#000000"}

        x = r.get("X", pd.NA)
        y = r.get("Y", pd.NA)
        try:
            x = None if pd.isna(x) else float(x)
        except Exception:
            x = None
        try:
            y = None if pd.isna(y) else float(y)  # Negate Y so increasing Y moves nodes up
        except Exception:
            y = None
        if x is not None and y is not None:
            node["x"] = x
            node["y"] = y

        nodes.append(node)

    node_ids = {n["id"] for n in nodes}

    edges = []
    seen = set()
    if connections_df is not None and not connections_df.empty and "From" in connections_df.columns and "To" in connections_df.columns:
        for _, r in connections_df.iterrows():
            a = r.get("From", None)
            b = r.get("To", None)
            if pd.isna(a) or pd.isna(b):
                continue
            a = str(a).strip()
            b = str(b).strip()
            if not a or not b:
                continue
            if a not in node_ids or b not in node_ids:
                continue
            key = tuple(sorted((a, b)))
            if key in seen:
                continue
            seen.add(key)
            
            edge_data = {
                "id": f"{a}|||{b}",
                "from": a,
                "to": b,
                "title": f"{a} ↔ {b}",
                "color": EDGE_COLOR,
                "width": EDGE_WIDTH,
            }
            
            # Add "EXP" label if exposed
            exposed_val = r.get("Exposed?", "")
            if str(exposed_val).strip().lower() == "yes":
                edge_data["label"] = "EXP"
                edge_data["font"] = {"size": 18, "align": "middle", "color": "red"}
                edge_data["labelHighlightBold"] = True
            
            edges.append(edge_data)

    if include_probe and PROBE_ID not in node_ids:
        nodes.append({
            "id": PROBE_ID,
            "label": "",
            "title": "",
            "shape": "dot",
            "size": 1,
            "x": 0,
            "y": 0,
            "fixed": True,
            "physics": False,
            "color": {"background": "rgba(0,0,0,0)", "border": "rgba(0,0,0,0)"},
            "font": {"size": 0, "color": "rgba(0,0,0,0)"},
            "selected": True,
        })

    return nodes, edges

def apply_positions_to_tray(tray_df: pd.DataFrame, positions: dict) -> pd.DataFrame:
    tray_df = ensure_xy_columns(tray_df)
    if not positions:
        return tray_df

    tray_df = tray_df.copy()
    rn_series = tray_df["RunName"].astype(str).str.strip()

    for nid, xy in positions.items():
        if str(nid).strip() == PROBE_ID:
            continue

        x = y = None
        if isinstance(xy, dict):
            x = xy.get("x")
            y = xy.get("y")
        elif isinstance(xy, (list, tuple)) and len(xy) >= 2:
            x, y = xy[0], xy[1]

        try:
            x = float(x)
            y = float(y)  # Negate Y when saving back
        except Exception:
            continue

        mask = rn_series == str(nid).strip()
        if mask.any():
            tray_df.loc[mask, "X"] = x
            tray_df.loc[mask, "Y"] = y

    return tray_df


def apply_offset_to_nodes(tray_df: pd.DataFrame, node_names: list[str], dx: float, dy: float) -> pd.DataFrame:
    """Offset specified nodes by dx, dy. Creates X/Y columns if missing."""
    tray_df = ensure_xy_columns(tray_df)
    tray_df = tray_df.copy()
    rn_series = tray_df["RunName"].astype(str).str.strip()
    
    for node_name in node_names:
        node_name = str(node_name).strip()
        if not node_name or node_name == PROBE_ID:
            continue
        
        mask = rn_series == node_name
        if mask.any():
            for idx in tray_df[mask].index:
                x_val = tray_df.loc[idx, "X"]
                y_val = tray_df.loc[idx, "Y"]
                
                # Convert to float, treating NaN as 0
                try:
                    x = float(x_val) if pd.notna(x_val) else 0.0
                except Exception:
                    x = 0.0
                try:
                    y = float(y_val) if pd.notna(y_val) else 0.0
                except Exception:
                    y = 0.0
                
                # Apply offset (negate dy so positive values move up)
                tray_df.loc[idx, "X"] = x + dx
                tray_df.loc[idx, "Y"] = y + dy
    
    return tray_df


def apply_scale_to_nodes(tray_df: pd.DataFrame, node_names: list[str], scale_factor: float) -> pd.DataFrame:
    """Scale specified nodes from their center point."""
    if not node_names or scale_factor <= 0:
        return tray_df
    
    tray_df = ensure_xy_columns(tray_df)
    tray_df = tray_df.copy()
    
    # Convert X and Y columns to float64 to avoid dtype warnings
    tray_df["X"] = pd.to_numeric(tray_df["X"], errors="coerce")
    tray_df["Y"] = pd.to_numeric(tray_df["Y"], errors="coerce")
    
    rn_series = tray_df["RunName"].astype(str).str.strip()
    
    # Calculate center point (average of all selected nodes)
    positions = []
    indices = []
    for node_name in node_names:
        node_name = str(node_name).strip()
        if not node_name or node_name == PROBE_ID:
            continue
        
        mask = rn_series == node_name
        if mask.any():
            for idx in tray_df[mask].index:
                x_val = tray_df.loc[idx, "X"]
                y_val = tray_df.loc[idx, "Y"]
                
                try:
                    x = float(x_val) if pd.notna(x_val) else 0.0
                except Exception:
                    x = 0.0
                try:
                    y = float(y_val) if pd.notna(y_val) else 0.0
                except Exception:
                    y = 0.0
                
                positions.append((x, y))
                indices.append(idx)
    
    if not positions:
        return tray_df
    
    # Calculate center
    center_x = sum(p[0] for p in positions) / len(positions)
    center_y = sum(p[1] for p in positions) / len(positions)
    
    # Scale each node from the center
    for i, idx in enumerate(indices):
        x, y = positions[i]
        
        # Calculate position relative to center
        rel_x = x - center_x
        rel_y = y - center_y
        
        # Scale the relative position
        new_rel_x = rel_x * scale_factor
        new_rel_y = rel_y * scale_factor
        
        # Calculate new absolute position
        tray_df.loc[idx, "X"] = center_x + new_rel_x
        tray_df.loc[idx, "Y"] = center_y + new_rel_y
    
    return tray_df


# ============================================================
# UNSAVED LAYOUT REMINDER helpers
# ============================================================

def _tray_xy_map(tray_df: pd.DataFrame) -> dict[str, tuple[float | None, float | None]]:
    df = ensure_xy_columns(tray_df)
    out: dict[str, tuple[float | None, float | None]] = {}
    if df is None or df.empty or "RunName" not in df.columns:
        return out

    for _, r in df.iterrows():
        rn = r.get("RunName", None)
        if pd.isna(rn) or str(rn).strip() == "":
            continue
        rn = str(rn).strip()

        x = r.get("X", pd.NA)
        y = r.get("Y", pd.NA)
        try:
            x = None if pd.isna(x) else float(x)
        except Exception:
            x = None
        try:
            y = None if pd.isna(y) else float(y)
        except Exception:
            y = None
        out[rn] = (x, y)
    return out

def _positions_map(positions: dict) -> dict[str, tuple[float | None, float | None]]:
    out: dict[str, tuple[float | None, float | None]] = {}
    if not isinstance(positions, dict):
        return out
    for nid, xy in positions.items():
        if str(nid).strip() == PROBE_ID:
            continue

        x = y = None
        if isinstance(xy, dict):
            x = xy.get("x")
            y = xy.get("y")
        elif isinstance(xy, (list, tuple)) and len(xy) >= 2:
            x, y = xy[0], xy[1]

        try:
            x = float(x)
            y = float(y)
        except Exception:
            x = y = None

        out[str(nid).strip()] = (x, y)
    return out

def _has_unsaved_layout_changes(tray_df: pd.DataFrame, last_positions: dict | None, tol: float = 0.5) -> bool:
    if not isinstance(last_positions, dict) or not last_positions:
        return False

    tmap = _tray_xy_map(tray_df)
    pmap = _positions_map(last_positions)

    if not pmap:
        return False

    for nid, (px, py) in pmap.items():
        tx, ty = tmap.get(nid, (None, None))
        if px is None or py is None or tx is None or ty is None:
            continue
        if abs(px - tx) > tol or abs(py - ty) > tol:
            return True

    return False


# ============================================================
# DataFrame mutation helpers
# ============================================================

def df_delete_node(tray_df, connections_df, node: str):
    node = str(node).strip()
    tray_df2 = tray_df.copy()
    tray_df2 = tray_df2[tray_df2["RunName"].astype(str).str.strip() != node].reset_index(drop=True)

    con2 = connections_df.copy()
    if "From" in con2.columns and "To" in con2.columns:
        con2 = con2[
            (con2["From"].astype(str).str.strip() != node) &
            (con2["To"].astype(str).str.strip() != node)
        ].reset_index(drop=True)

    return tray_df2, con2

def df_delete_edge(connections_df, a: str, b: str):
    aa = str(a).strip()
    bb = str(b).strip()
    con = connections_df.copy()

    def is_match(r):
        x = str(r.get("From", "")).strip()
        y = str(r.get("To", "")).strip()
        return set([x, y]) == set([aa, bb])

    if ("From" in con.columns) and ("To" in con.columns):
        mask = con.apply(is_match, axis=1)
        con = con[~mask].reset_index(drop=True)
    return con

def df_add_edge(connections_df: pd.DataFrame, a: str, b: str) -> tuple[pd.DataFrame, bool]:
    a = str(a).strip()
    b = str(b).strip()
    if not a or not b or a == b:
        return connections_df, False

    con = connections_df.copy()

    if "From" not in con.columns or "To" not in con.columns:
        return connections_df, False

    def is_dup(r):
        x = str(r.get("From", "")).strip()
        y = str(r.get("To", "")).strip()
        return set([x, y]) == set([a, b])

    if not con.empty:
        try:
            if bool(con.apply(is_dup, axis=1).any()):
                return connections_df, False
        except Exception:
            pass

    new_row = {col: "" for col in con.columns}
    new_row["From"] = a
    new_row["To"] = b
    con = pd.concat([con, pd.DataFrame([new_row])], ignore_index=True)
    return con, True

def get_edge_exposed_status(connections_df: pd.DataFrame, a: str, b: str) -> bool:
    """Get whether an edge is marked as exposed."""
    a = str(a).strip()
    b = str(b).strip()
    
    if "From" not in connections_df.columns or "To" not in connections_df.columns:
        return False
    
    def is_match(r):
        x = str(r.get("From", "")).strip()
        y = str(r.get("To", "")).strip()
        return set([x, y]) == set([a, b])
    
    for _, row in connections_df.iterrows():
        if is_match(row):
            exposed_val = row.get("Exposed?", "")
            return str(exposed_val).strip().lower() == "yes"
    
    return False

def set_edge_exposed_status(connections_df: pd.DataFrame, a: str, b: str, exposed: bool) -> pd.DataFrame:
    """Set whether an edge is marked as exposed."""
    a = str(a).strip()
    b = str(b).strip()
    
    if "From" not in connections_df.columns or "To" not in connections_df.columns:
        return connections_df
    
    con = connections_df.copy()
    
    def is_match(r):
        x = str(r.get("From", "")).strip()
        y = str(r.get("To", "")).strip()
        return set([x, y]) == set([a, b])
    
    for idx, row in con.iterrows():
        if is_match(row):
            con.at[idx, "Exposed?"] = "yes" if exposed else ""
    
    return con

def _compute_default_xy_near_network(tray_df: pd.DataFrame) -> tuple[float | None, float | None]:
    df = ensure_xy_columns(tray_df)
    xs = pd.to_numeric(df["X"], errors="coerce")
    ys = pd.to_numeric(df["Y"], errors="coerce")
    mask = xs.notna() & ys.notna()
    if mask.any():
        cx = float(xs[mask].mean())
        cy = float(ys[mask].mean())
        return cx + 140.0, cy
    return 200.0, 200.0

def df_add_node(tray_df: pd.DataFrame, name: str, x=None, y=None) -> tuple[pd.DataFrame, bool]:
    name = str(name).strip()
    if not name:
        return tray_df, False

    df = ensure_person_columns(ensure_xy_columns(tray_df.copy()))
    
    # Ensure X and Y columns are float64 dtype to avoid dtype warnings
    df["X"] = pd.to_numeric(df["X"], errors="coerce")
    df["Y"] = pd.to_numeric(df["Y"], errors="coerce")

    if "RunName" not in df.columns:
        df["RunName"] = ""
    if (df["RunName"].astype(str).str.strip() == name).any():
        return tray_df, False

    new_row = {col: "" for col in df.columns}
    new_row["RunName"] = name

    if (x is None or str(x).strip() == "") and (y is None or str(y).strip() == ""):
        dx, dy = _compute_default_xy_near_network(df)
        new_row["X"] = dx if dx is not None else None
        new_row["Y"] = dy if dy is not None else None
    else:
        try:
            if x is not None and str(x).strip() != "":
                new_row["X"] = float(x)
        except Exception:
            new_row["X"] = None
        try:
            if y is not None and str(y).strip() != "":
                new_row["Y"] = float(y)
        except Exception:
            new_row["Y"] = None

    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    return df, True

def df_rename_node(tray_df, connections_df, old: str, new: str):
    old = str(old).strip()
    new = str(new).strip()
    if not old or not new or old == new:
        return tray_df, connections_df

    t = tray_df.copy()
    mask = t["RunName"].astype(str).str.strip() == old
    if mask.any():
        t.loc[mask, "RunName"] = new

    c = connections_df.copy()
    if "From" in c.columns:
        c.loc[c["From"].astype(str).str.strip() == old, "From"] = new
    if "To" in c.columns:
        c.loc[c["To"].astype(str).str.strip() == old, "To"] = new

    return t, c

def df_duplicate_node(tray_df, source_name: str, new_name: str):
    source_name = str(source_name).strip()
    new_name = str(new_name).strip()
    if not source_name or not new_name:
        return tray_df
    df = ensure_xy_columns(tray_df.copy())

    if (df["RunName"].astype(str).str.strip() == new_name).any():
        return df

    src_mask = df["RunName"].astype(str).str.strip() == source_name
    if not src_mask.any():
        return df

    src_row = df[src_mask].iloc[0].to_dict()
    src_row["RunName"] = new_name

    try:
        if not pd.isna(src_row.get("X")):
            src_row["X"] = float(src_row["X"]) + 50.0
        if not pd.isna(src_row.get("Y")):
            src_row["Y"] = float(src_row["Y"]) + 50.0
    except Exception:
        pass

    df = pd.concat([df, pd.DataFrame([src_row])], ignore_index=True)
    return df

def df_duplicate_nodes(tray_df: pd.DataFrame, source_names: list[str]) -> tuple[pd.DataFrame, list[str]]:
    """Duplicate multiple nodes by adding _COPY suffix to each name. Returns updated df and list of new names."""
    df = ensure_xy_columns(tray_df.copy())
    new_names = []
    
    for source_name in source_names:
        source_name = str(source_name).strip()
        if not source_name:
            continue
        
        # Generate new name with _COPY suffix
        new_name = f"{source_name}_COPY"
        counter = 1
        while (df["RunName"].astype(str).str.strip() == new_name).any():
            new_name = f"{source_name}_COPY_{counter}"
            counter += 1
        
        src_mask = df["RunName"].astype(str).str.strip() == source_name
        if not src_mask.any():
            continue
        
        src_row = df[src_mask].iloc[0].to_dict()
        src_row["RunName"] = new_name
        
        try:
            if not pd.isna(src_row.get("X")):
                src_row["X"] = float(src_row["X"]) + 50.0
            if not pd.isna(src_row.get("Y")):
                src_row["Y"] = float(src_row["Y"]) + 50.0
        except Exception:
            pass
        
        df = pd.concat([df, pd.DataFrame([src_row])], ignore_index=True)
        new_names.append(new_name)
    
    return df, new_names

# ============================================================
# Selection parsing helper (edge delete)
# ============================================================

def parse_selected_edge(sel_edge) -> tuple[str | None, str | None, str]:
    if sel_edge is None:
        return None, None, "None"

    if isinstance(sel_edge, dict):
        a = str(sel_edge.get("from", "")).strip()
        b = str(sel_edge.get("to", "")).strip()
        if a and b:
            return a, b, f"{a} ↔ {b}"
        sid = sel_edge.get("id")
        if isinstance(sid, str) and "|||" in sid:
            x, y = sid.split("|||", 1)
            return x.strip(), y.strip(), f"{x.strip()} ↔ {y.strip()}"
        return None, None, str(sel_edge)

    if isinstance(sel_edge, (list, tuple)):
        if len(sel_edge) >= 2:
            a = str(sel_edge[0]).strip()
            b = str(sel_edge[1]).strip()
            if a and b:
                return a, b, f"{a} ↔ {b}"
        return None, None, str(sel_edge)

    if isinstance(sel_edge, str):
        s = sel_edge.strip()
        if "|||" in s:
            x, y = s.split("|||", 1)
            return x.strip(), y.strip(), f"{x.strip()} ↔ {y.strip()}"
        return None, None, s

    return None, None, str(sel_edge)


# ============================================================
# Streamlit UI
# ============================================================

st.set_page_config(page_title="Network Graph Editor", layout="wide")

st.markdown(
    "<style>div.block-container {padding-top: 1rem;}</style>",
    unsafe_allow_html=True,
)

st.title("Network Graph Editor")

st.sidebar.header("Workbook")

st.session_state.setdefault("uploader_key_v", 0)
uploader_key = f"uploader_{st.session_state.uploader_key_v}"

source = st.sidebar.radio(
    "Choose workbook source",
    options=["Upload Excel (.xlsx)", "Use demo workbook"],
    index=0,
    key="workbook_source",
)

uploaded = None
demo_clicked = False

if source == "Upload Excel (.xlsx)":
    uploaded = st.sidebar.file_uploader("Upload Excel (.xlsx)", type=["xlsx"], key=uploader_key)
else:
    demo_clicked = st.sidebar.button("Load demo workbook", key="load_demo_btn", width="stretch")
    st.sidebar.caption("Loads a built-in example network for editing and layout testing.")
    demo_bytes_for_dl = build_demo_workbook_bytes()
    st.sidebar.download_button(
        "Download demo workbook (.xlsx)",
        data=demo_bytes_for_dl,
        file_name="demo_network_configuration.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

st.session_state.setdefault("tray_df", None)
st.session_state.setdefault("connections_df", None)
st.session_state.setdefault("upload_hash", None)
st.session_state.setdefault("is_older_template", False)

st.session_state.setdefault("sel_nodes", [])
st.session_state.setdefault("sel_edges", [])

st.session_state.setdefault("focus_node", None)
st.session_state.setdefault("focus_depth", 2)

st.session_state.setdefault("graph_key_v", 0)

st.session_state.setdefault("layout_opt_active", False)
st.session_state.setdefault("layout_opt_last_positions", None)
st.session_state.setdefault("layout_opt_backup_xy", None)



st.session_state.setdefault("initial_layout_autosave_active", False)



st.session_state.setdefault("layout_unsaved_hint", False)

# Save positions notice (must render BELOW the button)
st.session_state.setdefault("save_positions_notice", None)  # ("success"/"warning"/"info"/"error", "message")

# Track graph interactions so we can CLEAR the notice when user clicks/drags/changes graph
st.session_state.setdefault("graph_interaction_sig", None)

# Multi-node offset feature
st.session_state.setdefault("multi_node_offset_enabled", False)
st.session_state.setdefault("multi_node_offset_dx", 0.0)
st.session_state.setdefault("multi_node_offset_dy", 0.0)

GRAPH_HEIGHT = 750

st.markdown(
    """
    <style>
      div[data-testid="stVerticalBlockBorderWrapper"]{
        border-radius: 10px !important;
      }
      .graphwrap, .graphwrap > div { width: 100% !important; }
      .graphwrap iframe { width: 100% !important; min-width: 100% !important; display: block !important; }
    </style>
    """,
    unsafe_allow_html=True,
)

if st.sidebar.button("Clear workbook", key="clear_workbook_btn"):
    st.session_state.tray_df = None
    st.session_state.connections_df = None
    st.session_state.upload_hash = None
    st.session_state.sel_nodes = []
    st.session_state.sel_edges = []
    st.session_state.focus_node = None
    st.session_state.graph_key_v += 1
    st.session_state.uploader_key_v += 1
    st.session_state.layout_opt_active = False
    st.session_state.layout_opt_last_positions = None
    st.session_state.layout_opt_backup_xy = None

    st.session_state.initial_layout_autosave_active = False


    st.session_state.layout_unsaved_hint = False
    st.session_state.save_positions_notice = None
    st.session_state.graph_interaction_sig = None

    st.rerun()

file_bytes = None
if uploaded is not None:
    file_bytes = uploaded.getvalue()
if demo_clicked:
    file_bytes = build_demo_workbook_bytes()

if file_bytes is not None:
    try:
        this_hash = hashlib.md5(file_bytes).hexdigest()
        if st.session_state.upload_hash != this_hash:
            loaded, is_older_template = load_excel_to_dfs(file_bytes)

            tdf_loaded = ensure_xy_columns(loaded["Tray"])
            st.session_state.tray_df = tdf_loaded
            st.session_state.connections_df = loaded["Connections"]
            st.session_state.upload_hash = this_hash
            st.session_state.is_older_template = is_older_template
            st.session_state.sel_nodes = []
            st.session_state.sel_edges = []
            st.session_state.focus_node = None
            st.session_state.graph_key_v += 1
            st.session_state.layout_opt_active = False
            st.session_state.layout_opt_last_positions = None
            st.session_state.layout_opt_backup_xy = None
                                
            st.session_state.initial_layout_autosave_active = (not tray_has_any_xy(tdf_loaded))

                                                                
            st.session_state.layout_unsaved_hint = False
            st.session_state.save_positions_notice = None
            st.session_state.graph_interaction_sig = None

            st.sidebar.success("Workbook loaded.")
    except Exception as e:
        st.sidebar.error(f"Failed to load workbook: {e}")
        st.stop()

if st.session_state.tray_df is None:
    st.info("Upload an Excel workbook or load the demo workbook to begin.")
    st.stop()

st.session_state.tray_df = ensure_xy_columns(st.session_state.tray_df)
st.session_state.tray_df = ensure_person_columns(st.session_state.tray_df)

dfs = {
    "Tray": st.session_state.tray_df,
    "Connections": st.session_state.connections_df,
}

val_errors = validate_dfs(dfs)
if val_errors:
    st.warning("Workbook validation warnings:")
    for e in val_errors:
        st.write(f"- {e}")

tab1, tab2, tabG = st.tabs(["Tray", "Connections", "Graph Editor"])

with tab1:
    st.subheader("Tray")
    st.caption("X and Y store node positions. Email, phone, role, and notes store person-style metadata for each node.")
    st.session_state.tray_df = st.data_editor(
        ensure_person_columns(st.session_state.tray_df),
        num_rows="dynamic",
        width="stretch",
        key="tray_editor",
        column_config={
            "Email": st.column_config.TextColumn("Email"),
            "Phone": st.column_config.TextColumn("Phone"),
            "Role": st.column_config.TextColumn("Role"),
            "Notes": st.column_config.TextColumn("Notes", width="large"),
        },
    )

with tab2:
    st.subheader("Connections")
    st.caption("Edit the network links between nodes.")
    st.session_state.connections_df = st.data_editor(
        st.session_state.connections_df,
        num_rows="dynamic",
        width="stretch",
        key="connections_editor",
    )

with tabG:

    graph_col, tools_col = st.columns([3, 1], gap="large")

    node_names = (
        st.session_state.tray_df["RunName"]
        .dropna()
        .astype(str)
        .map(str.strip)
        .replace("", pd.NA)
        .dropna()
        .unique()
        .tolist()
    )
    node_names = sorted(node_names, key=lambda s: s.lower())

    selection = None
    this_run_sig = None

    with graph_col:
        graph_box = st.container(border=False)
        with graph_box:
            st.markdown('<div class="graphwrap">', unsafe_allow_html=True)

            combined_highlight = set()

            include_probe = bool(st.session_state.layout_opt_active or st.session_state.initial_layout_autosave_active)

            nodes, edges = build_vis_nodes_edges(
                st.session_state.tray_df,
                st.session_state.connections_df,
                focus_node=st.session_state.focus_node,
                focus_depth=int(st.session_state.focus_depth),
                include_probe=include_probe,
                highlight_nodes=combined_highlight,
            )

            if st.session_state.layout_opt_active:
                options = {
                    "physics": {
                        "enabled": True,
                        "stabilization": {
                            "enabled": True,
                            "iterations": 1600,
                            "updateInterval": 50,
                            "fit": False,
                        },
                        "solver": "forceAtlas2Based",
                        "forceAtlas2Based": {
                            "gravitationalConstant": -260,
                            "centralGravity": 0.01,
                            "springLength": 240,
                            "springConstant": 0.02,
                            "damping": 0.4,
                            "avoidOverlap": 1.0,
                        },
                        "minVelocity": 0.20,
                        "maxVelocity": 70,
                        "timestep": 0.5,
                    },
                    "layout": {"improvedLayout": True},
                    "interaction": {
                        "dragNodes": True,
                        "dragView": True,
                        "zoomView": True,
                        "hover": True,
                        "multiselect": True,
                        "selectConnectedEdges": True,
                        "navigationButtons": False,
                        "keyboard": False,
                    },
                    "nodes": {"font": {"vadjust": 0}},
                    "edges": {"smooth": False, "color": EDGE_COLOR, "width": EDGE_WIDTH},
                    # "animation": {"duration": 0},
                }
            else:
                options = {
                    "physics": {"enabled": False},
                    "layout": {"improvedLayout": True},
                    "interaction": {
                        "dragNodes": True,
                        "dragView": True,
                        "zoomView": True,
                        "hover": True,
                        "multiselect": True,
                        "selectConnectedEdges": True,
                        "navigationButtons": False,
                        "keyboard": False,
                    },
                    "nodes": {"font": {"vadjust": 0}},
                    "edges": {"smooth": False, "color": EDGE_COLOR, "width": EDGE_WIDTH},
                    # "animation": {"duration": 0},
                }

            selection = streamlit_vis_network(
                nodes,
                edges,
                height=GRAPH_HEIGHT,
                options=options,
                key=f"vis_network_graph_{st.session_state.graph_key_v}",
            )

            st.markdown("</div>", unsafe_allow_html=True)

        clicked_node = None
        positions = None
        sel_nodes = []
        sel_edges = []

        if selection:
            try:
                sel_nodes, sel_edges, positions = selection
            except Exception:
                sel_nodes, sel_edges, positions = [], [], None

            st.session_state.layout_opt_last_positions = positions if isinstance(positions, dict) else st.session_state.layout_opt_last_positions

            cleaned_nodes = [n for n in (sel_nodes or []) if str(n).strip() != PROBE_ID]
            st.session_state.sel_nodes = cleaned_nodes
            st.session_state.sel_edges = sel_edges or []

            if cleaned_nodes:
                clicked_node = str(cleaned_nodes[0]).strip()
        else:
            sel_nodes, sel_edges, positions = [], [], None
            st.session_state.sel_nodes = []
            st.session_state.sel_edges = []

        def _positions_sig(pos: dict | None):
            if not isinstance(pos, dict) or not pos:
                return (0, 0, 0)
            items = [(k, v) for k, v in pos.items() if str(k).strip() != PROBE_ID]
            n = len(items)
            sx = sy = 0.0
            for k, v in items[:15]:
                x = y = 0.0
                if isinstance(v, dict):
                    x = float(v.get("x", 0.0) or 0.0)
                    y = float(v.get("y", 0.0) or 0.0)
                elif isinstance(v, (list, tuple)) and len(v) >= 2:
                    try:
                        x = float(v[0] or 0.0)
                        y = float(v[1] or 0.0)
                    except Exception:
                        x = y = 0.0
                sx += x
                sy += y
            return (n, round(sx, 1), round(sy, 1))

        this_run_sig = (
            tuple([str(x).strip() for x in (sel_nodes or []) if str(x).strip() != PROBE_ID]),
            tuple([str(x) for x in (sel_edges or [])][:3]),
            _positions_sig(positions),
        )

        # Clear message on real graph interaction, BUT not immediately after a "Save" forced refresh.
        if st.session_state.graph_interaction_sig is None:
            st.session_state.graph_interaction_sig = this_run_sig
        else:
            if this_run_sig != st.session_state.graph_interaction_sig:
                st.session_state.graph_interaction_sig = this_run_sig
                st.session_state.save_positions_notice = None

        if (not st.session_state.layout_opt_active) and isinstance(st.session_state.layout_opt_last_positions, dict):
            st.session_state.layout_unsaved_hint = _has_unsaved_layout_changes(
                st.session_state.tray_df,
                st.session_state.layout_opt_last_positions,
                tol=0.5,
            )
        else:
            st.session_state.layout_unsaved_hint = False

        if (
            st.session_state.initial_layout_autosave_active
            and (not st.session_state.layout_opt_active)
            and isinstance(positions, dict)
            and len(positions) > 0
        ):
            st.session_state.tray_df = apply_positions_to_tray(st.session_state.tray_df, positions)
            st.session_state.initial_layout_autosave_active = False
            st.session_state.graph_key_v += 1
            st.success("Initial layout locked in and saved to Tray (X/Y).")
            st.rerun()

    with tools_col:
        # Save button (OUTSIDE scrollable container - stays visible when scrolling)
        save_clicked = st.button("💾 Save current node positions to Tray (X/Y)", key="save_positions_btn", width="stretch")
        save_notice_slot = st.empty()
        
        # Scrollable tools container
        tools_box = st.container(height=GRAPH_HEIGHT, border=False)
        with tools_box:
            if st.session_state.layout_unsaved_hint and (not st.session_state.layout_opt_active):
                st.warning(
                    "You have **unsaved node position changes**.\n\n"
                    "Click **Save current node positions to Tray (X/Y)** to store the new layout in the workbook."
                )

            st.markdown("### Selection")

            sel_nodes2 = st.session_state.sel_nodes or []
            sel_edges2 = st.session_state.sel_edges or []

            # Multi-node operations
            if len(sel_nodes2) > 1:
                # Connect all selected nodes (FIRST)
                st.markdown("**Connect All Selected Nodes**")
                st.caption(f"Create connections between all {len(sel_nodes2)} selected nodes")
                
                if st.button("Connect all selected nodes", key="connect_all_nodes_btn", width="stretch"):
                    connections_added = 0
                    connections_already_exist = 0
                    
                    # Create connections between all pairs of selected nodes
                    for i, node1 in enumerate(sel_nodes2):
                        for node2 in sel_nodes2[i+1:]:
                            new_con, added = df_add_edge(st.session_state.connections_df, node1, node2)
                            if added:
                                st.session_state.connections_df = new_con
                                connections_added += 1
                            else:
                                connections_already_exist += 1
                    
                                    
                    if connections_added > 0:
                        message = f"Added {connections_added} new connection{'s' if connections_added != 1 else ''}"
                        if connections_already_exist > 0:
                            message += f" ({connections_already_exist} already existed)"
                        st.success(message)
                        st.session_state.graph_key_v += 1
                        st.rerun()
                    elif connections_already_exist > 0:
                        st.info(f"All {connections_already_exist} possible connections already exist.")
                    else:
                        st.warning("Could not add any connections.")
                
                
                # Move Multiple Nodes
                st.markdown("**Move Multiple Nodes**")
                st.caption(f"{len(sel_nodes2)} nodes selected")
                
                col1, col2 = st.columns(2)
                with col1:
                    dx = st.number_input("Offset X", value=0.0, step=10.0, key="multi_offset_dx")
                with col2:
                    dy = -st.number_input("Offset Y", value=0.0, step=10.0, key="multi_offset_dy")
                
                if st.button("Apply offset to selected", key="apply_offset_btn", width="stretch"):
                    st.session_state.tray_df = apply_offset_to_nodes(
                        st.session_state.tray_df,
                        sel_nodes2,
                        float(dx),
                        float(dy)
                    )
                    st.session_state.layout_unsaved_hint = True
                    st.session_state.graph_key_v += 1
                    st.success(f"Offset applied! Moved {len(sel_nodes2)} nodes by ({dx}, {dy})")
                    st.rerun()
                
                # Scale Multiple Nodes
                st.markdown("**Scale Multiple Nodes**")
                st.caption("Scales nodes from their center point")
                
                # Initialize scale value in session state if not present
                if "scale_value" not in st.session_state:
                    st.session_state.scale_value = 1.0
                
                scale_col1, scale_col2 = st.columns([3, 1])
                with scale_col1:
                    st.session_state.scale_value = st.number_input(
                        "Scale factor",
                        value=st.session_state.scale_value,
                        min_value=0.1,
                        step=0.1,
                        key="scale_input"
                    )
                with scale_col2:
                    # Add vertical spacing to align button with number input
                    st.write("")
                    if st.button("Apply", key="apply_scale_btn", width="stretch"):
                        scale_factor = float(st.session_state.scale_value)
                        if scale_factor == 1.0:
                            pass
                        else:
                            st.session_state.tray_df = apply_scale_to_nodes(
                                st.session_state.tray_df,
                                sel_nodes2,
                                scale_factor
                            )
                            st.session_state.layout_unsaved_hint = True
                            st.session_state.graph_key_v += 1
                            st.success(f"Scale applied! Scaled {len(sel_nodes2)} nodes by factor {scale_factor:.2f}")
                            st.rerun()

                # Display warning if scale factor is 1.0
                if st.session_state.scale_value == 1.0:
                    st.info("Scale factor is 1.0 - no change will occur")

                # Duplicate group
                if st.button("Duplicate selected group", key="dup_group_btn", width="stretch"):
                    new_df, new_names = df_duplicate_nodes(st.session_state.tray_df, sel_nodes2)
                    st.session_state.tray_df = ensure_xy_columns(new_df)
                    st.success(f"Duplicated {len(sel_nodes2)} nodes. New names: {', '.join(new_names)}")
                    st.session_state.graph_key_v += 1
                    st.rerun()

            elif sel_nodes2:
                node_id = str(sel_nodes2[0]).strip()
                tdf = st.session_state.tray_df
                row = tdf[tdf["RunName"].astype(str).str.strip() == node_id]
                x_val = y_val = None
                email_val = phone_val = role_val = notes_val = ""
                if not row.empty:
                    x_val = row.iloc[0].get("X", pd.NA)
                    y_val = row.iloc[0].get("Y", pd.NA)
                    email_val = str(row.iloc[0].get("Email", "") or "").strip()
                    phone_val = str(row.iloc[0].get("Phone", "") or "").strip()
                    role_val = str(row.iloc[0].get("Role", "") or "").strip()
                    notes_val = str(row.iloc[0].get("Notes", "") or "").strip()

                st.markdown("**Selected Node**")
                st.markdown("**Name:**")
                st.code(node_id, language=None)
                st.write("**Type:** Person")
                st.write(f"**X:** {'' if pd.isna(x_val) else x_val}")
                st.write(f"**Y:** {'' if pd.isna(y_val) else y_val}")
                if role_val:
                    st.write(f"**Role:** {role_val}")
                if email_val:
                    st.write(f"**Email:** {email_val}")
                if phone_val:
                    st.write(f"**Phone:** {phone_val}")
                if notes_val:
                    st.write(f"**Notes:** {notes_val}")

                st.markdown("**Edit Person Details**")
                edit_email = st.text_input("Email", value=email_val, key="sel_edit_email")
                edit_phone = st.text_input("Phone", value=phone_val, key="sel_edit_phone")
                edit_role = st.text_input("Role", value=role_val, key="sel_edit_role")
                edit_notes = st.text_area("Notes", value=notes_val, key="sel_edit_notes")
                if st.button("Save person details", key="save_person_details_btn", width="stretch"):
                    node_mask = st.session_state.tray_df["RunName"].astype(str).str.strip() == node_id
                    st.session_state.tray_df.loc[node_mask, "Email"] = edit_email
                    st.session_state.tray_df.loc[node_mask, "Phone"] = edit_phone
                    st.session_state.tray_df.loc[node_mask, "Role"] = edit_role
                    st.session_state.tray_df.loc[node_mask, "Notes"] = edit_notes
                    st.success(f"Updated details for {node_id}")
                    st.session_state.graph_key_v += 1
                    st.rerun()

                st.markdown("**Connect this node**")
                connect_options = [""] + [n for n in node_names if n != node_id]
                target = st.selectbox(
                    "Connect to (type to search)",
                    options=connect_options,
                    index=0,
                    key="sel_connect_target",
                )

                c_add, c_del = st.columns(2)
                with c_add:
                    if st.button("Add connection", key="sel_add_conn_btn", width="stretch"):
                        tgt = (target or "").strip()
                        if not tgt:
                            st.warning("Choose a target node.")
                        else:
                            new_con, added = df_add_edge(st.session_state.connections_df, node_id, tgt)
                            if added:
                                st.session_state.connections_df = new_con
                                st.success(f"Added connection: {node_id} ? {tgt}")
                                st.session_state.graph_key_v += 1
                                st.rerun()
                            else:
                                st.info("That connection already exists (or could not be added).")
                with c_del:
                    if st.button("Delete connection", key="sel_del_conn_btn", width="stretch"):
                        tgt = (target or "").strip()
                        if not tgt:
                            st.warning("Choose a target node.")
                        else:
                            before = len(st.session_state.connections_df) if st.session_state.connections_df is not None else 0
                            st.session_state.connections_df = df_delete_edge(st.session_state.connections_df, node_id, tgt)
                            after = len(st.session_state.connections_df) if st.session_state.connections_df is not None else 0
                            if before == after:
                                st.info("No matching connection was found to delete.")
                            else:
                                st.success(f"Deleted connection: {node_id} ? {tgt}")
                                st.session_state.graph_key_v += 1
                            st.rerun()

                if st.button("Delete node", key="sel_delete_node", width="stretch"):
                    t, c = df_delete_node(
                        st.session_state.tray_df,
                        st.session_state.connections_df,
                        node_id
                    )
                    st.session_state.tray_df = ensure_xy_columns(t)
                    st.session_state.connections_df = c
                    if st.session_state.focus_node == node_id:
                        st.session_state.focus_node = None
                    st.session_state.sel_nodes = []
                    st.session_state.sel_edges = []
                    st.success("Node deleted.")
                    st.session_state.graph_key_v += 1
                    st.rerun()

                st.markdown("**Rename**")
                new_name = st.text_input("New name", value=node_id, key="sel_rename_node_new")
                if st.button("Rename node", key="sel_rename_node_btn", width="stretch"):
                    if new_name.strip() and new_name.strip() != node_id:
                        if (st.session_state.tray_df["RunName"].astype(str).str.strip() == new_name.strip()).any():
                            st.error("That name already exists.")
                        else:
                            t, c = df_rename_node(
                                st.session_state.tray_df,
                                st.session_state.connections_df,
                                node_id,
                                new_name.strip()
                            )
                            st.session_state.tray_df = ensure_xy_columns(t)
                            st.session_state.connections_df = c
                            if st.session_state.focus_node == node_id:
                                st.session_state.focus_node = new_name.strip()
                            st.session_state.sel_nodes = [new_name.strip()]
                            st.session_state.sel_edges = []
                            st.success("Node renamed.")
                            st.session_state.graph_key_v += 1
                            st.rerun()
                    else:
                        st.warning("Enter a different non-empty name.")

                st.markdown("**Duplicate**")
                dup_name = st.text_input("Duplicate as", value=f"{node_id}_COPY", key="sel_dup_node_new")
                if st.button("Duplicate node", key="sel_dup_node_btn", width="stretch"):
                    if dup_name.strip():
                        if (st.session_state.tray_df["RunName"].astype(str).str.strip() == dup_name.strip()).any():
                            st.error("That duplicate name already exists.")
                        else:
                            st.session_state.tray_df = df_duplicate_node(st.session_state.tray_df, node_id, dup_name.strip())
                            st.success("Node duplicated.")
                            st.session_state.graph_key_v += 1
                            st.rerun()
                    else:
                        st.warning("Enter a name for the duplicate.")

            elif sel_edges2:
                raw_edge = sel_edges2[0]
                a, b, disp = parse_selected_edge(raw_edge)

                st.markdown("**Selected Connection**")
                st.write(f"**Connection:** {disp}")

                if not (a and b):
                    st.caption("Could not parse endpoints for this connection (unexpected format).")
                    st.code(str(raw_edge))
                else:
                    st.write(f"**From:** {a}")
                    st.write(f"**To:** {b}")
                    st.divider()
                    if st.button("Delete connection", key="sel_delete_edge", width="stretch"):
                        before = len(st.session_state.connections_df) if st.session_state.connections_df is not None else 0
                        st.session_state.connections_df = df_delete_edge(st.session_state.connections_df, a, b)
                        after = len(st.session_state.connections_df) if st.session_state.connections_df is not None else 0

                        st.session_state.sel_nodes = []
                        st.session_state.sel_edges = []

                        if before == after:
                            st.info("No matching connection was found to delete (already removed?).")
                        else:
                            st.success("Connection deleted.")
                        st.session_state.graph_key_v += 1
                        st.rerun()

                    st.caption("This deletes the row from the Connections sheet (undirected match).")

            else:
                st.info("Select a node or connection in the graph to edit it here.")

            with st.expander("?? Layout", expanded=False):
                if st.button("Home / Recenter graph view", key="home_recenter_btn", width="stretch"):
                    st.session_state.focus_node = None
                    st.session_state.sel_nodes = []
                    st.session_state.sel_edges = []
                    st.session_state.graph_key_v += 1
                    st.success("Recentered graph view.")
                    st.rerun()

                if not st.session_state.layout_opt_active:
                    if st.button("Optimize layout (turns on physics)", key="opt_turn_on_physics_btn", width="stretch"):
                        tdf = ensure_xy_columns(st.session_state.tray_df).copy()
                        backup = {
                            "RunName": tdf["RunName"].astype(str).tolist(),
                            "X": tdf["X"].tolist(),
                            "Y": tdf["Y"].tolist(),
                        }
                        st.session_state.layout_opt_backup_xy = backup

                        st.session_state.tray_df["X"] = pd.NA
                        st.session_state.tray_df["Y"] = pd.NA

                        st.session_state.initial_layout_autosave_active = False

                        st.session_state.focus_node = None
                        st.session_state.sel_nodes = []
                        st.session_state.sel_edges = []

                        st.session_state.layout_opt_active = True
                        st.session_state.layout_opt_last_positions = None
                        st.session_state.layout_unsaved_hint = False
                        st.session_state.graph_key_v += 1
                        st.success("Physics enabled for optimization. Let it settle, then save or cancel.")
                        st.rerun()
                else:
                    st.info(
                        "Optimization is active (physics ON).\n\n"
                        "✅ Let the graph settle, then click **Save optimized positions (turns off physics)**.\n"
                        "↩️ Or click **Cancel optimization (turns off physics)** to revert to the previous layout."
                    )

                    if st.button("Save optimized positions (turns off physics)", key="save_opt_positions_btn", width="stretch"):
                        positions2 = st.session_state.layout_opt_last_positions

                        if positions2 and isinstance(positions2, dict) and len(positions2) > 0:
                            st.session_state.tray_df = apply_positions_to_tray(st.session_state.tray_df, positions2)
                            st.session_state.layout_opt_active = False
                            st.session_state.layout_opt_last_positions = None
                            st.session_state.layout_opt_backup_xy = None
                            st.session_state.layout_unsaved_hint = False
                            st.session_state.graph_key_v += 1
                            st.success(f"Saved optimized positions for {len(positions2)} node(s). Physics is now OFF.")
                            st.rerun()
                        else:
                            st.warning(
                                "No positions were returned yet.\n\n"
                                "✅ Wait a moment for stabilization, then click **Save optimized positions** again."
                            )

                    if st.button("Cancel optimization (turns off physics)", key="cancel_opt_btn", width="stretch"):
                        backup = st.session_state.layout_opt_backup_xy
                        if backup and "RunName" in backup:
                            tdf = ensure_xy_columns(st.session_state.tray_df).copy()
                            rn = tdf["RunName"].astype(str).tolist()
                            bmap = {str(n).strip(): (backup["X"][i], backup["Y"][i]) for i, n in enumerate(backup["RunName"])}

                            xs = []
                            ys = []
                            for n in rn:
                                x, y = bmap.get(str(n).strip(), (pd.NA, pd.NA))
                                xs.append(x)
                                ys.append(y)
                            tdf["X"] = xs
                            tdf["Y"] = ys
                            st.session_state.tray_df = tdf

                        st.session_state.layout_opt_active = False
                        st.session_state.layout_opt_last_positions = None
                        st.session_state.layout_opt_backup_xy = None
                        st.session_state.layout_unsaved_hint = False
                        st.session_state.graph_key_v += 1
                        st.success("Optimization canceled. Previous layout restored. Physics is now OFF.")
                        st.rerun()

            # ------------------------------------------------------------
            # Save current positions
            # FIXED: after successful save, force graph re-mount so it
            # shows the saved layout immediately (no “stale view”).
            # Also reset graph_interaction_sig to avoid auto-clearing
            # the success message on that forced refresh.
            # (Note: save_clicked and save_notice_slot are now defined at top of tools_box)
            # Save button click handler (defined at top of tools_box, logic here)
            if save_clicked:
                pos = None
                sn = se = None
                try:
                    if selection:
                        sn, se, pos = selection
                except Exception:
                    sn, se, pos = None, None, None

                if isinstance(pos, dict) and len(pos) > 0:
                    st.session_state.tray_df = apply_positions_to_tray(st.session_state.tray_df, pos)
                    st.session_state.layout_unsaved_hint = False

                    # Keep message visible + immediately refresh graph instance
                    st.session_state.save_positions_notice = ("success", "✅ Positions saved to Tray (X/Y).")

                    # IMPORTANT: prevent the forced refresh from being interpreted as "user interaction"
                    st.session_state.graph_interaction_sig = None

                    # IMPORTANT: remount the vis network so it uses saved X/Y right away
                    st.session_state.graph_key_v += 1

                    # optional: clear selection so user sees a clean graph post-save
                    st.session_state.sel_nodes = []
                    st.session_state.sel_edges = []

                    st.rerun()
                else:
                    sel_active = bool(
                        (sn and len(sn) > 0) or
                        (se and len(se) > 0) or
                        (st.session_state.sel_nodes) or
                        (st.session_state.sel_edges)
                    )
                    if sel_active:
                        st.session_state.save_positions_notice = (
                            "warning",
                            "No positions were returned.\n\n"
                            "✅ Fix:\n"
                            "- Click **empty space in the graph** (to deselect the node/connection)\n"
                            "- Then click **Save** again."
                        )
                    else:
                        st.session_state.save_positions_notice = (
                            "warning",
                            "No positions were returned yet.\n\n"
                            "✅ Try dragging any node slightly, then click **Save** again."
                        )

            if st.session_state.save_positions_notice:
                lvl, txt = st.session_state.save_positions_notice
                if lvl == "success":
                    save_notice_slot.success(txt)
                elif lvl == "warning":
                    save_notice_slot.warning(txt)
                elif lvl == "info":
                    save_notice_slot.info(txt)
                else:
                    save_notice_slot.error(txt)

    st.divider()
    st.caption(
        "If the workbook loads with blank X/Y, the app will automatically lock in the FIRST layout once. "
        "After that, positions only change when you click **Save current node positions to Tray (X/Y)**."
    )
    st.caption(
        "**Multi-select:** Hold **Shift** and left-click drag to select multiple nodes together."
    )
